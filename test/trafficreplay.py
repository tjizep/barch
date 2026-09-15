#!/usr/bin/env python3
"""
Read a traffic recording and play it back - see TODO 316 and 317.

Recording is the server's half: `CONFIG SET traffic_capture on` and every command a
RESP client sends is appended to a file - one per thread that records, named from
`traffic_file`, so `barch_traffic.dat` is written as `barch_traffic.0.dat`,
`barch_traffic.1.dat` and so on. A file per thread because a shared one is a
shared lock. This is the other half: it reads the set, merges it back into arrival
order, and reissues it.

    # record something
    valkey-cli -p 14000 CONFIG SET traffic_capture on
    ...do the thing...
    valkey-cli -p 14000 CONFIG SET traffic_capture off     # flushes the files

    # see what it caught - name the set, not one of its files
    ./trafficreplay.py barch_traffic.dat --dry-run --print | head

    # play it back at half speed with 20% jitter on the timing
    ./trafficreplay.py barch_traffic.dat --port 14000 --speed 0.5 --jitter 0.2

Naming the set reads all of it; naming one file (`barch_traffic.2.dat`) reads just
that thread's share, which is occasionally what you want when a crash looks like
one connection's fault.

The recording knows which connection each command arrived on, and the replay keeps
that: one client per recorded connection, each command sent at its own offset from
the start. So four clients that overlapped during the recording overlap during the
replay, which is the whole point - a single stream reproduces the commands but
never the concurrency, and concurrency is what the interesting bugs need.

Jitter moves each command off its recorded offset by up to that fraction of the
gap in front of it, either way. Without it a replay is the same run every time,
and a race that needs two things to land together either always reproduces or
never does.

By default it turns capture off on the target before starting, because replaying
into a recording server records the replay, and that recording replays, and so on.
--keep-capture if you want that anyway.

Two things worth knowing about a recording. It appends, so a second run adds to
the first unless the file is moved aside or `traffic_file` is pointed somewhere
new. And it holds the client handshakes too - redis-py sends CLIENT SETINFO when
it connects - because what arrived is what a replay should send.

A recording whose last record is short is one the writer was still in the middle
of, or never closed, which is normal if the process died. That record is dropped
and the rest replays.
"""
import argparse
import http.client
import pathlib
import random
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import redis

MAGIC = b"barch-traffic-file-1\n"

# the two commands that change which space a connection is in. A `space:CMD`
# prefix is undone when the command finishes, so a prefixed USE switches the space
# and then switches straight back - it has to go out bare or the switch is lost.
# Which is fine, because the switch it is reproducing is the recording's own.
SPACE_SWITCHES = ("USE", "SELECT")

# an HTTP request is recorded as the arguments of a pseudo command called HTTP -
# see traffic.h and TODO 319:
#
#     HTTP <method> <raw url> <port> <content type> <body>
#
# It cannot be replayed on the connection it arrived on, because crow's request
# carries no connection id, so these are fired from a pool at their own offsets
# while the RESP records go to one client per recorded connection.
HTTP_MARKER = b"HTTP"


def is_http(argv):
    return argv[0].upper() == HTTP_MARKER and len(argv) >= 6


class BadRecording(Exception):
    pass


def decode(body):
    """one record body -> (nanos, connection, space, [args]), or None"""
    # length prefixed rather than delimited: an argument can hold newlines, so only
    # the lengths are text and the bytes are copied out by count
    fields = []
    rest = body
    for _ in range(4):
        head, sep, rest = rest.partition(b"\n")
        if not sep:
            return None
        fields.append(head)
    at, conn, space, argc = fields
    args = []
    for _ in range(int(argc)):
        ln, sep, rest = rest.partition(b"\n")
        if not sep:
            return None
        n = int(ln)
        if len(rest) < n + 1:
            return None
        args.append(rest[:n])
        rest = rest[n + 1:]
    if not args:
        return None
    return int(at), int(conn), space.decode("utf-8", "replace"), args


def files_of(path):
    """the files a recording is spread over.

    `barch_traffic.dat` means the set - every `barch_traffic.<n>.dat` beside it.
    An existing file means itself, so one thread's share can be read on its own.
    """
    p = pathlib.Path(path)
    if p.is_file():
        return [p]
    # `a/b.dat` -> a/b.*.dat, `a/b` -> a/b.*
    found = sorted(p.parent.glob(f"{p.stem}.*{p.suffix}"),
                   key=lambda q: (len(q.name), q.name))
    if not found:
        raise FileNotFoundError(f"no recording at {path} and nothing matching {p.stem}.*{p.suffix}")
    return found


def read_one(path):
    """one file -> ([(nanos, connection, space, [args])], unreadable)"""
    with open(path, "rb") as fh:
        blob = fh.read()
    if not blob.startswith(MAGIC):
        raise BadRecording(f"{path} does not start with {MAGIC.strip().decode()}")
    at = len(MAGIC)
    out = []
    skipped = 0
    while at < len(blob):
        nl = blob.find(b"\n", at)
        if nl < 0:
            skipped += 1
            break
        try:
            size = int(blob[at:nl])
        except ValueError:
            skipped += 1
            break
        body = blob[nl + 1:nl + 1 + size]
        if len(body) < size:
            skipped += 1              # the tail of a file still being written
            break
        at = nl + 1 + size
        rec = decode(body)
        if rec is None:
            skipped += 1
            continue
        out.append(rec)
    return out, skipped


def read_recording(path, limit=0):
    """a whole recording, in arrival order.

    Returns ([(nanos, connection, space, [args])], unreadable). A short record at
    the end of a file is the writer having been interrupted, and counts as
    unreadable like any other - the records in front of it are still good.

    The files are merged on the timestamp, because ordering across threads is the
    clock's job now that each thread writes its own. Within one connection it does
    not matter which file the records landed in: a connection's commands are
    sequential in real time, so the timestamps put them back the way they were
    sent.
    """
    out = []
    skipped = 0
    for f in files_of(path):
        got, missed = read_one(f)
        out.extend(got)
        skipped += missed
    out.sort(key=lambda rec: rec[0])
    if limit:
        out = out[:limit]
    return out, skipped


def wire_name(space, argv):
    """the name to send: a command recorded against a named space is reissued
    against it, unless args[0] already carries a `space:` prefix of its own"""
    name = argv[0].decode("utf-8", "replace")
    if name.upper() in SPACE_SWITCHES:
        return name
    # the recorded name is the space that was in force, which is not the same as
    # what args[0] carried - the client may have switched with USE instead
    if space and ":" not in name:
        return f"{space}:{name}"
    return name


def schedule(records, speed=1.0, jitter=0.0, max_gap=5.0, rnd=random):
    """give every record its offset in seconds from the start of the replay.

    Offsets rather than gaps, because the gaps are per connection and the timing
    is not: two connections keep their overlap only if both are placed on one
    timeline. A pause longer than max_gap is shortened, so an idle minute in the
    middle of a recording does not have to be sat through.
    """
    out = []
    prev_at = records[0][0]
    offset = 0.0
    for rec in records:
        gap = (rec[0] - prev_at) / 1e9
        prev_at = rec[0]
        if speed > 0:
            gap /= speed
        gap = min(gap, max_gap)
        offset += gap
        moved = offset
        if jitter > 0 and gap > 0:
            moved += rnd.uniform(-jitter, jitter) * gap
        out.append((max(0.0, moved), rec))
    return out


def http_once(host, port, method, url, ctype, body, timeout=30.0):
    """one recorded request, sent again. Returns the status code."""
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        headers = {}
        if ctype:
            headers["Content-Type"] = ctype
        conn.request(method, url, body=body or None, headers=headers)
        res = conn.getresponse()
        res.read()                      # the body matters for timing, not for us
        return res.status
    finally:
        conn.close()


def replay(records, connect, speed=1.0, jitter=0.0, max_gap=5.0, show=False, rnd=random,
           http_host=None, http_port=None, http_workers=32):
    """reissue a recording.

    The RESP records go to one client per recorded connection, each command at its
    own offset from the start, so connections that overlapped overlap again. The
    HTTP records go to a pool of `http_workers` threads, each request waiting for
    its own offset - the same timeline, a different way of getting on it, because
    an HTTP record has no connection to be put back on.

    `connect` makes a RESP client. Returns (sent, refused).
    """
    if not records:
        return 0, 0
    sched = schedule(records, speed, jitter, max_gap, rnd)

    streams = defaultdict(list)
    web = []
    for offset, (_, conn, space, argv) in sched:
        if is_http(argv):
            web.append((offset, argv))
        else:
            streams[conn].append((offset, space, argv))

    counts = {}
    lock = threading.Lock()
    ready = threading.Barrier(len(streams) + (1 if web else 0) + 1)
    start = [0.0]

    def hold(offset):
        wait = start[0] + offset - time.time()
        if wait > 0:
            time.sleep(wait)

    def run_resp(conn, stream):
        client = connect()
        sent = failed = 0
        ready.wait()
        for offset, space, argv in stream:
            hold(offset)
            name = wire_name(space, argv)
            try:
                client.execute_command(name, *argv[1:])
                sent += 1
            except redis.exceptions.ResponseError as e:
                # a replayed command can legitimately fail - a SET NX whose key
                # now exists, say - so this is counted and reported, not fatal
                failed += 1
                if show:
                    print(f"  ! {name}: {e}")
        with lock:
            counts[("resp", conn)] = (sent, failed)

    def run_web():
        sent = failed = 0
        def one(offset, argv):
            nonlocal sent, failed
            hold(offset)
            method = argv[1].decode("utf-8", "replace")
            url = argv[2].decode("utf-8", "replace")
            port = http_port or int(argv[3])
            ctype = argv[4].decode("utf-8", "replace")
            try:
                code = http_once(http_host, port, method, url, ctype, argv[5])
                # a 4xx or 5xx is the application answering, not the replay
                # failing, so it is counted as sent and shown rather than hidden
                if show and code >= 400:
                    print(f"  ! {code} {method} {url}")
                sent += 1
            except OSError as e:
                failed += 1
                if show:
                    print(f"  ! {method} {url}: {e}")
        pool = ThreadPoolExecutor(max_workers=http_workers)
        ready.wait()
        futures = [pool.submit(one, offset, argv) for offset, argv in web]
        for f in futures:
            f.result()
        pool.shutdown()
        with lock:
            counts[("http", 0)] = (sent, failed)

    threads = [threading.Thread(target=run_resp, args=(c, st), daemon=True)
               for c, st in streams.items()]
    if web:
        threads.append(threading.Thread(target=run_web, daemon=True))
    for t in threads:
        t.start()
    ready.wait()                     # everyone connected; now the clock starts
    start[0] = time.time()
    for t in threads:
        t.join()
    return (sum(s for s, _ in counts.values()), sum(f for _, f in counts.values()))


def shown(arg, width=48):
    s = arg.decode("utf-8", "replace")
    return s if len(s) <= width else s[:width] + "..."


def main():
    ap = argparse.ArgumentParser(description="replay a barch traffic recording")
    ap.add_argument("recording", help="the file traffic_capture wrote")
    ap.add_argument("--host", default="127.0.0.1", help="where to replay it")
    ap.add_argument("--port", type=int, default=14000)
    ap.add_argument("--speed", type=float, default=1.0, help="1 is real time, 2 is twice as fast")
    ap.add_argument("--jitter", type=float, default=0.0,
                    help="move each command by up to this fraction of the gap in front of it")
    ap.add_argument("--max-gap", type=float, default=5.0,
                    help="seconds; a longer pause in the recording is shortened to this")
    ap.add_argument("--limit", type=int, default=0, help="only the first N commands")
    ap.add_argument("--seed", type=int, default=None, help="fix the jitter, for a repeatable replay")
    ap.add_argument("--dry-run", action="store_true", help="read it and say what it holds, send nothing")
    ap.add_argument("--print", dest="show", action="store_true", help="print each command")
    ap.add_argument("--keep-capture", action="store_true",
                    help="do not turn capture off on the target first")
    ap.add_argument("--http-host", default=None,
                    help="where to send the recorded HTTP requests, if not --host")
    ap.add_argument("--http-port", type=int, default=None,
                    help="override the port the requests were recorded on")
    ap.add_argument("--http-workers", type=int, default=32,
                    help="how many requests may be in flight at once")
    args = ap.parse_args()

    rnd = random.Random(args.seed)

    try:
        records, skipped = read_recording(args.recording, args.limit)
    except (OSError, BadRecording) as e:
        print(e)
        return 1
    if not records:
        print(f"nothing recorded in {args.recording}"
              f"{f' ({skipped} unreadable)' if skipped else ''}")
        return 1

    span = (records[-1][0] - records[0][0]) / 1e9
    web = sum(1 for r in records if is_http(r[3]))
    conns = len({r[1] for r in records if not is_http(r[3])})
    what = []
    if len(records) - web:
        what.append(f"{len(records) - web} commands from {conns} connection(s)")
    if web:
        what.append(f"{web} HTTP requests")
    print(f"{' and '.join(what)} over {span:.3f}s"
          f"{f', {skipped} unreadable' if skipped else ''}")

    if args.dry_run:
        if args.show:
            first = records[0][0]
            for at, conn, space, argv in records:
                if is_http(argv):
                    print(f"  +{(at - first) / 1e9:9.6f}s :{argv[3].decode()} "
                          f"{argv[1].decode()} {shown(argv[2], 80)}"
                          + (f" [{len(argv[5])} bytes]" if argv[5] else ""))
                else:
                    print(f"  +{(at - first) / 1e9:9.6f}s c{conn} {wire_name(space, argv)} "
                          + " ".join(shown(a) for a in argv[1:]))
        return 0

    host = args.host
    port = args.port
    if not args.keep_capture:
        # otherwise the replay is appended to whatever is being recorded now, and
        # replaying that recording replays the replay
        redis.Redis(host=host, port=port, db=0, protocol=2).execute_command(
            "CONFIG", "SET", "traffic_capture", "off")

    def connect():
        return redis.Redis(host=host, port=port, db=0, protocol=2)

    started = time.time()
    sent, failed = replay(records, connect, speed=args.speed, jitter=args.jitter,
                          max_gap=args.max_gap, show=args.show, rnd=rnd,
                          http_host=args.http_host or host, http_port=args.http_port,
                          http_workers=args.http_workers)
    elapsed = time.time() - started
    print(f"replayed {sent} of {len(records)} in {elapsed:.3f}s, {failed} refused")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        # `--print | head` is in the usage above, so closing the pipe early is a
        # normal way to use this and not something to print a traceback about
        sys.exit(0)
