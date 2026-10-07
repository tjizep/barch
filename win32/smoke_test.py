#!/usr/bin/env python3
"""
A short end to end check of a barchd build, for the Windows CI job (TODO 598).
It runs anywhere barchd does, so it can be tried against a Linux build too:

    python3 win32/smoke_test.py path/to/barchd[.exe] [port]

What it covers is what the Windows port changed underneath barchd: the socket
listener, an expiry in milliseconds (a 32 bit `long` used to hold it), and a
save that survives the process being killed, which goes through the mmap, rename
and fsync replacements in win32/src/posix_compat.cpp. That's done twice, the
second time with arenas mapped from files that grow while they're mapped.
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def command(sock, *args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    sock.sendall(out)
    return read_reply(sock.makefile("rb"))


def read_reply(f):
    line = f.readline()
    if not line:
        raise RuntimeError("connection closed")
    kind, rest = line[:1], line[1:-2]
    if kind in (b"+", b"-"):
        return (kind + rest).decode()
    if kind == b":":
        return int(rest)
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        data = f.read(n + 2)[:-2]
        return data.decode()
    if kind == b"_":
        return None
    if kind == b"*":
        return [read_reply(f) for _ in range(int(rest))]
    raise RuntimeError("unexpected reply %r" % line)


def connect(port, proc, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("barchd exited with %d before listening" % proc.returncode)
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=10)
        except OSError:
            time.sleep(0.25)
    raise RuntimeError("barchd did not listen on %d within %ds" % (port, timeout))


def start(exe, port, data, log, extra):
    return subprocess.Popen([exe, "--port", str(port), "--dir", data] + extra,
                            stdout=log, stderr=subprocess.STDOUT)


def check(what, got, want):
    if got != want:
        raise AssertionError("%s: got %r, wanted %r" % (what, got, want))
    print("ok  ", what)


def pipeline_set(sock, prefix, n):
    """n SETs in one write, then their n replies"""
    out = bytearray()
    for i in range(n):
        k, v = b"%s:%d" % (prefix, i), b"v%d" % i
        out += b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len(k), k, len(v), v)
    sock.sendall(bytes(out))
    f = sock.makefile("rb")
    for i in range(n):
        reply = read_reply(f)
        if reply != "+OK":
            raise AssertionError("SET %s:%d: %r" % (prefix.decode(), i, reply))


def round_trip(exe, port, data, log, extra, label, keys):
    """write, SAVE, kill, start again and read it all back"""
    print("--", label)
    proc = start(exe, port, data, log, extra)
    try:
        with connect(port, proc) as s:
            check("PING", command(s, "PING"), "+PONG")
            check("SET", command(s, "SET", "smoke", "value"), "+OK")
            check("GET", command(s, "GET", "smoke"), "value")
            check("SET PX", command(s, "SET", "expiring", "v", "PX", 600000), "+OK")
            pttl = command(s, "PTTL", "expiring")
            if not (isinstance(pttl, int) and 590000 < pttl <= 600000):
                raise AssertionError("PTTL: got %r, wanted just under 600000" % pttl)
            print("ok   PTTL", pttl)
            if keys:
                pipeline_set(s, b"bulk", keys)
                print("ok   %d pipelined SETs" % keys)
            # timed, so a slow runner shows up as a number before it turns into
            # the socket timeout - it took over 10 s once on CI and under 1 s on
            # the rerun (TODO 606). Flushed, since CI holds stdout back otherwise
            started = time.monotonic()
            try:
                saved = command(s, "SAVE")
            except socket.timeout:
                print("SAVE had no reply after %.2f s" % (time.monotonic() - started), flush=True)
                raise
            print("SAVE took %.2f s" % (time.monotonic() - started), flush=True)
            check("SAVE", saved, "+OK")
        # no clean shutdown on purpose: what SAVE wrote has to be enough
        proc.kill()
        proc.wait()

        proc = start(exe, port, data, log, extra)
        with connect(port, proc) as s:
            check("GET after restart", command(s, "GET", "smoke"), "value")
            pttl = command(s, "PTTL", "expiring")
            if not (isinstance(pttl, int) and 0 < pttl <= 600000):
                raise AssertionError("PTTL after restart: got %r" % pttl)
            print("ok   PTTL after restart", pttl)
            for i in (0, keys // 2, keys - 1) if keys else ():
                check("GET bulk:%d after restart" % i, command(s, "GET", "bulk:%d" % i), "v%d" % i)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def main():
    exe = os.path.abspath(sys.argv[1])
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 14987
    root = tempfile.mkdtemp(prefix="barch-smoke-")
    # a file, not a pipe: barchd blocks once a pipe nobody reads fills up
    log_path = os.path.join(root, "barchd.log")
    log = open(log_path, "wb")
    try:
        plain = os.path.join(root, "plain")
        os.mkdir(plain)
        round_trip(exe, port, plain, log, [], "in memory arenas", 0)

        # arenas mapped from files, enough keys that the files have to grow while
        # they're mapped, and a restart that maps them back in
        mapped = os.path.join(root, "mapped")
        arenas = os.path.join(root, "arenas")
        os.mkdir(mapped)
        os.mkdir(arenas)
        round_trip(exe, port, mapped, log, ["--config", "arena_dir=" + arenas],
                   "file backed arenas", 50000)
        print("barchd smoke test passed")
    except Exception:
        log.flush()
        with open(log_path, "rb") as f:
            sys.stdout.write(f.read().decode(errors="replace")[-8000:])
        raise
    finally:
        log.close()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
