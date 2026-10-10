# RSS per key space with one key in it - TODO 639. Workbench only, delete before merging.
#
# Starts barchd, makes SPACES key spaces with one key each, and prints barchd's RSS
# and barch_vmm_bytes_allocated against a baseline with no extra spaces. Then SAVE,
# restart, touch every space so it loads, and print the same again: loading reads
# each saved page back whole, so that's the number most likely to differ.
#
#   python3 ci/lastpage-rss.py [BARCHD] [SPACES] [INTERNAL_SHARDS]
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import redis

HERE = os.path.dirname(os.path.abspath(__file__))
BINARY = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    HERE, "..", "cmake-build-lp-release", "barchd")
SPACES = int(sys.argv[2]) if len(sys.argv) > 2 else 200
SHARDS = sys.argv[3] if len(sys.argv) > 3 else None
PORT = int(os.environ.get("LP_PORT", "14639"))
DATA = tempfile.mkdtemp(prefix="lastpage-rss-")
LOG = os.path.join(DATA, "barchd.log")


def start():
    args = [BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
            "-c", "save_interval=86400000",
            "-c", "max_modifications_before_save=1000000000"]
    if SHARDS:
        args += ["-c", "internal_shards=" + SHARDS]
    # a file rather than a pipe nobody reads, which fills and hangs barchd
    p = subprocess.Popen(args, stdout=open(LOG, "ab"), stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise SystemExit("barchd exited with %s, see %s" % (p.returncode, LOG))
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise SystemExit("barchd did not listen on %d" % PORT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def rss(pid):
    with open("/proc/%d/statm" % pid) as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def vmm(r):
    for line in r.execute_command("INFO").decode().splitlines():
        if line.startswith("barch_vmm_bytes_allocated:"):
            return int(line.split(":")[1])
    return 0


def sample(p, r):
    time.sleep(0.5)
    return rss(p.pid), vmm(r)


def row(what, base, now):
    d_rss, d_vmm = now[0] - base[0], now[1] - base[1]
    print("%-28s rss %8.1f MB (+%7.1f KB/space)   vmm %8.1f MB (+%7.1f KB/space)" % (
        what, now[0] / 2**20, d_rss / 1024 / SPACES, now[1] / 2**20, d_vmm / 1024 / SPACES),
        flush=True)


def main():
    print("barchd %s, %d spaces, internal_shards %s" % (BINARY, SPACES, SHARDS or "default"))
    p = start()
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)
        base = sample(p, r)
        row("baseline", base, base)
        for i in range(SPACES):
            r.execute_command("USE lp%d" % i)
            r.execute_command("SET k v")
        r.execute_command("USE 0")
        row("one key per space", base, sample(p, r))
        r.execute_command("SAVE")
    finally:
        stop(p)

    p = start()
    try:
        r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120)
        # spaces may load at start-up, so the first run's empty baseline is the one to compare to
        for i in range(SPACES):
            r.execute_command("USE lp%d" % i)
            assert r.execute_command("GET k") == b"v", "space lp%d lost its key" % i
        r.execute_command("USE 0")
        row("after restart and load", base, sample(p, r))
    finally:
        stop(p)
        shutil.rmtree(DATA, ignore_errors=True)


main()
