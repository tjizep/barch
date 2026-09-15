# Traffic capture, ranges and listings all at once - the thread interleavings.
#
# Written as a TSan probe for TODO 325 and kept as a test, because the short set
# is what the sanitizer CI job runs and none of the rest of it turns capture on.
# There is nothing here a single thread can fail: it asserts only that the server
# survives and the clients see no errors. What it is for is giving TSan something
# to watch.
#
# What each thread is for:
#
#   - traffic capture: one file per thread, a registry lock, and capture_changed
#     flushing a file its owner may be writing (flockfile). The flipper thread is
#     the point: it turns capture on and off underneath eight busy writers.
#   - sharded_store::range: the new k-way merge, read concurrently with writes.
#   - fs::list: the adaptive batch, over a tree being changed underneath it.
#   - inner_lower_bound: reached by every range and every LB here.
import os, random, sys, threading, time
sys.path.insert(0, "/home/test/barch/test")
import scale, redis, barch

scale.workdir()
PORT = scale.port(default=15050)
barch.start("0.0.0.0", PORT)
ctl = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
# the first CONFIG SET straight after barch.start can be refused - see TODO 326 -
# and a read first makes it stick, which is also a fair thing for a test to do
ctl.execute_command("CONFIG", "GET", "traffic_*")
ctl.config_set("traffic_file", "probe_traffic.dat")
ctl.execute_command("USE", "configuration")
ctl.set("probe.shards", "7")
ctl.execute_command("USE", "probe")

# a few seconds is plenty under a sanitizer, where everything is ten times slower
SECONDS = scale.env_float("PROBE_SECONDS", scale.scaled_seconds(12.0, 2.0))
stop = threading.Event()
errors = []


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)


def guard(fn):
    def run(*a):
        try:
            fn(*a)
        except Exception as e:                      # noqa: BLE001 - reported below
            errors.append(f"{fn.__name__}: {e}")
    return run


@guard
def writer(n):
    r = client()
    r.execute_command("USE", "probe")
    i = 0
    while not stop.is_set():
        i += 1
        r.set("k%03d:%06d" % (n, i), "v" * (i % 97))
        if i % 5 == 0:
            r.get("k%03d:%06d" % (n, i // 2))
        if i % 11 == 0:
            r.execute_command("REM", "k%03d:%06d" % (n, i // 3))


@guard
def ranger(n):
    r = client()
    r.execute_command("USE", "probe")
    rnd = random.Random(n)
    while not stop.is_set():
        lo = "k%03d:%06d" % (rnd.randrange(8), rnd.randrange(500))
        r.execute_command("RANGE", lo, "l", rnd.choice((5, 50, 400)))
        r.execute_command("LB", lo)


@guard
def lister(n):
    r = client()
    r.execute_command("USE", "probe")
    i = 0
    while not stop.is_set():
        i += 1
        r.execute_command("FS", "PUT", f"/d{i % 7}/f{i}", "x" * 32)
        r.execute_command("FS", "LS", f"/d{i % 7}")
        r.execute_command("FS", "LS", "/")


@guard
def flipper():
    r = client()
    while not stop.is_set():
        r.config_set("traffic_capture", "on")
        time.sleep(0.05)
        r.config_set("traffic_capture", "off")      # closes files other threads write
        time.sleep(0.05)


threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
threads += [threading.Thread(target=ranger, args=(n,)) for n in range(2)]
threads += [threading.Thread(target=lister, args=(n,)) for n in range(1)]
threads += [threading.Thread(target=flipper)]
for t in threads:
    t.start()
time.sleep(SECONDS)
stop.set()
for t in threads:
    t.join()
# what a connection that sat idle through all of that can still do - the first
# run of this probe died here, so it is worth knowing which part fails
for what, fn in (("ping", lambda: ctl.execute_command("PING")),
                 ("get", lambda: ctl.get("k000:000001")),
                 ("config get", lambda: ctl.execute_command("CONFIG", "GET", "traffic_capture")),
                 ("config set", lambda: ctl.config_set("traffic_capture", "off"))):
    try:
        fn()
        print(f"  idle connection {what}: ok")
    except Exception as e:                          # noqa: BLE001
        print(f"  idle connection {what}: FAILED {e}")
fresh = client()
try:
    fresh.config_set("traffic_capture", "off")
    print("  fresh connection config set: ok")
except Exception as e:                              # noqa: BLE001
    print(f"  fresh connection config set: FAILED {e}")
files = [f for f in os.listdir(".") if "traffic" in f and f.endswith(".dat")]
print(f"{len(files)} traffic files written, {len(errors)} client errors")
for e in errors[:5]:
    print("  ", e)
assert not errors, f"{len(errors)} client errors, first: {errors[0]}"
assert files, "capture was on and wrote nothing"
assert ctl.execute_command("PING"), "the server did not survive"
print("traffic race probe ok")
