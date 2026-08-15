# Mix as many concurrent RESP calls as the box will reasonably hold, flip
# memory limits under them, and stop the server while calls are still in
# flight. KEYS walks pages on worker threads and writes the socket as it
# goes, so a restart mid-KEYS is the case this is for.
#
# Disconnects, timeouts and WRONGTYPE during the storm are allowed.
# After a quiet period the server has to answer SET, GET and KEYS again.
import os
import random
import threading
import time

import redis
import barch

PORT = 14083
SEED = int(os.environ.get("CHAOS_SEED", str(int(time.time()) & 0xFFFFFFFF)))
WORKERS = min(32, max(8, (os.cpu_count() or 4) * 4))
SECONDS = float(os.environ.get("CHAOS_SECONDS", "8"))
RESTARTS = int(os.environ.get("CHAOS_RESTARTS", "2"))
SEED_KEYS = 400

TRANSIENT = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.BusyLoadingError,
    OSError,
    ConnectionResetError,
    BrokenPipeError,
)

print("start chaos test seed %s workers %s seconds %s restarts %s" % (
    SEED, WORKERS, SECONDS, RESTARTS))

rng = random.Random(SEED)
done = threading.Event()
restarting = threading.Event()
failures = []
failures_lock = threading.Lock()


def connect():
    return redis.Redis(
        host="127.0.0.1",
        port=PORT,
        protocol=2,
        socket_timeout=2,
        socket_connect_timeout=2,
    )


def record(err):
    with failures_lock:
        failures.append(err)


def reconnect(old):
    try:
        old.close()
    except Exception:
        pass
    return connect()


def start_server():
    for _ in range(20):
        try:
            barch.start("0.0.0.0", PORT)
            c = connect()
            c.ping()
            c.close()
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not start on %s" % PORT)


def worker(tid):
    local = random.Random(SEED + tid * 7919)
    r = connect()
    n = 80
    while not done.is_set():
        if restarting.is_set():
            time.sleep(0.02)
            continue
        op = local.randrange(12)
        i = local.randrange(n)
        try:
            if op == 0:
                r.set("t%d:s:%d" % (tid, i), "v%d" % i)
            elif op == 1:
                r.get("t%d:s:%d" % (tid, i))
            elif op == 2:
                r.delete("t%d:s:%d" % (tid, i))
            elif op == 3:
                r.incr("t%d:c" % tid)
            elif op == 4:
                r.keys("t%d:s:*" % tid)
            elif op == 5:
                r.execute_command("KEYS", "t*:*", "COUNT")
            elif op == 6:
                r.execute_command("VALUES", "v*", "COUNT")
            elif op == 7:
                r.scan(0, match="t%d:*" % tid, count=20)
            elif op == 8:
                r.hset("t%d:h" % tid, "f%d" % i, "v%d" % i)
                r.hget("t%d:h" % tid, "f%d" % i)
            elif op == 9:
                r.rpush("t%d:l" % tid, "x%d" % i)
                r.lrange("t%d:l" % tid, 0, 4)
            elif op == 10:
                r.zadd("t%d:z" % tid, {"m%d" % i: i})
                r.zrange("t%d:z" % tid, 0, 4)
            else:
                pipe = r.pipeline(transaction=False)
                pipe.get("t%d:s:%d" % (tid, i))
                pipe.keys("t%d:s:%d" % (tid, i))
                pipe.execute()
        except TRANSIENT:
            r = reconnect(r)
        except redis.exceptions.ResponseError:
            pass
        except Exception as e:
            record("%s in worker %s: %s" % (type(e).__name__, tid, e))
            r = reconnect(r)
    try:
        r.close()
    except Exception:
        pass


def flip_config():
    r = connect()
    mems = ("67108864", "134217728", "18446744073709551615")
    thresh = ("0.5", "0.75", "0.85")
    while not done.is_set():
        if restarting.is_set():
            time.sleep(0.02)
            continue
        try:
            r.execute_command("CONFIG", "SET", "max_memory_bytes", rng.choice(mems))
            r.execute_command("CONFIG", "SET", "pre_evict_thresh", rng.choice(thresh))
        except TRANSIENT:
            r = reconnect(r)
        except redis.exceptions.ResponseError:
            pass
        except Exception as e:
            record("config: %s" % e)
            r = reconnect(r)
        time.sleep(0.15)
    try:
        r.close()
    except Exception:
        pass


def restarter():
    slice_s = SECONDS / max(RESTARTS, 1)
    for _ in range(RESTARTS):
        time.sleep(slice_s)
        if done.is_set():
            return
        restarting.set()
        time.sleep(0.05)
        try:
            barch.stop()
        except Exception as e:
            record("stop: %s" % e)
        time.sleep(0.15)
        try:
            start_server()
        except Exception as e:
            record("start: %s" % e)
        restarting.clear()


start_server()
ctl = connect()
ctl.execute_command("FLUSHDB")
for i in range(SEED_KEYS):
    ctl.set("seed:%05d" % i, "s%d" % i)
ctl.close()

threads = [threading.Thread(target=worker, args=(i,), name="chaos-%d" % i)
           for i in range(WORKERS)]
threads.append(threading.Thread(target=flip_config, name="chaos-cfg"))
threads.append(threading.Thread(target=restarter, name="chaos-restart"))
for t in threads:
    t.start()

time.sleep(SECONDS + 0.5)
done.set()
for t in threads:
    t.join(timeout=15)
    if t.is_alive():
        record("thread %s did not exit" % t.name)

if restarting.is_set():
    try:
        start_server()
    except Exception as e:
        record("final start: %s" % e)
    restarting.clear()

# a quiet period: the storm is over, the server has to be a server
quiet = None
for _ in range(30):
    try:
        quiet = connect()
        quiet.ping()
        break
    except TRANSIENT:
        time.sleep(0.1)
        quiet = None
assert quiet is not None, "server did not answer after the storm: %s" % failures

try:
    quiet.execute_command("CONFIG", "SET", "max_memory_bytes", "18446744073709551615")
except redis.exceptions.ResponseError:
    pass
quiet.set("chaos:final", "ok")
assert quiet.get("chaos:final") == b"ok", quiet.get("chaos:final")
got = quiet.keys("chaos:*")
names = [k.decode() if isinstance(k, bytes) else k for k in got]
assert "chaos:final" in names, names
quiet.close()

assert not failures, "chaos failures: %s" % failures[:12]
barch.stop()
print("complete chaos test")
