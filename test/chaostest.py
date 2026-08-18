# Mix as many concurrent RESP calls as the box will reasonably hold, flip
# memory limits under them, and stop the server while calls are still in
# flight. The mix is strings and TTL, hashes, lists, ordered sets,
# RANGE/COUNT, n-gram composite keys (docs/index.html #ref-ngram), and H3-style
# numeric composites. KEYS still writes the socket as it walks, so a
# restart mid-KEYS is the original case.
#
# Disconnects, timeouts and WRONGTYPE during the storm are allowed.
# After a quiet period the server has to answer SET, GET, KEYS and a
# gram RANGE again.
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
    sk = lambda i: "t%d:s:%d" % (tid, i)
    hk = "t%d:h" % tid
    lk = "t%d:l" % tid
    zk = "t%d:z" % tid
    ck = "t%d:c" % tid
    fk = "t%d:f" % tid
    grams = ("This ", "his i", "is is", "s is ", " is a", " a do")

    def fire():
        i = local.randrange(n)
        j = local.randrange(n)
        gram = grams[local.randrange(len(grams))]
        h3 = 600000000000000000 + tid * 1000 + i
        ops = (
            lambda: r.set(sk(i), "v%d" % i),
            lambda: r.get(sk(i)),
            lambda: r.delete(sk(i)),
            lambda: r.exists(sk(i)),
            lambda: r.append(sk(i), "x"),
            lambda: r.strlen(sk(i)),
            lambda: r.getrange(sk(i), 0, 3),
            lambda: r.setrange(sk(i), 0, "V"),
            lambda: r.setnx(sk(i), "nx"),
            lambda: r.getset(sk(i), "gs"),
            lambda: r.mset({sk(i): "m", sk(j): "n"}),
            lambda: r.mget(sk(i), sk(j)),
            lambda: r.incr(ck),
            lambda: r.incrby(ck, 2),
            lambda: r.decr(ck),
            lambda: r.decrby(ck, 2),
            lambda: r.incrbyfloat(fk, 0.5),
            lambda: r.expire(sk(i), 30),
            lambda: r.ttl(sk(i)),
            lambda: r.pttl(sk(i)),
            lambda: r.persist(sk(i)),
            lambda: r.execute_command("GETDEL", sk(i)),
            lambda: r.execute_command("GETEX", sk(i), "EX", 20),
            lambda: r.execute_command("SETEX", sk(i), 20, "ex"),
            lambda: r.execute_command("PREPEND", sk(i), "p"),
            lambda: r.execute_command("LENGTH", sk(i)),
            lambda: r.execute_command("RANGE", sk(0), sk(n - 1), 20),
            lambda: r.execute_command("COUNT", sk(0), sk(n - 1)),
            lambda: r.execute_command("MIN"),
            lambda: r.execute_command("MAX"),
            lambda: r.execute_command("LB", sk(i)),
            lambda: r.execute_command("UB", sk(i)),
            lambda: r.keys("t%d:s:*" % tid),
            lambda: r.execute_command("KEYS", "t*:*", "COUNT"),
            lambda: r.execute_command("VALUES", "v*", "COUNT"),
            lambda: r.scan(0, match="t%d:*" % tid, count=20),
            lambda: r.dbsize(),
            lambda: r.ping(),
            lambda: r.execute_command("INFO", "memory"),
            lambda: r.execute_command("STATS"),
            lambda: r.execute_command("SIZE"),
            lambda: r.execute_command("RANDOMKEY"),
            lambda: r.hset(hk, "f%d" % i, "v%d" % i),
            lambda: r.hget(hk, "f%d" % i),
            lambda: r.hmget(hk, "f%d" % i, "f%d" % j),
            lambda: r.hgetall(hk),
            lambda: r.hdel(hk, "f%d" % j),
            lambda: r.hexists(hk, "f%d" % i),
            lambda: r.hlen(hk),
            lambda: r.hkeys(hk),
            lambda: r.hvals(hk),
            lambda: r.hincrby(hk, "n", 1),
            lambda: r.hincrbyfloat(hk, "nf", 0.25),
            lambda: r.hsetnx(hk, "x%d" % i, "1"),
            lambda: r.hstrlen(hk, "f%d" % i),
            lambda: r.rpush(lk, "x%d" % i),
            lambda: r.lpush(lk, "y%d" % i),
            lambda: r.rpop(lk),
            lambda: r.lpop(lk),
            lambda: r.llen(lk),
            lambda: r.lrange(lk, 0, 4),
            lambda: r.zadd(zk, {"m%d" % i: i}),
            lambda: r.zrange(zk, 0, 4),
            lambda: r.zcard(zk),
            lambda: r.zscore(zk, "m%d" % i),
            lambda: r.zrank(zk, "m%d" % i),
            lambda: r.zrem(zk, "m%d" % j),
            lambda: r.zcount(zk, 0, n),
            lambda: r.zincrby(zk, 1, "m%d" % i),
            lambda: r.execute_command("txt:SET", "%s|%d" % (gram, i), "1"),
            lambda: r.execute_command("txt:RANGE", gram + "|0", gram + "|999999", 20),
            lambda: r.execute_command("txt:COUNT", gram + "|0", gram + "|999999"),
            lambda: r.execute_command("txt:REM", "%s|%d" % (gram, j)),
            lambda: r.execute_command("spatial_data:SET", "%d p%d" % (h3, tid), "1"),
            lambda: r.execute_command(
                "spatial_data:RANGE",
                "%d 0" % (h3 - 50),
                "%d 99999999999999" % (h3 + 50),
                10,
            ),
            lambda: r.execute_command("COPY", sk(i), "t%d:cp:%d" % (tid, i)),
            lambda: r.execute_command("RENAMENX", sk(i), "t%d:rn:%d" % (tid, i)),
        )
        ops[local.randrange(len(ops))]()

    while not done.is_set():
        if restarting.is_set():
            time.sleep(0.02)
            continue
        try:
            fire()
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
ctl.execute_command("USE", "configuration")
ctl.set("txt.key_split", "|")
ctl.execute_command("SAVE")
ctl.execute_command("USE")
for i in range(SEED_KEYS):
    ctl.set("seed:%05d" % i, "s%d" % i)
for i, gram in enumerate(("This ", "his i", "is is", "s is ")):
    ctl.execute_command("txt:SET", "%s|%d" % (gram, i), "1")
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
quiet.execute_command("txt:SET", "is is|2", "1")
grams = quiet.execute_command("txt:RANGE", "is is|0", "is is|999999", 100)
assert grams, "txt:RANGE after the storm answered empty"
quiet.close()

assert not failures, "chaos failures: %s" % failures[:12]
barch.stop()
print("complete chaos test")
