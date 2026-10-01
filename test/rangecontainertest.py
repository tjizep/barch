# Lists, hashes and ordered sets are refused on a range sharded space - TODO 569.
#
# Container commands route by the bare name, but the rebalancer cuts shards by
# stored key, and a container's stored keys start with a lead byte the name
# doesn't have. So the rebalancer moved a container's entries away from the
# shard its name routes to: an RPUSH of 800 items read back LLEN 800 and then 0
# a few seconds later, with nothing logged. Until containers route by the key
# the rebalancer sorts by, every container command refuses a range space.
#
# Four parts:
#   1. every list, hash and ordered set command in the source answers the
#      refusal on a range space, and nothing gets created
#   2. the command table below covers every one the source registers, so a new
#      container command can't slip past it
#   3. a hash sharded space still takes the same commands
#   4. a hash space holding containers, converted to range, says at load that
#      they can't be reached
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")

scale.workdir()
PORT = scale.port(default=14980)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "rangecontainer_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
LOG = os.path.join(os.getcwd(), "rangecontainer.log")

REFUSAL = "range sharded"

# one call per registered command. The arguments are the ones each command
# would take for real, so that before the fix every write here succeeds
COMMANDS = {
    # lists
    "LPUSH": ["L", "a"], "RPUSH": ["L", "a"], "LPUSHX": ["L", "a"], "RPUSHX": ["L", "a"],
    "LPOP": ["L"], "RPOP": ["L"], "LBACK": ["L"], "LFRONT": ["L"], "LLEN": ["L"],
    "LRANGE": ["L", "0", "-1"], "LINSERT": ["L", "BEFORE", "a", "b"],
    "LMPOP": ["1", "L", "LEFT"], "LMOVE": ["L", "L2", "LEFT", "RIGHT"],
    "RPOPLPUSH": ["L", "L2"], "BLPOP": ["L", "0.1"], "BRPOP": ["L", "0.1"],
    "BLMPOP": ["0.1", "1", "L", "LEFT"], "BLMOVE": ["L", "L2", "LEFT", "RIGHT", "0.1"],
    "BRPOPLPUSH": ["L", "L2", "0.1"],
    # hashes
    "HSET": ["H", "f", "v"], "HMSET": ["H", "f", "v"], "HSETNX": ["H", "f", "v"],
    "HINCRBY": ["H", "n", "1"], "HINCRBYFLOAT": ["H", "x", "1.5"],
    "HGET": ["H", "f"], "HMGET": ["H", "f"], "HGETALL": ["H"], "HKEYS": ["H"],
    "HVALS": ["H"], "HLEN": ["H"], "HEXISTS": ["H", "f"], "HSTRLEN": ["H", "f"],
    "HRANDFIELD": ["H"], "HSCAN": ["H", "0"], "HDEL": ["H", "f"],
    "HGETDEL": ["H", "FIELDS", "1", "f"], "HEXPIRE": ["H", "100", "FIELDS", "1", "f"],
    "HEXPIREAT": ["H", "9999999999", "FIELDS", "1", "f"],
    "HTTL": ["H", "FIELDS", "1", "f"], "HEXPIRETIME": ["H", "FIELDS", "1", "f"],
    # ordered sets
    "ZADD": ["Z", "1", "a"], "ZINCRBY": ["Z", "1", "b"], "ZREM": ["Z", "a"],
    "ZCARD": ["Z"], "ZCOUNT": ["Z", "-inf", "+inf"], "ZSCORE": ["Z", "a"],
    "ZMSCORE": ["Z", "a"], "ZRANK": ["Z", "a"], "ZREVRANK": ["Z", "a"],
    "ZFASTRANK": ["Z", "a"], "ZRANDMEMBER": ["Z"],
    "ZRANGE": ["Z", "0", "-1"], "ZREVRANGE": ["Z", "0", "-1"],
    "ZRANGEBYSCORE": ["Z", "-inf", "+inf"], "ZREVRANGEBYSCORE": ["Z", "+inf", "-inf"],
    "ZRANGEBYLEX": ["Z", "-", "+"], "ZREVRANGEBYLEX": ["Z", "+", "-"],
    "ZLEXCOUNT": ["Z", "-", "+"], "ZRANGESTORE": ["D1", "Z", "0", "-1"],
    "ZREMRANGEBYSCORE": ["Z", "-inf", "+inf"], "ZREMRANGEBYLEX": ["Z", "-", "+"],
    "ZREMRANGEBYRANK": ["Z", "0", "-1"],
    "ZUNION": ["1", "Z"], "ZINTER": ["1", "Z"], "ZDIFF": ["1", "Z"],
    "ZINTERCARD": ["1", "Z"], "ZUNIONSTORE": ["D2", "1", "Z"],
    "ZINTERSTORE": ["D3", "1", "Z"], "ZDIFFSTORE": ["D4", "1", "Z"],
    "ZPOPMIN": ["Z"], "ZPOPMAX": ["Z"], "ZMPOP": ["1", "Z", "MIN"],
    "BZPOPMIN": ["Z", "0.1"], "BZPOPMAX": ["Z", "0.1"], "BZMPOP": ["0.1", "1", "Z", "MIN"],
}

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def registered():
    """every command the source registers under list, hash or orderedset"""
    found = set()
    pat = re.compile(r'r\["([A-Z]+)"\]\s*=\s*\{[^}]*\{[^}]*"(list|hash|orderedset)"')
    for f in ("list_api.cpp", "hash_api.cpp", "ordered_api.cpp"):
        with open(os.path.join(SRC, f)) as src:
            for line in src:
                if line.lstrip().startswith("//"):
                    continue
                m = pat.search(line)
                if m:
                    found.add(m.group(1))
    return found


def start():
    log = open(LOG, "a")
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                         stdout=log, stderr=subprocess.STDOUT)
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not listen on %d" % PORT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=30)


def client():
    return redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)


def make_space(r, name, ranged):
    r.execute_command("configuration:SET", name + ".ordered", "1")
    r.execute_command("configuration:SET", name + ".shards", "4")
    r.execute_command("configuration:SET", name + ".range_sharded", "1" if ranged else "0")
    r.execute_command("configuration:SAVE")


print("TestRangeContainers")

# --- 2. the table covers the source ----------------------------------------------
in_source = registered()
missing = sorted(in_source - set(COMMANDS))
check(len(in_source) > 50, "found the container commands in the source (%d)" % len(in_source))
check(not missing, "every registered container command is tested %s" % (missing or ""))

proc = start()
try:
    r = client()
    make_space(r, "rc", True)
    make_space(r, "hc", False)

    # --- 1. a range space refuses every one -----------------------------------------
    r.execute_command("USE", "rc")
    not_refused = []
    for name, args in sorted(COMMANDS.items()):
        try:
            r.execute_command(name, *args)
            not_refused.append(name)
        except redis.exceptions.ResponseError as e:
            if REFUSAL not in str(e):
                not_refused.append("%s (%s)" % (name, e))
    check(not not_refused, "range space refuses all %d container commands %s"
          % (len(COMMANDS), not_refused[:8] if not_refused else ""))
    check(r.dbsize() == 0, "nothing was created on the range space (DBSIZE %d)" % r.dbsize())
    # plain keys go on working: the refusal is only for containers
    r.set("plain", "v")
    check(r.get("plain") == b"v", "plain keys still work on the range space")
    r.delete("plain")

    # --- 3. a hash space takes them -------------------------------------------------
    r.execute_command("USE", "hc")
    r.rpush("L", *["e%04d" % i for i in range(800)])
    r.hset("H", mapping={"f%04d" % i: "v" for i in range(800)})
    r.zadd("Z", {"m%04d" % i: i for i in range(800)})
    check(r.llen("L") == 800 and r.hlen("H") == 800 and r.zcard("Z") == 800,
          "hash space holds a list, a hash and an ordered set")
    r.execute_command("SAVE")
    # --- 4. turned into a range space, it says at load what can't be reached -------
    r.execute_command("configuration:SET", "hc.range_sharded", "1")
    r.execute_command("configuration:SAVE")
finally:
    stop(proc)

with open(LOG, "w"):
    pass
proc = start()
try:
    r = client()
    r.execute_command("USE", "hc")
    try:
        r.llen("L")
        refused = False
    except redis.exceptions.ResponseError as e:
        refused = REFUSAL in str(e)
    check(refused, "the converted space refuses LLEN rather than answering wrong")
finally:
    stop(proc)
with open(LOG, errors="replace") as f:
    log = f.read()
check("lists, hashes or ordered sets" in log and "hc" in log,
      "the load said the converted space holds containers it can't reach")

print("%s: %d failure(s)" % ("FAIL" if failures else "PASS", failures))
sys.exit(1 if failures else 0)
