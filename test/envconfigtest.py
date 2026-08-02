# Configuration taken from the environment.
#
# Every setting answers to BARCH_ followed by its name in upper case, and the redis
# names work too with hyphens written as underscores. The values are in the form CONFIG
# SET takes, so BARCH_MAXMEMORY=100m means what it looks like.
#
# The environment is read when the module is imported, so a subprocess is the only way
# to test it - by the time this file is running its own import has already happened.
import os
import subprocess
import sys
import tempfile

# runs in the child: bring up a server and report what the settings ended up as
CHILD = r'''
import sys, redis, barch
PORT = int(sys.argv[1])
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2)
def cfg(n):
    res = r.execute_command("CONFIG", "GET", n)
    if isinstance(res, dict):
        return {k.decode(): v.decode() for k, v in res.items()}.get(n, "")
    return res[1].decode() if len(res) > 1 else ""
for name in sys.argv[2:]:
    print("RESULT %s=%s" % (name, cfg(name)))
r.close()
barch.stop()
'''


# the child runs from a file in a directory of its own rather than through -c. With -c
# python puts the working directory first on sys.path, and the build directory holds a
# barch.so belonging to another target, which shadows the real module and fails the
# import with "does not define module export function (PyInit_barch)". Running a script
# puts that script's directory first instead, and this one is empty.
_CHILD_DIR = tempfile.mkdtemp(prefix="barch-envcfg-")
_CHILD_PY = os.path.join(_CHILD_DIR, "child.py")
with open(_CHILD_PY, "w") as fh:
    fh.write(CHILD)


def settings(env, port, *names):
    e = dict(os.environ)
    e.update(env)
    out = subprocess.run([sys.executable, _CHILD_PY, str(port)] + list(names),
                         capture_output=True, text=True, env=e, cwd=os.getcwd())
    got = {}
    for line in out.stdout.split("\n"):
        if line.startswith("RESULT "):
            k, _, v = line[len("RESULT "):].partition("=")
            got[k] = v
    assert got, f"child produced no settings.\nstdout:\n{out.stdout[-2000:]}\nstderr:\n{out.stderr[-2000:]}"
    return got


print("start env config test")

# what the defaults are, so the assertions below prove the environment did something
base = settings({}, 14061, "max_scan_iterators", "max_memory_bytes", "eviction_policy")
assert base["max_scan_iterators"] != "42", "pick a different test value, 42 is the default"

# barch's own name
got = settings({"BARCH_MAX_SCAN_ITERATORS": "42"}, 14062, "max_scan_iterators")
assert got["max_scan_iterators"] == "42", got

# a redis name, and a redis byte unit with it. 256mb is binary, so 268435456
got = settings({"BARCH_MAXMEMORY": "256mb"}, 14063, "max_memory_bytes", "maxmemory")
assert got["max_memory_bytes"] == "268435456", got
assert got["maxmemory"] == "268435456", got

# a redis name whose value needs translating, written with an underscore for the hyphen
got = settings({"BARCH_MAXMEMORY_POLICY": "allkeys-lru"}, 14064,
               "eviction_policy", "maxmemory-policy")
assert got["eviction_policy"] == "allkeys-lru", got
assert got["maxmemory-policy"] == "allkeys-lru", got

# barch's own name wins when a setting is exported under both
got = settings({"BARCH_MAXMEMORY": "100mb", "BARCH_MAX_MEMORY_BYTES": "200000000"},
               14065, "max_memory_bytes")
assert got["max_memory_bytes"] == "200000000", \
    f"barch's own name should win over the redis alias, got {got}"

# a setting barch reports but cannot change is refused, and does not stop the rest
got = settings({"BARCH_APPENDONLY": "yes", "BARCH_MAX_SCAN_ITERATORS": "37"}, 14066,
               "appendonly", "max_scan_iterators")
assert got["appendonly"] == "no", f"appendonly is not settable, got {got}"
assert got["max_scan_iterators"] == "37", f"a refusal should not stop the rest, got {got}"

# and a value the setter will not take is refused without taking the process with it
got = settings({"BARCH_MAX_SCAN_ITERATORS": "not-a-number"}, 14067, "max_scan_iterators")
assert got["max_scan_iterators"] == base["max_scan_iterators"], \
    f"a bad value should leave the default in place, got {got}"

print("complete env config test")
