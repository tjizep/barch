# CONFIG SET against the space registry - TODO 553.
#
# Opening a space takes the registry lock and then config_mutex (the key_space
# constructor reads aof_dir). ApplyEvictionType and SetOrderedKeys used to take
# them the other way round: config_mutex first, then the registry lock to see if
# the default space is open. A space opening while one of those runs could
# deadlock. TSan reports the cycle as soon as it has seen both orders, even on
# one thread; other builds only catch it when the timing actually lines up.
#
# TSan reports a pair of mutexes once per process, so each setting gets its own
# child - otherwise a fix for one would hide the other.
#
# It isn't only a report: a background thread opening the configuration space
# while CONFIG SET runs deadlocks the process, so a child that hangs fails too.
import os
import subprocess
import sys

SETTINGS = {
    "eviction_policy": "allkeys-lru",
    "ordered_keys": "yes",
}

if len(sys.argv) > 1:
    import scale
    import barch

    scale.workdir()
    name = sys.argv[1]
    barch.KeyValue()                      # the default space is open
    barch.setConfiguration(name, SETTINGS[name])
    barch.KeyValue("lockorder")           # and a new one opens after
    print(name, "done", flush=True)
    sys.exit(0)

print("start config lock order test", flush=True)
failed = []
for name in SETTINGS:
    try:
        r = subprocess.run([sys.executable, os.path.abspath(__file__), name], timeout=120)
    except subprocess.TimeoutExpired:
        failed.append((name, "hung"))
        continue
    if r.returncode != 0:
        failed.append((name, r.returncode))
assert not failed, f"children failed: {failed}"
print("complete config lock order test", flush=True)
