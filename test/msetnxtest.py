# MSETNX checks and writes as one step - TODO 540.
#
# It checked every key in one pass and wrote them in a second, and each pass took
# one shard's latch at a time. A SET between the two passes was written over:
# MSETNX answered 1, as if none of its keys had existed, and the SET, which had
# also been acknowledged, was lost.
#
# Checked, against barchd: sixteen keys, most of them on different shards, and a
# SET of the last one racing each MSETNX. When MSETNX answers 1 and the SET is
# acknowledged too, the only order that fits is MSETNX first, so the SET's value
# has to be the one left.
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14540)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "msetnx_data")
ROUNDS = int(scale.env_float("BARCH_MSETNX_ROUNDS", 600, 100))

shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA,
                      "--no-save-on-exit"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    end = time.time() + 60
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end or p.poll() is not None:
                raise AssertionError("barchd did not start")
            time.sleep(0.1)

    a = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
    b = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
    keys = ["m%d" % i for i in range(16)]
    pairs = []
    for k in keys:
        pairs += [k, "a"]
    wins = lost = 0
    for _ in range(ROUNDS):
        a.delete(*keys)
        acked = []
        t = threading.Thread(target=lambda: acked.append(b.set(keys[-1], "b")))
        t.start()
        answered = a.execute_command("MSETNX", *pairs)
        t.join()
        if answered == 1:
            wins += 1
            if acked == [True] and a.get(keys[-1]) == b"a":
                lost += 1
    print("MSETNX answered 1 in %d of %d rounds; a concurrent SET was lost in %d" % (wins, ROUNDS, lost))
    assert wins > 0, "MSETNX never won a round, so this proved nothing"
    assert lost == 0, "MSETNX wrote over %d acknowledged SETs" % lost
finally:
    p.send_signal(signal.SIGKILL)
    p.wait(timeout=30)
print("msetnx checks pass")
