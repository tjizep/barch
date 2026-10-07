import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# active_defrag changed while the server runs reaches the spaces it already has -
# TODO 605.
#
# Each shard used to copy the setting when it was built, and maintenance only read
# that copy, so CONFIG SET active_defrag changed nothing for a space that already
# existed. Turning it off didn't stop defrag there, and turning it on again (after
# starting with it off) didn't start it.
#
# Checked here on one space made before either change: with defrag off, fragmented
# pages stay as they are for a good number of maintenance ticks; turned back on, it
# runs over them.

PORT = scale.port(default=14000)
FILL = 4000
KEEP = 50


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        k = s[i].decode() if isinstance(s[i], bytes) else str(s[i])
        if k.lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


def wait_cycles(r, n, limit=30):
    """until maintenance has ticked n more times, so a pass had every chance to run"""
    start = stat(r, "maintenance_cycles")
    deadline = time.monotonic() + limit
    while stat(r, "maintenance_cycles") - start < n and time.monotonic() < deadline:
        time.sleep(0.05)
    return stat(r, "maintenance_cycles") - start


print("start defrag toggle test")
barch.setConfiguration("active_defrag", "on")
barch.setConfiguration("maintenance_poll_delay", "40")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    # made while defrag is on, so its shards start out with it on
    r.execute_command("toggle:SET first x")

    # off, then pages that need defragging
    r.execute_command("CONFIG", "SET", "active_defrag", "off")
    for i in range(FILL):
        r.execute_command(f"toggle:SET f{i:05d} {'v' * 120}")
    for i in range(KEEP, FILL):
        r.execute_command(f"toggle:DEL f{i:05d}")

    before = stat(r, "pages_defragged")
    ticks = wait_cycles(r, 300)
    moved = stat(r, "pages_defragged") - before
    assert moved == 0, (f"active_defrag is off, but defrag moved {moved} pages over "
                        f"{ticks} maintenance ticks in a space made before it was turned off")

    # and on again
    r.execute_command("CONFIG", "SET", "active_defrag", "on")
    before = stat(r, "pages_defragged")
    deadline = time.monotonic() + 20
    while stat(r, "pages_defragged") == before and time.monotonic() < deadline:
        time.sleep(0.05)
    moved = stat(r, "pages_defragged") - before
    assert moved > 0, "active_defrag is back on, but defrag never ran over the fragmented pages"

    for i in range(KEEP):
        assert r.execute_command(f"toggle:GET f{i:05d}") == b"v" * 120, f"f{i:05d} lost in defrag"
    r.close()
finally:
    barch.setConfiguration("active_defrag", "on")
    barch.stop()
print("complete defrag toggle test")
