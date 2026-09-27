import time

import scale
import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

# Guards defrag over a page holding a tombstone - TODO 364.
#
# A space that depends on another answers a delete of a key that only its source has
# by writing a tombstone: a leaf that hides the source's key. Defrag lifts every live
# leaf off a fragmented page and puts it back somewhere else, and checked each lift by
# watching get_size() drop by one. get_size() is the tree size minus the tombstones,
# plus the source's size, so lifting a tombstone - which takes one off both - leaves it
# where it was, and defrag aborted the process: "key not marked as deleted but it was
# not found". That's the rare SIGABRT redispytest hit in full suite runs.
#
# Checked here: a tombstone on a fragmented page survives defrag - the server doesn't
# abort, the source's key stays hidden, and the space's size is still right - with
# writes landing in the source while defrag runs, since the source's size was in the
# check too.

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


print("start defrag tomb test")
barch.setConfiguration("maintenance_poll_delay", "40")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
try:
    r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2, socket_timeout=60)
    r.execute_command("CLEARALL")
    for i in range(20):
        r.execute_command(f"src:SET z{i} src{i}")
    r.execute_command("SPACES DEPENDS dep ON src")

    # the tombstones first, so they sit on the first pages dep writes
    for i in range(20):
        r.execute_command(f"dep:REM z{i}")
        assert r.execute_command(f"dep:GET z{i}") is None, "REM in dep didn't hide the key"
    # then enough to fill pages, and most of it deleted again so they fragment
    for i in range(FILL):
        r.execute_command(f"dep:SET f{i:05d} {'v' * 120}")
    for i in range(KEEP, FILL):
        r.execute_command(f"dep:DEL f{i:05d}")
    expect = r.execute_command("dep:DBSIZE")

    before = stat(r, "pages_defragged")
    deadline = time.monotonic() + 20
    n = 0
    while stat(r, "pages_defragged") == before and time.monotonic() < deadline:
        # the source keeps changing while defrag runs over dep
        r.execute_command(f"src:SET w{n} x")
        n += 1
    defragged = stat(r, "pages_defragged") - before
    print(f"defrag moved {defragged} pages; {n} writes to src meanwhile")
    assert defragged > 0, "defrag never ran over the fragmented pages"
    time.sleep(0.5)                 # a few more ticks

    for i in range(20):
        got = r.execute_command(f"dep:GET z{i}")
        assert got is None, f"after defrag dep:GET z{i} is {got!r} - the tombstone came back as a key"
    assert r.execute_command("src:GET z0") == b"src0", "the source lost its key"
    size = r.execute_command("dep:DBSIZE")
    assert size == expect + n, (f"dep's DBSIZE is {size} after defrag, expected {expect} "
                                f"plus the {n} keys written to its source")
    for i in range(KEEP):
        assert r.execute_command(f"dep:GET f{i:05d}") == b"v" * 120, f"f{i:05d} lost in defrag"
    r.close()
finally:
    barch.stop()
print("complete defrag tomb test")
