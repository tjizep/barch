"""How big a test should be.

The suite takes about 510 seconds uninstrumented and several times that under a
sanitizer, which is why TSan only ever got pointed at chaostest by hand. Rather
than keep a second set of shortened tests in step with the real ones, the tests
read their size from here and the size comes from the environment:

    BARCH_TEST_SCALE=0.05 ctest -L short

Unset means 1.0, so a normal run is exactly what it was. See TODO 206.

Tests that already had their own knob keep it, and an explicitly set one wins -
`BARCH_PERF_ENTRIES=50000` still means fifty thousand whatever the scale says.
That is what `env_int` and `env_float` below are for.
"""

import os
import sys
import uuid

_workdir = ""


def scale() -> float:
    """The multiplier. 1.0 unless BARCH_TEST_SCALE says otherwise."""
    try:
        s = float(os.environ.get("BARCH_TEST_SCALE", "1"))
    except ValueError:
        return 1.0
    # a negative or zero scale would mean "run nothing", which is never what
    # anyone wants from a test - treat it as unset
    return s if s > 0 else 1.0


def short() -> bool:
    """True when this is a shortened run."""
    return scale() < 1.0


def scaled(n: int, floor: int = 1) -> int:
    """`n` scaled, never below `floor`.

    The floor matters: a count that scales to zero does not run a small version
    of the test, it runs no version of it, and still reports success.
    """
    return max(floor, int(round(n * scale())))


def scaled_seconds(s: float, floor: float = 1.0) -> float:
    """A duration scaled, with the same floor argument as `scaled`."""
    return max(floor, s * scale())


def env_int(name: str, default: int, floor: int = 1) -> int:
    """An explicit environment value, else `default` scaled."""
    v = os.environ.get(name)
    if v is not None:
        return int(v)
    return scaled(default, floor)


def env_float(name: str, default: float, floor: float = 0.0) -> float:
    """An explicit environment value, else `default` scaled."""
    v = os.environ.get(name)
    if v is not None:
        return float(v)
    return max(floor, default * scale())


def port(offset: int = 0, default: int = 14000) -> int:
    """The port this test should bind.

    ctest hands each test its own base in BARCH_TEST_PORT so they can run
    together - 34 test files had a port literal in them and eighteen of those
    were 14000. Unset means the old literal, so running a test by hand is
    unchanged.

    `offset` is for tests that need more than one: each test owns a block, so
    port(0) and port(1) are both its own.
    """
    base = os.environ.get("BARCH_TEST_PORT")
    if base is None:
        return default + offset
    return int(base) + offset


def workdir(name: str = "", unique: bool = False) -> str:
    """Make a directory for this test and work in it.

    barch writes its shards to the current directory, so two tests in one
    directory fight over the files and a test run twice reads what the last run
    left. Doing it here rather than through ctest's WORKING_DIRECTORY means the
    isolation belongs to the test: running it by hand lands in its own directory
    too, instead of scattering .dat files through the build root.

    Named after the test, not a uuid, so a failure can be looked at afterwards
    and a hundred runs do not leave a hundred directories. BARCH_TEST_UNIQUE=1,
    or unique=True, appends one for when a guaranteed-empty directory matters
    more than being able to find it. See TODO 209.
    """
    global _workdir
    if _workdir:
        # calling twice would nest, since the cwd has already moved into it
        return _workdir
    if not name:
        # the ctest name first: TestScanGuarantees and TestScan are both
        # scantest.py, and so are the two redispytest runs, so the script name
        # would put two tests in one directory - which is the thing this is for
        name = os.environ.get("BARCH_TEST_NAME", "")
    if not name:
        name = os.path.splitext(os.path.basename(sys.argv[0]))[0] or "test"
    if unique or os.environ.get("BARCH_TEST_UNIQUE") == "1":
        name = f"{name}-{uuid.uuid4().hex[:8]}"
    # Pin the root in the environment before moving, so a child process that
    # calls this too resolves to the same directory instead of nesting a second
    # one inside it. rangeconverttest re-executes itself for its second phase and
    # has to find what the first phase wrote.
    root = os.environ.get("BARCH_TEST_ROOT") or os.getcwd()
    os.environ["BARCH_TEST_ROOT"] = root
    d = os.path.join(root, "t", name)
    os.makedirs(d, exist_ok=True)
    os.chdir(d)
    _workdir = d
    return d


def note(test: str) -> None:
    """One line saying the run was shortened, so a fast pass is not mistaken
    for a full one when reading ctest output."""
    if short():
        print(f"{test}: BARCH_TEST_SCALE={scale()} - shortened run", flush=True)


# LD_PRELOAD is how a sanitizer runtime gets into this process, because barch is a
# module python dlopens - see ci/README.md. It is also inherited by anything the
# test spawns, and git or valkey-server are not instrumented, so they die on it
# with "cannot allocate memory in static TLS block".
#
# The runtime is already loaded here by the time this runs, and LD_PRELOAD is only
# read at exec, so dropping it now costs this process nothing and leaves children
# clean. Import scale before spawning anything. See TODO 206.
SANITIZER_PRELOAD = os.environ.get("LD_PRELOAD", "")
if SANITIZER_PRELOAD:
    os.environ.pop("LD_PRELOAD")


def barch_child_env(env=None):
    """Environment for a child that loads barch itself.

    The opposite case to the one above: a python child that imports barch needs
    the sanitizer runtime just as this process did, so it gets LD_PRELOAD put
    back. Children that are not instrumented - git, valkey-server - want the
    plain `os.environ`, which no longer has it.
    """
    e = dict(os.environ if env is None else env)
    if SANITIZER_PRELOAD:
        e["LD_PRELOAD"] = SANITIZER_PRELOAD
    return e
