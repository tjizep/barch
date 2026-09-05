# Running the CI build locally

`local-ci.sh` runs the GitHub workflow's build inside a container, so a
compiler this machine does not have gets a look at the tree before a push
does.

```
ci/local-ci.sh 22                 configure and build, gcc 11
ci/local-ci.sh 24                 the same on gcc 13
ci/local-ci.sh 22 --configure     stop after cmake
ci/local-ci.sh 22 --target lbarch just one target
ci/local-ci.sh 22 --tests         build, then ctest
ci/local-ci.sh 22 --shell         a prompt in the container
```

The tree is bind mounted rather than copied and the container runs as you,
so nothing lands root-owned and there is no second checkout to drift. Each
image builds into its own `ci-build-ubuntu<n>`, which git ignores.

## What it is and is not

It reproduces the **compiler**, which is where the differences that matter
have been. It does not reproduce the hosted runner: the images here install
what the build reaches for rather than the hundred-odd packages a GitHub
image ships with, and a missing dependency will look like a build failure.
It also cannot reproduce the runner's *hardware* — see below, because that
turned out to matter.

## The failure this was written for

`-march=native` on a host with AVX512-BF16 makes gcc define
`__AVX512BF16__`. NumKong reads that as "this compiler has bf16" and does
`typedef __bf16 nk_bf16_t`. gcc only grew the `__bf16` scalar type in 13,
and 11 and 12 define the macro anyway, so the typedef fails and around 600
errors follow it.

Seeing it needs gcc under 13 *and* a CPU with bf16 at the same time. A
24.04 host never shows it whatever the CPU, and a 22.04 runner on an older
CPU does not either — which is why it arrived looking like a change in our
code rather than a change of runner. `CMakeLists.txt` now defines
`NK_NATIVE_BF16=0` for gcc under 13, which falls back to their
`unsigned short`: same two bytes, same layout.

Note the second half of that: this only reproduces here because this
machine's CPU has bf16. On a host without it, `ci/local-ci.sh 22` would
have compiled quite happily.

## Sanitizers and the short test set

The short set finishes under TSan in about four and a half minutes, which is
worth running before anything that touches threading. It has found real bugs
repeatedly: DONE 186, 188, 190, 192, and the dangling endpoint reference in
DONE 212.

`.github/workflows/ubuntu24-tsan.yml` is that run as a CI job - the fourth
workflow, and the only one that builds with a sanitizer. It does the short set
and nothing else, because the full suite under TSan is far too slow for CI. To
reproduce what it does, locally:

    cmake -B build-tsan -DTEST_OD=ON -DSANITIZE=thread \
          -DCMAKE_BUILD_TYPE=RelWithDebInfo
    cmake --build build-tsan --target barch --parallel 2
    ( cd build-tsan ; BARCH_TEST_SCALE=0.05 ctest -L short --output-on-failure )

`-DSANITIZE=thread` does the rest: it adds the flags, finds the runtime, and
wires the preload and `setarch -R` that the python tests need. Both are needed
because `_barch.so` is a module python dlopens - the runtime is not in the
process when it starts so its interceptors never install, and TSan lays out
shadow memory that ASLR otherwise lands on top of, which shows up as
`FATAL: ThreadSanitizer: unexpected memory mapping` before the first test runs.

`-DSANITIZE=address` is wired the same way. It has not been run to a clean pass,
so treat it as untested rather than working.

`BARCH_TEST_SCALE` is a multiplier the tests read - see `test/scale.py`. Unset
means 1.0 and a normal run is exactly what it was. 0.05 is what the numbers
above are from. Tests that already had their own knob still honour it, so
`BARCH_PERF_ENTRIES=50000` means fifty thousand whatever the scale says.

### Running it under CPU pressure

`ci/tsan-stress.sh` runs the set on a couple of cores with busy loops pinned to
those same cores, several times over. The point is the interleavings, not the
speed: a race that needs an unlucky one turns up on a loaded CI runner and not
on a workstation with sixteen idle cores, which is how the two in DONE 215 got
in.

    ci/tsan-stress.sh                       # 3 runs of the short set on 2 cpus
    ci/tsan-stress.sh -n 10 -c 1            # 10 runs, everything on cpu 0
    ci/tsan-stress.sh -q 20%                # a cgroup quota instead of crowding
    ci/tsan-stress.sh -- -R TestFetchLuau   # anything after -- goes to ctest

Pinning on its own does almost nothing here - the tests spend their time
waiting on sockets, so squeezing them onto one core costs about 8% of the wall
clock and changes little else. The hogs are what makes the difference: they are
not nice'd, so a test thread that wakes up goes to the back of a queue. That is
the state a shared runner is in.

The quota (`-q`) throttles instead of crowding - the cgroup gets a slice of the
wall clock and is frozen for the rest of it. It needs a user systemd session,
which a GitHub runner does not have, so it is a workstation lever; `taskset` is
the portable one.

**The CI job does not run this way, on purpose.** Two stressed runs of four
tests failed twice: once on a real race (TODO 225) and once on `FUNCTION
timeout`, a deadline missed because the machine was busy. `SANITIZE_EXITCODE=66`
cannot tell those apart - both are just a failed test - so a stressed CI job
would produce failures nobody can triage without opening the log, which is how a
sanitizer job gets ignored. Stress locally, where a failure has someone looking
at it already.

Two things worth knowing:

- A TSan finding fails the test that produced it, because the short set runs
  clean now, so anything new is a regression. That was not always so - it started
  at 0, reporting without failing, while the tail of known races was still there.
  `-DSANITIZE_EXITCODE=0` puts it back to reporting only, which is what an ASan
  run still wants. Real test failures fail either way.
- `TestBarchLru` is not in the short set. Its assertions are about absolute
  memory pressure rather than key counts, so a smaller run just means eviction
  keeps up and it fails for the wrong reason.

Anything that spawns a child process - git, valkey-server - must `import scale`
before it does. `LD_PRELOAD` is inherited, and an uninstrumented binary dies on
it with "cannot allocate memory in static TLS block"; importing `scale` drops
the variable, which is safe because the runtime is already loaded by then.

`chaostest.py` and `fetchluautest.py` are the two that have earned their keep.
Use the suppressions file: without it every write unlock is reported, because
TSan does not intercept `pthread_mutex_timedlock` and barch takes its write
latch with `try_lock_for`. See `ci/tsan.supp` and DONE 189.

## Running tests in parallel

    ctest -j4                   # 152s here, against 502s serial
    ctest -j4 -L short          # 26s, the set worth running under a sanitizer

Each python test calls `scale.workdir()` and moves into `<build>/t/<TestName>`
itself, so it is isolated however it was started - through ctest or by hand.
Ports come from ctest through `BARCH_TEST_PORT`, since a test cannot pick one
without knowing about the others. `BARCH_TEST_UNIQUE=1` makes the directory
name unique per run when a guaranteed-empty one is wanted.

Two scripts are run twice by ctest under different test names and share their
directory on purpose - `largetest.py` and `repltest.py`, where the second run
reads what the first saved.

`-j4` rather than more: `-j8` is no faster here - 147s against 152s - and fails
one test per run, a different one each time, both passing alone. The tests
start servers, so past a few jobs they are competing with themselves for cores.
Pick a number well under the core count.

What still runs one at a time is in `_barch_serial_tests` in `CMakeLists.txt`:
the lua and C++ tests under TestStarter, which bind their ports from lua and
cannot read the environment, and `TestBarchList` and
`TestBarchSimpleClusterRPC`, which share `test/build` where their valkey lives.

Two tests are not isolated on purpose: `TestBarchList` and
`TestBarchSimpleClusterRPC` run in `test/build` because the valkey they start
lives there, so they share a lock rather than a directory.

If a parallel run seems to hang rather than fail, that is what a port collision
looks like - the client connects to another test's server and blocks on a read.
Tests carry a 600s `TIMEOUT` so it ends rather than sitting there.
