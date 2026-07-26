# TODO
1. [Done] The swig_api.cpp flat view verified at every site [26-07-2026] Nr 8

2. [Done] Audit of the other commands that could open an empty array [26-07-2026] Nr 7

3. [Done] The length parameter is gone from end_array [26-07-2026] Nr 6

4. [Done] rpc_caller and vk_caller reconciled on the discarded array [26-07-2026] Nr 9

5. [Done] Out of bounds read in both glob matchers [26-07-2026] Nr 1

6. [Done] VALUES globs over values and answers with keys [26-07-2026] Nr 2

7. [Done] HELLO implemented for RESP2, protocol 3 refused with NOPROTO [26-07-2026] Nr 3

8. [Done] SCAN threading and service queueing reviewed [26-07-2026] Nr 10

9. [Done] RESP3 support, so a default configured client connects [26-07-2026] Nr 4

10. [Done] HELLO AUTH runs the real AUTH and takes its OK back [26-07-2026] Nr 5

11. [Done] ZCOUNT registered, the other three documented as deliberate [26-07-2026] Nr 11

12. **TestGlobPerformance hung for ~75s and then took the process down in CI.**
   One pipeline run failed `TestGlobPerformance` with a SEGFAULT after the client timed
   out. The reference pattern's `VALUES ... COUNT` call did not answer for about
   seventy five seconds - the whole test took 81s and everything before that command
   accounts for only a few - and the crash was reported after the python traceback, so
   the process died tearing down rather than in the command itself.
   Not reproduced. Ruled out locally: two cores via `taskset`; worker oversubscription
   at 1, 4, 16 and 64 `iteration_worker_count` on two cores, which reaches 800ms and
   not 75s; exiting while a glob is still running server side; and three consecutive
   full runs. The same command takes ~230ms locally and CI's own scan overhead line
   showed only a 4x general slowdown, so 75s is two orders of magnitude beyond slow.
   The leading guess is memory pressure on the runner. CI runs this after 34 other
   tests in the same directory, and the corpus is 110 MiB on top of whatever the arena
   already holds - the run that failed never printed a memory line because there was
   not one yet.
   The test no longer hides it: `BARCH_PERF_ENTRIES` and `BARCH_PERF_TIMEOUT` size the
   run, `used_memory`/`rss`/`peak` are printed after loading and after the baseline, and
   the body is wrapped so `barch.stop()` always runs. A repeat now fails with a
   diagnosable exit 1 and the memory numbers beside it rather than a bare SEGFAULT.
   *Settle it by:* reading the memory lines from the next failure. If they are high,
   lower `BARCH_PERF_ENTRIES` in the workflow. If they are not, the hang is real and
   wants a stack from the server while it is stuck - the timeout now leaves it alive
   long enough to attach.

