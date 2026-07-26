# TODO

Open questions and unverified assumptions left over from the glob / SCAN / reply
shape work. Each one records what is uncertain and what would settle it.

1. [Done] The swig_api.cpp flat view verified at every site [26-07-2026] Nr 8

2. [Done] Audit of the other commands that could open an empty array [26-07-2026] Nr 7

3. [Done] The length parameter is gone from end_array [26-07-2026] Nr 6

4. **Reconcile `rpc_caller` and `vk_caller` on the discarded array.**
   `discard_array()` genuinely rewinds for the RESP path but the base default just
   closes the array, which is all the valkey module path can do with a
   `POSTPONED_LEN` reply. So a blocking pop that registers a block contributes nothing
   on RESP and an empty array under valkey. That preserves the existing valkey
   behaviour deliberately, but the two paths now differ.
   *Settle it by:* deciding whether the valkey path should suppress the empty array
   too, which probably means not starting the array until the outcome is known.

5. [Done] Out of bounds read in both glob matchers [26-07-2026] Nr 1

6. [Done] VALUES globs over values and answers with keys [26-07-2026] Nr 2

7. [Done] HELLO implemented for RESP2, protocol 3 refused with NOPROTO [26-07-2026] Nr 3

8. **Threading and service queueing review of `SCAN`.**
   Deliberately parked. `SCAN` keeps a per-connection iteration holding shard pointers
   across calls, and `art::glob` serialises every `KEYS` behind a single
   `glob_queue` mutex while iterating pages on worker threads. The `SCAN` comment
   "we need to eventually get rid of the iteration - it will only get removed if the
   iteration completes or when the connection closes" points at a leak on abandoned
   scans. None of this has been looked at.

9. [Done] RESP3 support, so a default configured client connects [26-07-2026] Nr 4

10. [Done] HELLO AUTH runs the real AUTH and takes its OK back [26-07-2026] Nr 5

11. **Four commands are declared and implemented but not reachable over RESP.**
   `HGETEX` is commented out in `barch_apis.cpp`, `HQUERY`, `ZCOUNT` and `COMMAND`
   were never added to the map, so all four answer `unknown command` to a client.
   `HUPDATEEX` is not a command at all - it is a helper with a different signature
   that `HGETEX` calls - so it belongs in neither place.
   This is why the entry 2 audit could not reach them, and it overlaps with entry 1:
   the same commands are the ones with no binding level coverage.
   *Settle it by:* deciding for each whether it is meant to be a command. If it is,
   register it and give it a test; if it is not, say so where the declaration is, so
   the next person does not read an unregistered entry point as an oversight.
