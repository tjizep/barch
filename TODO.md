# TODO

Open questions and unverified assumptions left over from the glob / SCAN / reply
shape work. Each one records what is uncertain and what would settle it.

1. **Verify the flat-view substitution across every `swig_api.cpp` call site.**
   `end_array` no longer splices an array reply into `results`, so all 49 sites in
   `swig_api.cpp` were moved onto `rpc_caller::flat_size/flat_empty/flat_at/append_flat`
   by mechanical substitution rather than by reading each one. The accessors are meant
   to reproduce the previous flat list exactly, and the binding tests (`testbarch.py`,
   `listtest.py`) pass, but they do not reach every site. `HGETEX`, `HQUERY`,
   `HUPDATEEX` and the `Z*` commands have no binding-level coverage at all.
   *Settle it by:* adding binding tests that call those commands through the Python
   module and check the returned value counts, rather than inferring from the ones
   that are covered.

2. **Audit for other commands that depended on the old splat.**
   `bpop` opened an array before knowing whether it could pop anything and relied on
   the top level splat to discard it when empty; it now calls `discard_array()`. That
   was found by `bstartest.py` failing, not by inspection, and nothing in the compiler
   or the type system would have caught it. Any other command that calls
   `start_array()` and then conditionally pushes nothing has the same latent problem.
   *Settle it by:* walking the `start_array` callers (`barch.cpp`, `hash_api.cpp`,
   `list_api.cpp`, `ordered_api.cpp`, `auth_api.cpp`) and checking each one that can
   finish with an empty array, ideally with a test per command at zero results.

3. **Decide what `end_array(size_t length)` is for.**
   Both implementations ignore the argument, yet callers pass meaningful and
   inconsistent values - `KEYS` passes the reply count, `SCAN` passes 1, `STATS`
   passes 0. Either the parameter should be wired up or it should be removed, because
   as it stands it reads as though it does something.

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

9. **Real RESP3 support, so a default configured client can connect.**
   `HELLO` now negotiates properly (entry 7, `DONE.md` Nr 3) but only ever agrees to
   protocol 2. A client left on its own defaults asks for 3, gets `NOPROTO` and still
   fails to connect - correctly and diagnostically now, rather than with
   `unknown command`, but it fails. Every test passes `protocol=2` to work around it.
   `redis_parser.h` emits only the RESP2 types: `+`, `-`, `:`, `$` and `*`. RESP3 adds
   map `%`, set `~`, double `,`, boolean `#`, big number `(`, verbatim `=`, null `_`
   and push `>`. The writer would need those, `Variable` would need to carry the
   distinction between a map and a flat array so replies like `HELLO`, `CONFIG GET`
   and `XPENDING` can be shaped per protocol, and the negotiated version would have to
   be reachable from the reply path.
   *Settle it by:* deciding whether barch wants RESP3 at all. Staying RESP2 only is a
   defensible answer as long as it is documented, since the client can be configured
   for it - but the default experience is a failed connection, which is a poor first
   impression.

10. **The `HELLO AUTH` form is refused rather than supported.**
   `HELLO 2 AUTH user pass` answers with an error telling the client to send `AUTH`
   separately. barch's `AUTH` replies `OK` on success, and there is no way to run it
   from inside `HELLO` without that `OK` landing in front of the handshake and
   corrupting the reply. Low priority: the form is only reachable for a RESP2 client
   that also has credentials, and such a client can send `AUTH` on its own.
   *Settle it by:* splitting `auth_api.cpp`'s `AUTH` into a function that authenticates
   and sets the ACL without replying, plus a thin command that adds the `OK`, then
   calling the former from `HELLO`.

