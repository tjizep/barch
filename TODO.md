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

12. [Done] The asynchronous call path started a second read chain [26-07-2026] Nr 12

13. [Done] Same defect as entry 12, diagnosed there [26-07-2026] Nr 12

14. `tree_filter_key` hands back a `value_type` that points into a per thread
    `temp_key` buffer, so a filtered key is only valid until the next call on that
    thread. `shard::remove` holds one across `dependencies->search(key)`, which calls
    the filter again. It is currently harmless because the first call already appended
    the null terminator, so the second finds nothing to copy and leaves the buffer
    alone - but that is an invariant nobody states and nothing checks. Settle it by
    either documenting the invariant at `s_filter_key` and asserting it, or by giving
    the three `filter_key` callers in shard.cpp their own local buffer via the
    `s_filter_key(std::string&, value_type)` overload they already have available
    (`opt_rpc_insert` already does exactly this).

15. [Done] The hash set looked keys up through a thread_local side channel [01-08-2026] Nr 13
