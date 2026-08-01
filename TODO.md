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

14. [Done] The filtered key borrowed a shared per thread buffer [01-08-2026] Nr 14

15. [Done] The hash set looked keys up through a thread_local side channel [01-08-2026] Nr 13

16. [Done] The lower bound trace was read back out of a thread_local [01-08-2026] Nr 15

17. The sharding layer covers keys_api.cpp only. hash_api, list_api, ordered_api,
    barch.cpp and configuration.cpp still route and lock at the call site, so two
    idioms are live at once. Convert them onto barch::sharded_store, adding whatever
    operations they need that keys did not (the ordered set and hash commands work on
    a container key rather than a bare key, so they may want a container flavoured
    accessor rather than the bare with_key_write escape hatch).

18. SCAN is the one keys command still holding shards directly, because its cursor
    lives in caller::iteration_ptr, which belongs to the connection rather than to the
    store. Moving it is the first real test of the stateful design: sharded_store would
    have to own the cursor, which is what the class was shaped for. Settle whether the
    cursor belongs to the store, to the caller, or to something new that owns both.

19. [Done] Sharding layer defined and keys_api converted onto it [01-08-2026] Nr 16
