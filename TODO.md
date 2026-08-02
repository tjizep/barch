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

17. [Done] Remaining API files converted onto the sharding layer [01-08-2026] Nr 17

18. [Done] SCAN cursor split between the connection and the store [01-08-2026] Nr 18

19. [Done] Sharding layer defined and keys_api converted onto it [01-08-2026] Nr 16

20. Key space administration - KSPACE DEPENDS / MERGE / RELEASE - locks two key spaces
    at once, in a hand chosen order, and sharded_store models a single space. Decide
    whether a cross space lock ordering belongs in the layer (a free function taking
    two stores, say) or stays where it is. Nothing is known to be wrong today; the
    concern is that the ordering rule that avoids deadlock is written out at each site
    rather than in one place. `barch::all_shards`, which walks every space, is the
    same question.

21. [Done] SIZE and HEAPBYTES relaxed to read locks [01-08-2026] Nr 19

22. [Done] Every command moved into a {category}_api file [01-08-2026] Nr 25

23. [Done] CLIENT LIST implemented [01-08-2026] Nr 22

24. [Done] Redis configuration names, and the other CONFIG subcommands [01-08-2026] Nr 23

25. [Done] PING renamed to RPING and redis's PING added [01-08-2026] Nr 24

26. INFO with no arguments answers "not implemented". INFO <section> works - memory,
    commandstats and the rest - but the bare form every client sends on connect, and
    that redis-cli's own INFO uses, falls through to the error at the end of
    barch::INFO. Found while testing CONFIG RESETSTAT. What is uncertain is only which
    sections the default should contain: redis returns a documented default set rather
    than everything, so settle by checking what redis-py's INFO parser and redis-cli
    expect to find, then join those sections.

27. [Done] All logging converted to lzr_log [02-08-2026] Nr 28

28. [Done] Variable would not take an unsigned type narrower than 64 bits [02-08-2026] Nr 29

29. General compilation speedup. Converting the logger took 0.65s of includes out of
    every translation unit and moved a real file only 2 to 6% (DONE 28), which says the
    time is spread across the big headers rather than concentrated anywhere obvious. A
    full build is the thing to measure against, not a single file.

    Known starting points, largest first:

      - variable.h is about 1.0s on its own, more than anything else measured. It pulls
        in fast_float, fmt/format.h and a std::variant over eleven alternatives. Two
        specific questions: whether fast_float is needed in the header at all, since the
        conversion helpers it serves are declared rather than defined there, and whether
        the variant has to be visible to every consumer or could sit behind a pointer
        for the files that only pass Variables through.
      - the art headers and asio, which dominate the files that were only 2% faster.
        rpc/server.cpp is the worst in the project at about 4.0s.
      - fmt is now confined to lzr_log.cpp for logging, but variable.h still includes
        fmt/format.h for one call in Variable::to_string.

    Worth doing first, because it turns guessing into measuring: build with
    -ftime-report or -ftime-trace and total the cost per header across the project,
    rather than timing files one at a time as was done for the logger. That will also
    say whether precompiled headers or explicit instantiation would pay, which is a
    different answer from trimming includes.

30. Implement the stateful ordered range sharding the opt_range_sharded option selects.
    The option and its plumbing exist and are tested (DONE 30); nothing routes by it.

    A prototype settled the algorithm - test/rangeshard_prototype.cpp, on std::map, not
    wired into the build. Run it before changing anything here. What it established:

      - **shed in both directions, not just upwards.** With upward only the last shard
        is a sink: ascending inserts all route to it and it has nowhere to push, giving
        16.00x imbalance on 16 shards - every key in one shard. Descending was fine,
        which is what makes this easy to miss.
      - **cascade with a budget per level, not one budget for the walk.** One shared
        budget is spent entirely on the first hop, so the shard that just received those
        keys is over and nothing relieves it until an insert lands there - which for
        ascending never happens. 13.99x.
      - **shed to meet the neighbour half way, not down to the threshold.** Shedding
        everything above the threshold dumps a block into the neighbour and pushes the
        whole cascade over at once. Cost was quadratic in shard count - 5.6, 24, 100,
        502, 1007 moves per insert at 4, 8, 16, 32, 64 shards - and past 32 shards the
        budget could not keep up and balance was lost (8.9x, then 21.4x). Moving
        min(budget, (size - neighbour)/2) makes it linear: 1.1, 5.9, 12.3, 25.1, 50.5
        at 4, 16, 32, 64, 128 shards, balance held at 1.25x throughout.
      - **the threshold needs slack.** At exactly total/shard_count a shard is over the
        moment it is one key above average, so it thrashes: 32 moves per insert on
        random keys. At 1.25x it is 0.00.

    With all four: random 0.08 moves per insert, clustered 0.52, ascending and
    descending about N/2.5, worst single insert bounded by roughly the shard count,
    imbalance 1.25 to 1.31x, and the partition and index invariants hold throughout.

    The one thing to know before choosing this over hash sharding: an append only
    workload costs O(shard_count) moves per insert and there is no way around it. Every
    new key lands at the top, so one key has to cross every boundary to keep the
    partition balanced. That is the intrinsic price of an ordered partition with a fixed
    shard count, and it is exactly the workload where hash sharding costs nothing. Range
    sharding being opt in per key space is therefore right, and off by default is right.

    Two things are settled that were open:

      - **the index never has to be persisted.** It is nothing but the minimum key of
        each shard above 0, so a load rebuilds it by asking each shard for its first
        key, which an art finds walking down the left spine. The prototype rebuilds it
        after every run and asserts it matches the one maintained incrementally, on all
        four workloads. That removes a whole class of problem: no index file, nothing to
        get out of step with the shards, and nothing to version.
      - **the index is a sorted flat vector, not a map.** At most shard_count entries,
        read on every route and written only by a rebalance, so the memmove on insert is
        paid rarely and the binary search hits a couple of cache lines. 18% faster over
        the whole prototype run - 13.5s to 11.1s at 64 shards and 200k keys - with
        identical results.

    Still to settle, and not answered by the prototype:

      - how the index is read by every routing thread while a rebalance rewrites it. The
        prototype is single threaded. A flat vector helps here too, since a whole new
        one can be built and swapped in behind a pointer rather than mutated in place.
      - what a move looks like against real shards. The prototype erases from one
        std::map and inserts into another; barch has to do that across two locked
        shards, with readers seeing one side or the other and never both or neither.
      - whether rebalancing belongs on the insert path at all or on the maintenance
        thread. The prototype does it inline, which is what bounds it, but 25 moves on
        an insert is a latency spike a background sweep would not have.
      - 12 of 64 shards were empty on random keys while still measuring balanced.
        Harmless, but it says the boundaries settle in a way worth a look.
      - sharded_store::range and the striation walk, which can stop after the shards
        that overlap the range once shards are ordered. That is most of the point of
        doing this and should be designed in rather than retrofitted.
