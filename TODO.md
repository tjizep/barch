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
    The option, its per key space plumbing and its reporting exist and are tested
    (DONE 30); nothing reads it yet, so a space with it set is still hash sharded.

    What it has to do: route a key to a shard by the range it falls in rather than by
    its hash, so a shard holds a contiguous span of the key order. That is what makes it
    stateful in the sense sharded_store was shaped for - a hash needs no state beyond the
    shard count, a range needs the boundaries, and those have to be held somewhere,
    consulted on every route, and kept when the space is saved and loaded.

    The layer is ready for it: sharded_store::shard_for() and shards() are virtual and
    every other operation is composed from those two, so a range routing subclass
    overrides shard_for and inherits the rest.

    The questions to settle before writing it, roughly in order:

      - where the boundaries live. A key space member is the obvious place, but they
        have to survive a restart, so they belong in whatever the space already
        persists, and they have to be readable by every thread routing a key while
        being rewritten by whatever rebalances them.
      - how they are chosen initially. An empty space has no idea what its keys look
        like. Splitting on first insert, sampling, or taking a hint from configuration
        are all defensible and they behave very differently on a cold load.
      - when and how they move. A range shard fills unevenly by nature, which is the
        cost of the ordering it buys. Splitting a shard means moving keys between
        shards, which is the first operation in barch that does that, and every reader
        has to see one side or the other and never both or neither.
      - what happens to the operations that assume any key can be on any shard.
        sharded_store::range and the striation walk in particular do far less work when
        the shards are ordered - a range can stop after the shards that overlap it -
        and that is most of the point, so it is worth designing for rather than
        retrofitting.
      - whether a space can be converted after it has keys in it, or whether the option
        is fixed when the space is created. Fixed is much simpler and probably right to
        start with; the tests already assume nothing either way.
