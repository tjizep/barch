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

20. [Done] One lock order, written down once [09-08-2026] Nr 42
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

30. [Done] Ordered range sharding implemented [02-08-2026] Nr 31

31. art::iterator's one argument form - the one that is meant to start at the first key
    in a shard - walks nothing. It finds the tree minimum but never fills the trace list,
    then reads last_node off the empty one. Nothing had noticed because every existing
    caller uses the two argument form; found writing the range repartitioner, which now
    seeds from the shard's own minimum instead (DONE 31). What is uncertain is only
    whether to fix it or delete it: it has no callers, and a walk from the first key is
    already expressible. Settle by checking whether the trace can be filled as cheaply as
    lower_bound fills it, since if it cannot the two argument form is the honest one.

32. Range sharding and replication have not been put in the same room. A rebalance moves
    a key between shards with tree_insert and tree_remove, which do not replicate, on the
    reasoning that where a key physically lives is a local decision and a replica routes
    by its own index built from its own shards. That is self consistent but untested, and
    the question is whether a replica of a range sharded space converges on the same
    partition or merely on the same contents - the second is fine, the first is not
    required, and nothing currently checks which one happens. Settle with a repltest
    variant over a range sharded space.

33. A range sharded space with pull sources is not rebalanced at all - see DONE 31 for
    why the sizes and minimums do not mean what the algorithm needs. It still routes,
    so it works, but it never balances, and a space that silently does not do the thing
    its option asks for is the situation the option was built to avoid. Either the
    algorithm learns about tombstones and upstream keys, or the combination is refused
    the way range sharding without ordered keys already is.

34. The rebalancer's budget and tolerance are constants in range_index.cpp - 64 keys per
    lock pair and 1.25x. Both were chosen in the prototype and neither has been tuned
    against a real workload. The budget in particular is the one that decides how long
    two shards are held still at a time, so it trades rebalancing throughput against
    tail latency, and 64 is a guess that happens to work.

35. docs/DOCUMENTATION-STANDARD.md is written, but nothing conforms to it yet. Most of it
    was extracted rather than invented - barch-docs.html already followed the three block
    spine across all thirteen reference articles without a single exception, and SET.md
    already had most of the command reference shape - so the descriptive half needs no
    work at all. What has work behind it is the handful of rules that were added because
    an audit found defects the existing habits would not have caught.

    Roughly in the order that finds the most defects per hour:

      - Retrofit `<!-- src: -->` citations, article by article. This is less clerical than
        it sounds. An audit of the range sharding article found three defects and all
        three were in exactly the material the citation rule covers, namely a reference
        table row, an error tree entry, and an example nobody had ever run. The other
        twelve articles have not been audited at all, so this is where the unknown
        defects are.
      - Error trees for ref-hash, ref-ordered, ref-list, ref-acl, ref-compression,
        ref-memory, ref-stats, swig-python and swig-lua. Each one needs its failure modes
        read out of the source first, since the section is missing rather than empty.
      - Run every Code matrix pane against a live server. One was already wrong - an
        `INFO SHARD` call sent as a single token, which the handler does not match - and
        it survived because reading is the only thing that has ever checked them. What is
        uncertain is whether this can become a gate in ci/ or has to stay manual, because
        the panes need a server, several of them need two, and some mutate state.
      - Bring the markdown command references up to the eight part shape in section 4. None
        of them carry it in full today. SET.md is closest, which is why it was used as the
        template, but even it lacks the SWIG synonyms section and does not say what
        happens in the cases where the syntax is fine but the request cannot be fulfilled.
      - swig-lua's Endpoint reference block is prose where every other one is a table.
      - Normalise the tab labels to the controlled vocabulary.
      - The docs/*.md migration in section 11. The dispositions are decided per file, and
        the only real care needed is that the five in-scope files README links to - APIS,
        COMPRESSION, PYTHONSERVEREXAMPLE, USECASE and ZFASTRANK - keep working URLs.

    The question section 11 left open - whether command references stay as markdown or
    move into the site - has since been answered in favour of the site. #ref-commands is
    the draft index, generated from the registration tables, and section 4 now describes
    the shape of the per-command entries that go under it. Section 11's migration table
    still stands, but it should be read with that decision already made.

    The index has a detail pane: clicking a command shows what is registered for it and
    its section 4 reference. All hundred and ten now carry that reference - see entry 37
    for what writing them turned up. Reply shapes were taken off a running server wherever
    the command could safely be run and from the handler otherwise. The detail data is a
    JSON blob in the page's script; it was generated, so if the registrations change it
    should be regenerated rather than hand patched.

    What is left on the index is a second pass rather than a first: the eight commands
    that could not be exercised against a live server without disrupting it - the four
    dangerous ones, START, STOP, PULL and RETRIEVE - have their replies from the source
    only, and MULTI, EXEC and the CLIENT subcommands were not exercised at all.

36. [Done] Every Z* command declared an ACL category that does not exist [09-08-2026] Nr 32

37. [Done] The RESP commands that answered with the wrong thing [09-08-2026] Nr 33
38. [Done] Redis compatibility, the behaviour that differed under a shared name [09-08-2026] Nr 37

39. The tests that spawn a valkey-server leak it when they fail. routetest.py, pulltest.py
    and pulldebug.py each start `valkey-server --port 7777 --loadmodule _barch.so` with
    subprocess.Popen and kill it on their last line, with nothing in between to catch an
    exception. Any assertion that fails part way through - which is the normal case while
    something is being worked on - leaves the server running and holding port 7777, and
    the next run of any of the three then cannot bind and waits instead of failing. That
    is what a whole ctest run hanging until its timeout looks like from the outside, and
    the leaked servers sit at zero cpu, so nothing draws attention to them.

    Guarded for now by registering an atexit handler next to each Popen, which fires on a
    failed assertion as well as on a clean exit. That is the small version of the fix and
    it is enough to stop the leak.

    The other half of this turned out to be in test_starter.cpp, which was supposed to
    clear a stale server before each lua test and never did - see DONE 36. So leaked
    servers accumulated and nothing collected them, which is what made the leak matter.

    What is still open is the shape of it: these three tests are the only ones that manage
    an external process, they do it three different ways, and none of them checks that the
    port was actually free before starting. A context manager that owns the process and
    refuses to start when 7777 is already held would remove both the leak and the confusing
    hang that follows it. Worth doing when one of the three is next touched rather than as
    a job of its own.

    Also worth a look while in there: a full ctest run takes longer than fifteen minutes
    on this machine, which is long enough that the per test timeouts are doing nothing and
    a hang is only visible as the whole run being killed. Per test timeouts would attribute
    it.

    And configuring the main project destroys the lua test harness and rebuilds valkey.
    CMakeLists.txt around line 241 does this at *configure* time, not build time:

        execute_process(COMMAND rm -rf ${TEST_BUILD_DIR} ...)
        execute_process(COMMAND mkdir ${TEST_BUILD_DIR} ...)
        execute_process(COMMAND cmake .. ...)
        execute_process(COMMAND make ...)

    so every `cmake .` deletes test/<CMAKE_BUILD_TYPE>/ - TestStarter and the whole fetched
    valkey tree with it - then re-fetches valkey from github and rebuilds all of it. That
    takes minutes, execute_process prints nothing, and the four `RESULT_VARIABLE result`
    values are never looked at, so a failure is silent as well as slow.

    What it looks like from the outside is a suite that intermittently fails for no reason.
    A run that starts inside that window reports sixteen lua tests as "Not Run" and
    TestBarchSimpleClusterRPC as a missing valkey-server, none of which mentions the
    rebuild. It cost real time twice in one session, both times immediately after adding a
    test, which is exactly when someone reconfigures.

    Worth fixing properly rather than working around: the side project only needs
    configuring when it does not exist or its CMakeLists has changed, and the results of
    those four execute_process calls should be checked. A missing TestStarter should also
    be one clear message rather than sixteen unexplained ones.

    Two more things the same interrupted run showed up:

      - killing a run part way leaves the shard files in the build directory truncated,
        and every python test then aborts on startup with "use after free - no data
        allocated" while loading them. It is a confusing report for what is really a short
        file, and it takes the whole suite down until the .dat files are cleared. Loading
        should recognise a truncated shard and say so.
      - the venv install is itself a test, TestBarchInstallPy, so running one python test
        with ctest -R uses whatever module was last installed rather than what was last
        built. A test can pass or fail against stale code with nothing saying so. Making
        the python tests depend on the install, with set_tests_properties DEPENDS, would
        stop that.

40. [Done] The translation harness, and what it took to make it faithful [10-08-2026] Nr 52

41. [Done] A lock nothing used, compiled out behind _EXPERIMENTAL_ [10-08-2026] Nr 47

42. [Done] A barch abort inside valkey hung the process instead of ending it [09-08-2026] Nr 35

43. Why a shard address read out of a file is invalid in the first place. DONE 35 stopped
    this taking the process down - a shard that will not parse now logs `could not load`
    and is skipped - but something is still producing files the loader cannot read, and a
    skipped shard is silently missing data, which is only tolerable because it happens in
    a build directory rather than to anyone's data.

    What is known: it appears on full ctest runs and not on single tests, and every test
    shares one working directory, CMAKE_BINARY_DIR. 2805 .dat files were sitting there,
    written by several different tests, some of which configure different shard counts
    for the same space name. Two candidates, and they are not exclusive:

      - a loader reading a file another test is still writing. Nothing coordinates access
        to that directory between tests, and save is not atomic as far as the reader is
        concerned - there is no temp-file-and-rename.
      - a space written with one shard count and reopened with another. The file name
        carries the shard number, so a stale nodes_x_5.dat from a run that used more
        shards is still there when a later run uses fewer, and nothing checks that the
        file it opened belongs to the layout it expects.

    Both would be settled by giving each test its own working directory, which is worth
    doing anyway, and the second additionally by writing the shard count and a format
    version into the file header and refusing a mismatch by name rather than by failing
    to parse the contents.

    The other half of DONE 35 that was left alone: several loader threads can still reach
    the failure at once. Throwing makes that harmless where load catches it, but anywhere
    an abort is genuinely right it should be funnelled so the first caller wins and the
    rest are held, rather than four threads racing into a signal handler that cannot take
    them.

    Caught in the act. A valkey-server had been sitting at zero cpu for six minutes with
    all seven threads in futex_do_wait, and the backtraces say exactly what happened:

      - several `barch::shard::load` threads called art::resolve_read_node, went down
        through logical_allocator::basic_resolve into arena::base_hash_arena::get_page_data
        and hit `abort_with("invalid page address")` at sastam.cpp:160. So more than one
        thread called abort() at the same moment.
      - abort raises SIGABRT, and valkey installs sigsegvHandler for it. The handler takes
        a global `signal_handler_lock` at debug.c:2123. Three threads are parked there.
      - the thread that won the lock went on into printCrashReport, doFastMemoryTest,
        killThreads and killMainThread, which calls pthread_join on the main thread -
        debug.c:2023. The main thread is one of the threads blocked acquiring
        signal_handler_lock.

    So the crash handler joins a thread that is waiting for the crash handler's own lock.
    Nothing can make progress, nothing is using cpu, and the process never exits. That is
    the stall, and it is a deadlock between barch's abort and valkey's crash reporter
    rather than anything in barch's own locking.

    Two separate things to fix, and the first is the one that matters:

      - a shard file that does not parse is bad input, not a broken invariant, and calling
        abort() on it is the wrong response. It is especially wrong inside the valkey
        module, because the host's crash handler is not reentrant across threads and turns
        a clean exit into a hang. shard::load should fail, say which file and why, and let
        the caller decide - which is also what would have made the truncated files in the
        build directory report themselves instead of taking the suite down twice.
      - the same abort path is reachable from several loader threads at once, so even
        where aborting is right it should be funnelled through something that lets the
        first caller win and holds the rest, rather than having four threads race into a
        signal handler that cannot take it.

    Worth knowing for next time: this is only visible from inside the process. From the
    outside it looks like a server that will not stop, which is what it was reported as,
    and killing it discards the evidence. If it happens again, trace before killing.

    Still open underneath all this: what made the page address invalid. The files were
    written by the run that was going on at the time, in the build directory that every
    test shares as its working directory, so a loader reading a file another test is still
    writing is the obvious suspect and would explain why this only shows up on full runs.
    Not proven. Giving each test its own directory would settle it and is probably worth
    doing regardless.

44. [Done] Array replies come back wrong through a remote binding [09-08-2026] Nr 40

45. [Done] Translate valkey's tests: string.tcl and keyspace.tcl [09-08-2026] Nr 41

46. [Done] hash.tcl and expire.tcl, and the crash they found [10-08-2026] Nr 53

47. [Done] zset.tcl and list.tcl, and a blocking pop that took the store with it [10-08-2026] Nr 56

48. [Done] INCRBYFLOAT implemented [09-08-2026] Nr 44
49. [Done] WRONGTYPE, as far as it goes without a stored type tag [09-08-2026] Nr 45
50. [Done] Error codes, and a wrong argument count reported for a wrong argument [09-08-2026] Nr 46
51. A compress-all command, to reclaim what the partial writes leave behind.

    APPEND, PREPEND and SETRANGE decompress a value, write into it and store it back
    uncompressed. That is deliberate - compressing on every partial write puts the whole
    value through the dictionary each time, and a value built by repeated SETRANGE would
    pay it on every call, so the latency belongs at the end rather than in the middle. The
    consequence is that a space which sees a lot of partial writes drifts towards
    uncompressed, and nothing ever brings it back.

    So there wants to be something that walks a key space and compresses what is not
    compressed. Points to settle when it is written:

      - explicit command, maintenance thread, or both. The maintenance thread already
        exists for the range rebalancer and runs off the insert path, which is the right
        place for it; an explicit command is easier to reason about and to test. Doing the
        command first and calling it from maintenance later costs nothing.
      - it has to be interruptible and budgeted the way the rebalancer is (DONE 31), or a
        space large enough to be worth compressing is a space large enough for this to
        hold a lock too long. The rebalancer's budget-per-lock-pair shape applies directly.
      - whether it is worth compressing a value at all is a decision the dictionary can
        already make - `dictionary::compress` answers empty when it did not help - so the
        walk keeps whatever it is given when compression does not pay.
      - a counter, so it can be seen doing something: values examined, values compressed,
        bytes saved. statistics::value_bytes_compressed exists and is the obvious place.

    Worth having a number before building it: how much does a realistic workload actually
    leave uncompressed? If partial writes are rare in practice this is not urgent, and
    `INFO` reporting compressed against uncompressed value bytes per space would say so
    and is much cheaper than the walk.

52. [Done] The commands string.tcl and keyspace.tcl expected [09-08-2026] Nr 43

53. [Done] A lead byte per container kind, and the name claimed at creation [10-08-2026] Nr 48

54. [Done] as_composite rewrote the key it was given [10-08-2026] Nr 50

55. [Done] A logical export, so a version bump has somewhere for the data to go [10-08-2026] Nr 61

56. `used_memory_startup` is reported through a clamp, so it moves when it should not.

    `redisinfotest.py` asserts the startup baseline stays put across a write, and it does
    not always. Measured on 09-08-2026, on a clean data directory:

        before: startup=613293744 used=614922302
        after : startup=613293744 used=705851798

    which passes - but only by 1.6 MB out of 613, a third of a percent. The reported value
    is `std::min(get_startup_memory(), used)` at info_api.cpp:158. The accumulated total
    and the current total are close enough that a transient allocation freed between the
    load and the first INFO puts `used` under the accumulated figure, the first read is
    clamped to `used` and the second is not, and the baseline appears to move.

    Two things are worth separating here. The clamp is there because the same test also
    asserts `startup <= used`, so it cannot simply be removed - it would report a baseline
    larger than the memory in use, which is worse than the flake. The real question is why
    the accumulated figure is that close to the total at all.

    `add_startup_memory` (key_space.cpp:210) sums `memory_after - memory_before` for each
    key space as it loads, and the comment there already notices the difficulty: other
    threads allocate at the same time, so a growth measured around one space includes work
    done by another. Summing those deltas double counts, which is exactly how the sum ends
    up level with the total rather than comfortably below it.

    So the fix is probably not at the reporting end. Take one reading of total memory once
    every space has finished loading and record that as the baseline, rather than adding up
    per space differences taken while the loads overlap. That is a single number, it cannot
    exceed the total it was read from, and the clamp then never fires.

    Seen as a suite failure under `ctest -j4`; it does not reproduce standalone, which fits
    a timing sensitive margin rather than a wrong constant.

57. Two file scope statics that outlive the threads which touch them.

    Carried out of entry 41, which disposed of a third one by compiling it out. These two
    are live code, so they need reading rather than removing:

      - `art/art.cpp` has a file scope `static std::mutex glob_queue{}`. Destroying a
        locked mutex is undefined, and the glob commands run on their own threads, so the
        question is whether any of them can still be holding it when static destruction
        runs.
      - `repl_api.cpp` has a file scope `static restarter restart;`. If its destructor
        stops or joins anything then it runs during static destruction with the same
        exposure, and a destructor that joins a thread which is itself blocked is a stall
        wearing a different symptom.

    Entry 41 is the caution to read these with. The same reasoning applied to
    shared_mutex.cpp produced a hypothesis that fitted the observed stalls exactly and was
    still wrong, because nothing ever called that code. So establish first whether either
    of these is reached on a shutdown path at all, and only then decide whether the
    lifetime needs changing. The canonical fix, if one is needed, is the never destroyed
    form: reach the object through a function and let it outlive the threads.

58. [Done] A range over a shard holding one key answered nothing [10-08-2026] Nr 49

59. [Done] What a keyspace command sees when the name holds a collection [10-08-2026] Nr 51

60. [Done] The commands hash.tcl and expire.tcl expect, implemented [10-08-2026] Nr 54

61. [Done] Expiry measured against unix time rather than machine uptime [10-08-2026] Nr 55

62. [Done] HSCAN, and a cursor scoped to a prefix [10-08-2026] Nr 57

63. [Done] The list commands zset.tcl and list.tcl expect, and the two key moves [12-08-2026] Nr 65

64. [Done] Ordered set validation, and two cases that were not what they looked like [10-08-2026] Nr 59

65. [Done] An exclusive lex bound, and the two ends of the range [10-08-2026] Nr 60

66. [Done] The member index had no empty component, so it collided with a real name [10-08-2026] Nr 62

67. [Done] Auto-flush of the current RESP array level [14-08-2026] Nr 75 a98494b

68. [Done] A failed EXPORT leaves an empty file where the old one was [12-08-2026] Nr 64

69. [Done] Per shard statistics, clear and load no longer rewrite the globals [12-08-2026] Nr 63

70. [Done] bloom_t is heap::vector<bool>; the substitution does not break tests [14-08-2026] Nr 73

71. [Done] The command index in docs/index.html does not know the commands from DONE 65 [13-08-2026] Nr 66

72. [Done] Claude-isms in docs/index.html [14-08-2026] Nr 67

73. [Done] User and developer sections in docs/index.html [14-08-2026] Nr 68

74. [Done] art_* tree functions moved into namespace art [14-08-2026] Nr 69

75. [Done] SAVE and RELOAD raced the range rebalancer [14-08-2026] Nr 70

76. [Done] LOAD and SAVEALL raced the range rebalancer [14-08-2026] Nr 71

77. [Done] Stateful sharding is a key space check, not a range-sharding special case [14-08-2026] Nr 72

78. [Done] Git hash on done lines, and a TODO for every code-changing instruction [14-08-2026] Nr 74 a98494b

79. [Done] KEYS writes each key to the socket; auto-flush rolled back [15-08-2026] Nr 76 a98494b

80. [Done] KEYS second walk loads only pages that hit [15-08-2026] Nr 77 29f9160

81. `glob_page_list` is `vector<size_t>` of the page ids that hit. A
    later `vector<bool>` only wins if it can be indexed by page id.
    Those ids are arena keys, not `0..occupied`. They run up to
    `max_allocated_page` and have holes, so a bit per id also counts
    free and never-used pages. Dense shards shrink; sparse ones can
    grow past the `size_t` list. A compressed bitmap is the thing that
    stays small in both cases. Put it behind the typedef when the
    `size_t` list is actually the cost. Do not change the walk.
    Settled when `test/keysstreamtest.py` still answers the same keys
    for an empty match, a selective pattern, and `KEYS *`.

82. [Done] Chaos test for KEYS under restart and memory pressure [15-08-2026] Nr 78 caf3daf

83. [Done] N-gram text index is composite keys, documented [15-08-2026] Nr 79 75e8474

84. [Done] H3 geospatial index is composite keys, documented [15-08-2026] Nr 80 75e8474

85. [Done] Chaos test covers a larger RESP subset, including n-grams [15-08-2026] Nr 81 56cd394

86. [Done] SET at the memory ceiling raises not enough memory [15-08-2026] Nr 82 aaa5449

87. [Done] Luau instruction budget is a slice, not a kill [17-08-2026] Nr 83 78a270e

88. [Done] CI MULTI, DROP deadlock, and foreign write-back [18-08-2026] Nr 84 561b393

89. [Done] Incoming keys can split on a per-space regex [18-08-2026] Nr 85 e668501

90. [Done] key_split feeds $n [18-08-2026] Nr 86 e668501

91. [Done] TestForeign no longer aborts on the write lock [18-08-2026] Nr 87 371d7a9

92. [Done] N-gram frames split on | so the gram keeps its spaces [18-08-2026] Nr 88 4a4b73f

93. [Done] FOREIGN waiter uses a millisecond clock [18-08-2026] Nr 89 10233c2

94. [Done] Idle MySQL and Postgres pool connections have a maximum age [19-08-2026] Nr 90 662969a

95. [Done] cmake --build . failed on barchlua's Lua headers [19-08-2026] Nr 91 662969a

96. [Done] Idle SQL pool drop moved to the key space maintenance thread [19-08-2026] Nr 92 662969a

100. [Done] latch_t is now debuggable_server_lock [20-08-2026] Nr 93 7fa7f38

101. [Done] Deadlock dumps name the latch, holders, and held list [20-08-2026] Nr 94 7fa7f38

102. [Done] CI locktest missing and SpaceThread lock livelock [20-08-2026] Nr 95 8ec4a95

103. [Done] Nested shared self-deadlock under write_intent [20-08-2026] Nr 96 8ec4a95

104. [Done] CI fails compiling Luau on unused parameters [20-08-2026] Nr 97 20ee740

105. [Done] Latch dumps and writer backtraces behind BARCH_LOCK_DEBUG [20-08-2026] Nr 98 bf19ae0

106. [Done] Shared-to-unique upgrade for compress-under-read [20-08-2026] Nr 99 9b33fcb

107. [Done] Coverage CI killed mid-compile (exit 143) [20-08-2026] Nr 100 2612f00

108. [Done] locktest four-reader throughput fails on 2-core CI [21-08-2026] Nr 101 cf5ae9a

109. [Done] GET one lookup and vector_stream memcpy [21-08-2026] Nr 102 b67c1ea

110. [Done] Parser views, shared-lock fast path, GET bulk write [21-08-2026] Nr 103 47d2f19

111. [Done] Command cache, short headers, bulk header, skip empty repl [21-08-2026] Nr 104 47d2f19

112. [Done] Empty bulk RESP parse timed out zadd empty score [21-08-2026] Nr 105 3ca7c3c

113. [Done] More valkey cases, and the zset/expire bugs they found [21-08-2026] Nr 106 cb02f90

114. [Done] ZREVRANK, ZREMRANGEBYRANK, and ZRANGESTORE [22-08-2026] Nr 107 5f7ec85

115. [Done] Translator expansions for remaining zset stubs [22-08-2026] Nr 108 5f7ec85

116. [Done] TestKeys and TestComposites asserted pre-compat answers [22-08-2026] Nr 109 5f7ec85

117. [Done] OrderedSet.revrange sent ZREVRANGE BYSCORE [22-08-2026] Nr 110 5f7ec85

118. [Done] exists_many probed every key twice [22-08-2026] Nr 111 5f7ec85

119. [Done] TTL truncated the seconds, and EXPIRETIME too [22-08-2026] Nr 113 367fe0a

120. [Done] ZUNIONSTORE and ZINTERSTORE stored NaN [22-08-2026] Nr 114 367fe0a

121. [Done] Differential ran all files into one server, and lost a case to its keying [22-08-2026] Nr 116 367fe0a

122. [Done] Put the Z* compatibility plan in Z-COMPAT-PLAN.md. [22-08-2026] Nr 112 367fe0a

123. [Done] WEIGHTS only parsed integers [22-08-2026] Nr 115 367fe0a

124. [Done] WEIGHTS took any number of weights, and named a bad one badly [22-08-2026] Nr 117 367fe0a

125. [Done] Phase 3: BZPOPMIN/BZPOPMAX, and ZPOP's leftover index [22-08-2026] Nr 118 1c17a23

126. [Done] Phase 4 remrange helpers, and an exclusive-bound bug [22-08-2026] Nr 119 1c17a23

127. [Done] blocked_clients, the pop-name foreach, and nested list rendering [22-08-2026] Nr 120 1c17a23


128. [Done] The deferring client, and the two bugs it found [22-08-2026] Nr 122 1c17a23

129. [Done] The string writers and a name that holds a collection [22-08-2026] Nr 126 1c17a23

130. [Done] ACCEPTED reasons pointed at closed TODOs, two were wrong [22-08-2026] Nr 121 1c17a23

131. [Done] MULTI's reply protocol [22-08-2026] Nr 123 1c17a23

132. [Done] Repeated key name in a blocking pop, and the double signal [22-08-2026] Nr 124 1c17a23

133. [Done] A parked client saw inside a transaction, and the lower case EXEC [22-08-2026] Nr 125 1c17a23

134. [Done] Waiters on one key are served in arrival order [22-08-2026] Nr 127 1c17a23

97. LRU compress, and LRU compress-then-evict.

    Two new eviction policies, next to the existing `allkeys-lru` /
    `volatile-lru` ones. The first compresses the LRU victim instead of
    deleting it. The second only deletes a victim that is already
    compressed, so an uncompressed hot-but-aging value gets compressed
    first and only leaves memory on a later pass. Compression itself
    should run without the shard lock: copy the value, compress off the
    lock, then swap it back under the lock if the key has not been
    rewritten in between. `dictionary::compress` already answers empty
    when compression does not pay, so that case keeps the uncompressed
    value.

    Related to 51 (a walk that compresses what partial writes left
    behind) but not the same job. 51 is a bulk reclaim. This is the
    memory-pressure path, driven by the LRU list the way eviction
    already is.

    Open questions, the access one first:

      - when a GET (or anything else that reads the value) hits a
        compressed key under either policy, should it be stored back
        uncompressed, left compressed, or only inflated for the reply?
        Inflating and storing uncompressed is the current SETRANGE
        habit and makes the next GET cheap, but it fights the policy:
        the same key will be compressed again as soon as it ages.
        Leaving it compressed keeps memory down and makes every GET pay
        the inflate. A third option is to inflate into the reply only
        and not rewrite the leaf. Settle by measuring GET latency and
        the compress/inflate churn on a space that actually sits near
        `pre_evict_thresh`, not by guessing.
      - `lru-compress-evict` needs a rule for a victim that will not
        compress (too small, or `dictionary::compress` said no). Evict
        it anyway, skip it, or treat it as already "compressed" for
        the purpose of the second pass? Skipping can livelock the
        sweeper on a space full of incompressible values.
      - names. Redis has no such policy, so these are barch's own.
        `allkeys-lru-compress` / `allkeys-lru-compress-evict` (and the
        volatile pair) would sit next to the existing spellings. Or a
        single `allkeys-lru` with a side switch for what the victim
        does. The first is easier to CONFIG GET. Settle when the
        CONFIG SET / `KSPACE OPTION` surface is designed, not before.
      - the off-lock compress still has to notice a concurrent SET.
        The swap-back is a compare of generation or of the leaf
        pointer; if it lost, the compressed copy is discarded. That
        is the same shape as a lost CAS, and it has to be true or
        this is just eviction with extra copies.

    Not urgent until 51 has a number for how much of a space is
    uncompressed under a real workload. If almost everything is
    already compressed, these policies have nothing to do.

98. [Done] User-defined Luau RESP functions [26-08-2026] Nr 145 9de0165

99. HTTP as a foreign source, on an asynchronous client.

    A new `<name>.foreign` kind, next to `mysql` / `postgres` /
    `luau`, that fills a miss with an HTTP GET (or a configured
    method) rather than a SQL query. Same coalescing and waiter as
    the existing sources: one in-flight request per key, parked
    GET, tomb on a 404, `-ERR FOREIGN` on a transport error. The
    client has to be asynchronous so it does not occupy a foreign
    worker for the whole round trip the way `sql.query` does today.

    httplib is already in `external/include` and is synchronous.
    Asio is already the RPC stack. Those are the two obvious
    libraries; a third would need a reason. Settle the library by
    writing one GET against a local test server and seeing whether
    the waiter, the query timeout, and cancellation on UNLOAD all
    still mean what they mean for SQL.

    Other points, once the library is picked:

      - the request. URL template with the same `?` / `$n` / `$$`
        macros as `foreign_query`, plus optional headers. A password
        in a header is the same secret problem as `foreign_dsn`:
        `file:` or `env:`, never the replicated configuration
        space.
      - which status codes are a miss (tomb) and which are an error.
        404 is the obvious miss. 204, 410, 301, 5xx are not obvious.
        A source that returns 200 with an empty body is another.
      - TLS. The RPC path already has certificates. Reuse them, or
        give the space its own, or talk HTTP only. Talking HTTP only
        is not acceptable for anything that carries a token.
      - pooling. SQL has `foreign_pool_size` and idle max age.
        HTTP/1.1 connections and HTTP/2 streams are a different
        pool, but the same knobs should still mean "how many
        concurrent calls" and "do not keep a dead socket".

135. ACL rights per key space, through KSPACE ACL.

    An ACL is a set of categories and nothing else. `AUTH` walks
    `user:cat:<user>:<cat>` out of the auth shard, `cats2vec` turns
    it into a bitvector, and `is_authorized` at
    asio_resp_session.h:188 compares that against the command's own
    vector. No part of it mentions a key space, so a user who may
    SET may SET in every space there is, and a space is not a
    boundary anyone can be kept inside.

    The surface is a KSPACE subcommand that varies a user's bitmap
    for one space, taking the same flags `ACL SETUSER` already
    takes:

        KSPACE ACL [KSNAME] SETUSER default -read -write +function

    which is anonymous users kept from writing or reading functions
    in that space while still being allowed to run them. KSNAME
    absent means the selected space.

    Verb first, name after, because every other KSPACE subcommand
    puts its verb at spos 1 and `kspace_spec::parse_options` reads
    it there - `EXIST`, `OPTION`, `DEPENDS`. `KSPACE KS1 ACL ...`
    with the name first is the other way round and would need a
    look-ahead, and it is ambiguous against a space that is
    actually called `acl`. With the verb first the optional name is
    unambiguous, since what follows it is always SETUSER, GETUSER
    or DEL.

    Why this composes with the function categories in 98's K, which
    is what makes the example above work at all:

      - invoking a user-defined function needs `function` and
        nothing else, because that is what its own declared cats
        default to.
      - SETF is `{"write","data","function"}` and GETF is
        `{"read","data","function"}`.

    So `-read -write +function` is exactly execute-only: SETF loses
    `write`, GETF loses `read`, the function itself keeps the one
    category it needs. Any nested `call` inside that function is
    checked against the same user and the same space, so an
    execute-only user running a function that calls GET is refused
    at the GET - a function cannot be used to launder a right its
    caller does not have.

    How the two bitmaps combine. The per-space entry is a set of
    explicit `+`/`-` overrides, not a replacement map: resolution
    starts from the user's global vector and applies the space's
    overrides on top. That falls out of what `acl_spec::parse_set`
    already builds - `cat[name] = (*value == '+')` - and it answers
    the default question the cheap way: a space with no entry
    leaves the user exactly as they are today, so nothing that
    works now breaks.

    Worth being explicit that this means a space rule can widen as
    well as narrow. `+function` on KS1 grants it there even if the
    user is globally without it, which is what makes "execute
    functions in KS1 only" expressible, and is the sort of thing a
    reviewer should not have to infer.

    Where it resolves. AUTH knows the user but not the space, so it
    loads that user's per-space overrides at the same time as their
    categories - spaces are few - into a small map on the caller
    keyed by canonical space name, and the check picks the vector
    out of it by `caller.kspace()`.

    That fixes the hazard that would otherwise sink this: today
    `run_params` caches the authorization result together with the
    command lookup, keyed on the command name alone, `prev_cn` plus
    `ic`, calling `is_authorized` only when the name changes. A
    right that depends on the space makes that cache wrong -
    `KS1:GET` then `KS2:GET` is one name and two answers. The fix is
    small, because `run_params` already knows when the space
    changed: it sets `should_reset_space` around a `space:CMD`
    prefix. Drop the cached authorization whenever the space
    changes, or key it on (name, space).

    Storage follows the existing shape one level down -
    `user:space:<user>:<space>:<cat>` - so the same prefix walk
    reads it. `>secret` is refused in the KSPACE form: a secret
    belongs to the user, not to their rights in one space.

    Built. `KSPACE ACL [KSNAME] SETUSER alice -read -write +function`
    writes `user:space:<user>:<space>:<cat>`, AUTH reads a user's
    overrides at the same time as their categories, and
    `caller::get_space_acl()` is the global vector with that
    space's differences applied. A space with no rule answers the
    global vector itself, so a user nobody has written a rule for
    costs nothing and behaves exactly as before.

    Three things worth recording out of building it:

      - the hazard was real and was where it was expected. The
        authorization check sat *inside* the lookup cache -
        `if (prev_cn != cn) { find; if (!authorized) refuse; }` -
        so a repeated command name was never checked again. Fine
        while rights were global, wrong the moment they vary: the
        test does open:SET, shut:SET, open:SET on one connection
        and each has to answer for itself. The check now runs per
        command; the resolution is what is cached, once per space
        change rather than per command.
      - a SETUSER states a space's whole rule rather than adding
        to it. Otherwise the rule accumulates and you have to read
        it back to know what a new one means.
      - `>secret` is refused per space: a secret belongs to the
        user, not to their rights in one place. `~pattern` is
        refused too, per 136.

    Still to answer:

      - `~pattern` filters, now refused rather than silently
        dropped - see 136. Implementing key-level rights for real
        is a separate job.
      - replication is the one that still matters. docs/ACL.md
        says outright that acl data does not replicate, and the
        auth store is a standalone `barch::shard` rather than a
        key space, so a per space rule reaches no replica. That
        was true of categories before this and is not made worse,
        but per space rights make it more obviously wrong.
      - docs/ACL.md needs the KSPACE ACL form, and its list of
        thirteen categories is now fourteen with `function`.
      - spaces that come and go. A rule naming a space that does
        not exist yet, and a space dropped by UNLOAD while a user
        holds rules on it. Keeping the rule is probably right,
        since the name can come back.
      - replication. docs/ACL.md says outright that acl data is
        stored separately and does not replicate, and the auth
        store is a standalone `barch::shard` called "auth" built in
        auth_api.cpp rather than a key space. Per-space rights are
        worth little if a replica cannot see them, so that gap has
        to be closed here or the limitation stated in the docs.
      - GETUSER for a space, and whether plain `ACL GETUSER` should
        show that space overrides exist. A right that is invisible
        in the obvious place is a right nobody will remember.

    Docs: docs/ACL.md says there are thirteen categories and lists
    them. `function` from 98's K makes fourteen, and the KSPACE ACL
    form wants its own section beside the `ACL` one.

136. [Done] ACL SETUSER accepted ~pattern and dropped it [22-08-2026] Nr 128 1c17a23

137. [Done] Live reload, settled by the compile epoch [06-09-2026] Nr 234 527bfe8

138. [Done] art::iterator::last() finds nothing in a single key tree [24-08-2026] Nr 129 1fee45f

139. [Done] foreign_script kept Luau source in the configuration space [25-08-2026] Nr 130 4761263

140. [Done] barch.call built a whole caller per command [25-08-2026] Nr 131 9d612c8

141. [Done] barch.space.NAME rebuilt a store interface per call [25-08-2026] Nr 132 9d612c8

142. [Done] SQL foreign tests wrote Luau source into foreign_script [25-08-2026] Nr 133 9d612c8

143. [Done] A function returning nothing took the server down [26-08-2026] Nr 134 9d612c8

144. [Done] Wall clock deadline documented, and two stale doc claims [26-08-2026] Nr 137 9d612c8

145. [Done] Keys are strings, in and out [26-08-2026] Nr 138 9d612c8

146. [Done] Stored functions are documented [26-08-2026] Nr 139 d99231f

147. [Done] Nested script calls are bounded [26-08-2026] Nr 140 d99231f

148. [Done] store.get tells a cached miss from an absent key [26-08-2026] Nr 141 d99231f

149. [Done] FLUSHALL leaves the configuration space alone [26-08-2026] Nr 142 d99231f

150. [Done] One Luau state per session, not per session and space [26-08-2026] Nr 143 d99231f

151. [Done] Luau memory is in the statistics [26-08-2026] Nr 144 d99231f

152. [Done] Ordered ART GET: two failed speedups, two correctness fixes [27-08-2026] Nr 146 1b1be6b

153. [Done] GET lower_bound on a stack path, not the heap trace list [27-08-2026] Nr 147 1b1be6b

154. [Done] Hybrid ART plus hash index [27-08-2026] Nr 148 1b1be6b

155. [Done] Hybrid keys as the default [27-08-2026] Nr 149 1b1be6b

156. [Done] Private one-shard ART for Luau [27-08-2026] Nr 150 1b1be6b

157. [Done] HNSW stored functions over Levenshtein [27-08-2026] Nr 151 1b1be6b

158. [Done] Git-driven functions from a checkout [27-08-2026] Nr 152 d045bf8

159. [Done] Non-luau checkout files become keys [27-08-2026] Nr 153 0e88261

160. [Done] Colon is the builtin, a dotted name is the stored function [28-08-2026] Nr 154 62b2324

161. [Done] MYSPACE:HNSW.SET is colon then dot [28-08-2026] Nr 155 62b2324

162. [Done] Space-aware require [28-08-2026] Nr 156 bea392b

163. [Done] Function sync require in a scratch space [28-08-2026] Nr 157 3c3548a

164. [Done] NumKong f64/f32/f16/bf16 in Luau [30-08-2026] Nr 158 5d27f3c

165. [Done] Luau nk vector slice [30-08-2026] Nr 159 5d27f3c

166. [Done] nk vectors are 0-based [30-08-2026] Nr 160 5d27f3c

167. [Done] nk vectors are 1-based like Luau [30-08-2026] Nr 161 5d27f3c

168. [Done] nk vector conversion constructors [30-08-2026] Nr 162 5d27f3c

169. [Done] nk vectors from a barch value buffer [30-08-2026] Nr 163 5d27f3c

170. [Done] nk vector dot, cosine, Euclidean [30-08-2026] Nr 164 5d27f3c

171. [Done] nk vector sum and average [30-08-2026] Nr 165 5d27f3c

172. [Done] Git function sync pin to a commit [30-08-2026] Nr 166 5d27f3c

173. [Done] simdjson in Luau [30-08-2026] Nr 167 5d27f3c

174. [Done] Crow HTTP for stored Luau [30-08-2026] Nr 168 7efe967

175. [Done] HTTP example README and deploy.py [30-08-2026] Nr 169 7efe967

176. [Done] transport() kind http vs resource [30-08-2026] Nr 170 7efe967

177. Full-path HTTP example with a foreign MySQL or Postgres space. An HTTP
    resource reads a barch key, and a miss (or the first access) goes to
    the foreign server, so the database is only reached through the
    stored function rather than from the client. Settle with a deploy.py
    like examples/http that starts the space as foreign=mysql or
    foreign=postgres, serves a resource, and a GET that fills from SQL
    and a second GET that hits the cached key.

178. [Done] Multi-threaded HTTP ingress [30-08-2026] Nr 171 6da0969

179. [Done] HTTP handlers use barch.store for session state [30-08-2026] Nr 173 a537170

180. [Done] HTTP Luau VM pool [30-08-2026] Nr 172 6da0969

181. [Done] HTTP session statistics [30-08-2026] Nr 174 4a1c729



182. [Done] CI killed mid-compile again, and the bare -j behind it [31-08-2026] Nr 175 a30ffaa

183. [Done] Shard .dat files by name in .gitignore [31-08-2026] Nr 176 2b8bce6

184. [Done] A pre-commit hook for force-added shard data [31-08-2026] Nr 177 22189c6

185. [Done] NumKong bf16 on gcc 11, and a container to catch it [31-08-2026] Nr 178 bee9f27

186. [Done] http.request inside stored Luau functions [31-08-2026] Nr 179 f302898

187. Exercise the outbound `http.request` client inside the existing
    multithreaded Crow ingress test, not just on its own. `httptest.py`
    already runs eight threads over /page, /echo and /sess while the same
    threads write over RESP, which mixes the VM pool with `store.locked`
    in a way the isolated fetch test does not. A handler must not fetch
    from its own Crow server there: that takes two pool slots per
    request and deadlocks once the slots run out, so the requests go to
    a separate upstream. Settle with a /fetch resource in that worker
    loop coming back correct every time, and the hazard written down.


187. [Done] Outbound fetches in the concurrent Crow test [31-08-2026] Nr 180 f302898

188. `transport()` with `kind = "resp"`, so a stored function key can
    expose several RESP commands under names of its own choosing, each
    with its own ACL and replication categories:

        return {
            kind = "resp",
            methods = {GETNAME = get_name, SETNAME = set_name},
            categories = {GETNAME = {"read"}, SETNAME = {"write", "data"}},
        }

    Consistent with `kind = "resource"`, and it closes two real gaps.
    Today every stored function is authorized against one fixed
    `function_cats()` whatever it does, so a read-only function needs
    the same rights as one that writes. And the resolve branch in
    `asio_resp_session` never replicates, so a stored function that
    writes is not sent on to destinations the way a builtin `is_write()
    && is_data()` command is.

    Categories are checked against `categories()` and an unknown one
    fails SETF rather than being ignored. Settle with: a key exposing
    two names under different categories, each callable; a user granted
    only one of the categories getting one command and refused the
    other; an invalid category refused at SETF; the write one reaching a
    replication destination and the read one not; and `FUNCTIONS`
    listing the exposed names.


188. [Done] transport() with kind = "resp" [01-09-2026] Nr 181 60cbcfe

189. [Done] The HNSW example on a resp transport() [01-09-2026] Nr 182 60cbcfe

191. [Done] SET vs GET memtier was hybrid still on [02-09-2026] Nr 183 d0a26c5

190. [Done] Low-level buffer access on the shard and in Luau [04-09-2026] Nr 207 ee012e3

192. Optimize SET. First the measurement DONE 183 stopped short of:
    memtier SET over a million keys with `ordered_keys` off and
    `hybrid_keys` off, so the hash path is measured on its own, at 1, 2
    and 4 threads with pipeline 50. Each thread count gets a fresh
    server, because a second run over the same keyspace overwrites
    rather than inserts and that is not the same cost. Settle with the
    three numbers, what they say about how SET scales, and where the
    time goes.

193. [Done] Increment bench too slow for the coverage CI [03-09-2026] Nr 184 f4bdc13

194. The ubuntu24 coverage CI segfaulted in `TestFetchLuau` and 85+ local
    runs at the runner's thread shape would not reproduce it, so there is
    nothing to debug from. Make that job produce a backtrace the next
    time any test faults: core dumps enabled, a core pattern that lands
    somewhere writable, and a step that runs gdb over whatever cores the
    run left. The build is already RelWithDebInfo so the symbols are
    there. Settle with: a run that shows the capture step in place and
    reports no cores on a green run, and a deliberately faulted binary
    proving the backtrace comes out readable.

195. [Done] Thread sanitizer on the fetch and chaos tests [03-09-2026] Nr 185 511cc84

196. [Done] Use-after-free on a session the collector retires [03-09-2026] Nr 186 511cc84

197. [Done] The resp io pool was announced before it was published [03-09-2026] Nr 188 fe5911e

198. [Done] The unlocked-mutex reports are a TSan limitation [03-09-2026] Nr 189 fe5911e

199. [Done] do_read and do_write hold the session again [03-09-2026] Nr 190 fe5911e

201. [Done] Triage of the ART and allocator races [03-09-2026] Nr 191 fe5911e

202. [Done] The const node cache no longer writes shared state [03-09-2026] Nr 192 82e9325

203. [Done] The hash_arena races are INFO again [03-09-2026] Nr 193 82e9325

204. [Done] INFO takes the shard latch [03-09-2026] Nr 194 82e9325

205. [Done] The per-command statistics are atomic now [03-09-2026] Nr 195 82e9325

206. [Done] A short test set that runs under a sanitizer [04-09-2026] Nr 196 82e9325

207. [Done] Per-test directories and ports, and ctest -j [04-09-2026] Nr 197 82e9325

208. [Done] The rest of the ports, and the suite in parallel [04-09-2026] Nr 199 82e9325

209. [Done] Tests make their own directory [04-09-2026] Nr 198 82e9325

210. [Done] ctest -j 2 in CI [04-09-2026] Nr 200 78817c3

211. [Done] The java and lua bindings are optional, and now actually optional [04-09-2026] Nr 201 78817c3

212. [Done] What breaks at SANITIZE_EXITCODE=66 [04-09-2026] Nr 203 52881bc

213. [Done] Four of the families behind exitcode 66 [04-09-2026] Nr 204 52881bc

214. [Done] The short set is clean under TSan, and 66 is on [05-09-2026] Nr 212 31470ca

215. [Done] simdjson takes a luau buffer, and now makes one [04-09-2026] Nr 205 07786c1

216. [Done] The web server takes and gives luau buffers [04-09-2026] Nr 206 64f0474

217. [Done] JSON round-trip, health, and internal counters over Crow [04-09-2026] Nr 208 ee012e3

218. [Done] Space flag vs shard file after load, SET-OK-GET-miss [04-09-2026] Nr 209 ee012e3

219. [Done] HTTP identity so `barch.call` can run under an ACL [04-09-2026] Nr 210 9c603b4

220. A fourth CI job that builds and runs only the TSan short set.
    `.github/workflows/ubuntu24-tsan.yml`: RelWithDebInfo with
    `-DSANITIZE=thread`, build `barch` alone, then
    `BARCH_TEST_SCALE=0.05 ctest -L short` serially, with the exit code left
    at the repo default of 66 so a report fails the job. The recipe is the
    one in `ci/README.md` and it passes locally (DONE 212), so what is not
    settled is runner specific: whether `setarch -R` is allowed on a
    GitHub runner, whether the shadow mapping survives ubuntu-24.04's
    `vm.mmap_rnd_bits`, and how long the job takes. Settle with the first
    push that shows the job green, and record the wall clock time.

221. [Done] ubuntu24-sanitize was the coverage job [05-09-2026] Nr 213 31470ca

222. [Done] Templated HTTP routes, and Crow's uninvited /static [05-09-2026] Nr 214 31470ca

223. [Done] Three TSan reports the CI found and the local runs did not [05-09-2026] Nr 215 dfda7a6

224. [Done] Running the TSan set under CPU pressure [05-09-2026] Nr 216 dfda7a6

225. [Done] The latch TSan could not see [05-09-2026] Nr 219 dfda7a6

226. [Done] A parked client that disconnects is never noticed [05-09-2026] Nr 217 dfda7a6

227. [Done] The chaos thread that had not stopped working [05-09-2026] Nr 220 dfda7a6

228. [Done] The defrag fragmentation read had no latch [05-09-2026] Nr 218 dfda7a6

229. [Done] The bench after the sanitizer work [05-09-2026] Nr 221 198ad57

230. [Done] The same bench at 5% writes [05-09-2026] Nr 222 198ad57

231. [Done] The 20:80 bench on the hash path alone [05-09-2026] Nr 223 198ad57

232. [Done] Six threads was measuring memtier, not barch [05-09-2026] Nr 224 198ad57

233. [Done] HTTP latency, and the 40ms every keep-alive request was paying [05-09-2026] Nr 225 198ad57

234. [Done] Crow patched for TCP_NODELAY [05-09-2026] Nr 232 527bfe8

235. [Done] Serving the file store, in C++ [05-09-2026] Nr 228 198ad57

236. [Done] A file store made of keys, in luau [05-09-2026] Nr 226 198ad57

237. [Done] barchd, barch as a program [05-09-2026] Nr 227 198ad57

238. [Done] LOADKEYS, and deploy.py retired [05-09-2026] Nr 230 527bfe8

239. [Done] An arena's pages can come from a named file [07-09-2026] Nr 255 c5df974

240. [Done] A key space for the boot imports [05-09-2026] Nr 231 527bfe8

241. [Done] Recording a port stopped starting a server [07-09-2026] Nr 254 c5df974

242. [Done] require out of the file store [05-09-2026] Nr 233 527bfe8

243. [Done] Compiled luau is invalidated when its source changes [06-09-2026] Nr 234 527bfe8

244. [Done] HTTP handlers reload too [06-09-2026] Nr 235 527bfe8

245. [Done] Publishing a change is opt in, and per name [06-09-2026] Nr 236 527bfe8

246. [Done] A script can publish what it wrote [07-09-2026] Nr 253 c5df974

247. [Done] require(what, true) [06-09-2026] Nr 237 527bfe8

248. [Done] A version in the file metadata [06-09-2026] Nr 238 527bfe8

249. [Done] A stored function on a schedule [09-09-2026] Nr 265

    Left open from it, and both were named in the original entry as the things to
    settle first: `scan_checkout` still skips the `configuration` folder, so no job
    deploys from a git checkout, and letting `configuration/cron/` through as a merge
    raises the question of how a checkout *deletes* a job it no longer contains. And
    the lease that decides which node runs a job in a replicated set - the first cut
    is node-local because that is the honest small version, not because the question
    is answered. `catchup` and `overlap = "queue"` are parsed and reported but behave
    as `false` and `skip`.

250. [Done] A cron ACL category [06-09-2026] Nr 239 c5df974

251. `"admin"` is not a category. `categories()` (`barch_apis.cpp:43`)
    lists fifteen names and `admin` is not among them - it is a *role* in
    `auth_api.cpp:37`, alongside `all`, `readonly` and `user`. But six
    commands register with it: `LOADFS` and `LOADKEYS` (`fs_api.cpp:547`,
    `:550`), `EXPORT` and `IMPORT` (`export_api.cpp:395`, `:396`), `HTTP`
    (`http_api.cpp:1345`) and `FUNCTIONS` (`function_api.cpp:1716`).

    `set_cats` runs the list through `cats2vec`, which ignores a name it
    does not know (`barch_apis.cpp:64`), so the bit is silently dropped
    and those commands are gated only by their remaining categories. The
    intent was clearly that they need something more than `write` and
    `data`; the effect is that they do not.

    Two ways out and they are not equivalent. Appending `admin` to
    `categories()` makes the six mean what they say - and immediately
    refuses those commands to every existing user who was getting them
    through `+write +data`, which is a rights change on an upgrade and
    cannot be quiet. Dropping the word from the six registrations makes
    the code honest about what it does today and changes nothing at
    runtime. Settle by deciding whether these six should have been
    privileged, and if so, say so in the release notes rather than in a
    surprise.

    Worth checking at the same time whether an unknown name should fail a
    registration outright. A resp `transport()` already refuses one
    (DONE: "a category that does not exist fails the SETF"), on the
    grounds that a name nobody recognises reads as "needs nothing" - which
    is exactly what happened here, in the builtins, where nothing was
    checking.

252. [Done] Git repositories in the configuration space [06-09-2026] Nr 240 c5df974

    Left open from it: rejecting a declared collision at the write rather than
    at the read, which needs a hook on SET into the configuration space -
    settings are ordinary keys and anyone can write one, so today a bad
    setting disables its repository and says why in FUNCTIONS STATUS. And a
    `FUNCTIONS REPOS` listing, if STATUS turns out not to be enough.

253. [Done] A git checkout in the file store, browsed in a browser [06-09-2026] Nr 241 c5df974

    Left open from it: the write half of the file manager REST - create, rename,
    move, copy, delete. The blocker is real rather than effort: the fs layout has
    no directory object, so a folder is implied by the paths under it and a rename
    is a rewrite of every key beneath a prefix. Doing that atomically wants the
    staging `load_fs_directory` already has, which means the operation belongs in
    C++ beside it rather than in a luau handler. A modification time in the
    metadata would go in at the same time, since the widget has a date column and
    nothing to put in it.

254. [Done] A directory interface over RESP [06-09-2026] Nr 245 and 247 c5df974

    `FS` for the file store, `DIR` for a key namespace with the separator as an
    argument. Left over, and not part of this: `git_repos.cpp` still parses
    `git/repositories/<name>/<setting>` by hand, and moving it onto `DIR` needs
    the slash-versus-colon question settled - the reader wants colons so that a
    directory of little files actually deploys, which is TODO 252's unfinished
    half. The original entry follows, for the reasoning.

    A directory interface over RESP. The browser example (DONE 241) was picked
    as a hard problem to see what was missing, and this is what it found.

    There are three ways to spell a path in barch today and no way to walk one.
    A configuration setting is `<space>.<setting>`, dotted. A directory imported
    by LOADKEYS or a checkout is `a:b:file`, colon joined - `key_in` in
    `function_sync.cpp`. The file store is `fs:m:/a/b/file` with real slashes,
    because there a path is content rather than a namespace. And DONE 240 added
    a fourth, `git/repositories/<name>/url`, which is the one that gave the game
    away: it is read by a range scan splitting on `/`, so the directory of little
    files it was supposed to be deployed from would import as
    `git:repositories:<name>:url` and never be seen. The story was file per
    setting; the code only ever accepted SET.

    The deeper miss is that the listing itself does not exist. `fmapi.luau` in
    the example lists a folder by ranging over every descendant key and
    deduplicating the first segment in luau - O(everything below) to show one
    level, written by hand because there was nothing to call. That is the third
    time a directory walk has been written in this codebase: `scan_checkout`,
    `gather`, and now a handler.

    So: a `DIR` family over ordinary keys, with the separator as an argument
    rather than a convention baked in, defaulting to `:` because that is what
    the importer already produces:

        DIR LS    <path> [SEP s] [LIMIT n] [AFTER name]
        DIR COUNT <path> [SEP s]
        DIR RM    <path> [SEP s]
        DIR MV    <from> <to> [SEP s]
        DIR CP    <from> <to> [SEP s]

    `LS` answers one level: each child with its name, whether it is a leaf or has
    children, and a leaf's size. `AFTER` pages it. Nothing else needs inventing -
    GET and SET already work on a leaf, because a leaf is just a key.

    Making the separator an argument is what makes this worth having rather than
    a fourth convention. `DIR LS fs:m:/repo SEP /` lists the file store, which is
    exactly the listing the example hand wrote. `DIR LS git` lists the
    repositories. One implementation, both layouts, and no adapter.

    It can be done in O(children) rather than O(descendants), which is the whole
    point: `LB` already exists (`keys_api.cpp:2366`, an ART lower bound), so a
    listing seeks to the prefix, takes the first key, derives the child, and then
    seeks past that child's whole subtree instead of reading it. It needs an
    ordered space - a hash routed one cannot range at all - and should say so
    rather than quietly returning nothing.

    What it deliberately is not: there is no directory object, so an empty
    directory cannot exist and `DIR RM` of a leaf and of a node are the same
    operation. No metadata, no times - those belong to the fs store, which has
    them.

    `MV`, `CP` and `RM` are the half that needs care rather than typing, and they
    are the same problem TODO 253 parked: a subtree spans shards, so atomicity
    wants the staging `load_fs_directory` already has rather than a loop. Settle
    that before writing them; `LS` and `COUNT` are read only and can land first.

    Doing this also settles the naming for what has not been built yet - the cron
    layout in TODO 249 is written with slashes on the same wrong assumption, and
    should be `cron:jobs:<name>` and `cron:conf:workers` so a directory of files
    deploys as one.

255. [Done] barch::staged, and three rollbacks deleted [06-09-2026] Nr 242 c5df974

256. A path based file API, and the layout to go under it. The design is in the
    session that produced DONE 241 and 242; this is the entry it is being built
    against.

    The layout changes from two key classes to three, so that a file's data is
    keyed by an id rather than by its path:

        fs:n:<path>      name  -> {id, size, type, version}   the directory entry
        fs:n:<path>/     a directory: {dir:true}, no id
        fs:i:<id>        inode -> {size, chunk, chunks, type, version}
        fs:c:<id>:<n>    one chunk, id and n both fixed width hex

    Three things that buys. A read stops rebuilding a key that carries the whole
    path, so a file with a 120 byte path stops paying for it on every chunk. The
    `|` in today's `fs:d:<path>|<n>` stops being a reserved character nothing
    enforces - no part of a chunk key comes from user text any more, so a file
    called `a|00000000` cannot reach another file's chunks. And a rename becomes
    a rewrite of name records only: moving a 1GB directory writes a few hundred
    bytes and touches no inode and no chunk, which is most of what the file
    manager's write half was blocked on.

    The cost, and the one invariant to keep: a listing would otherwise need an
    inode read per entry to show a size, so the name record carries `size`,
    `type` and `version` as well. The inode is the truth and the name record is
    a hint refreshed on every commit.

    The interface over it is `barch::fs` - `normalise`, `stat`, `read`, `write`,
    `mkdir`, `remove`, `list`, and a `file` handle that holds an id rather than a
    path so `open_id` never walks one. One shot calls are the handle underneath.

    Steps, each landing on its own with the suite as the check:

      0. [Done] `barch::staged` - Nr 242.
      1. [Done] the id allocator, `src/ids.h` - blocks, and the invalidation any
         clear of a space needs.
      2. [Done] `src/fs.h` over the new layout, and the importers onto it.
      3. [Done] `handle_file` onto it, the second metadata parser deleted.
         Landed with 2: the layout changed, and with no dual format reader every
         reader had to move at once. Nr 243.
      4. [Done] Directory markers, mkdir and rmdir - Nr 244.
      5. [Done] `barch.fs.*` in luau and the RESP `FS` family - Nr 243 and 245.
      6. [Done] The file manager write half - Nr 246.

    The old layout is incompatible in both directions and no dual format reader
    should be attempted: getting that subtly wrong serves half a file. A
    `fs:layout` marker is written by the new code, and a store holding `fs:m:`
    keys without one is refused with a message saying to re-import. Every store
    that exists was made by LOADFS and can be remade by re-running it.

257. [Done] HTTP STOP left the port open [06-09-2026] Nr 248 c5df974

258. [Done] A shop example [07-09-2026] Nr 249 c5df974

    The original entry follows, for what the data is.

    A shop example: `examples/shop`. The amazon-products.csv in
    `examples/shopping` as a storefront - a grid, a product page, a basket and a
    checkout - with the catalog as files in an fs tree keyed by category, and the
    product images cached on demand from `m.media-amazon.com` rather than shipped.

    It is there to put the pieces together on something that looks like a real
    site: fs directories as the category tree, `barch.fs` from luau, a files
    route for the static app, resource routes for the API, and a second key space
    holding images that fills itself from the network as they are asked for.

    The data is 1000 products, 55 columns, of which 14 are always filled and three
    are always empty. `categories` is a clean JSON array 2 to 9 deep, which is the
    category tree. `image_url` is filled on 995 rows and the images are small -
    WebP at 4 to 20KB, because the URL carries Amazon's size suffix - so the whole
    image set is about 10MB rather than the 800MB the Myntra file would have been.
    `final_price` is a quoted string, `initial_price` is the literal text `null` on
    186 rows, and `domain` disagrees with itself (`https://www.amazon.com/` on 793
    rows, `www.amazon.com` on 174), with 31 rows on .in and 2 on .co.uk, so prices
    are not comparable across the file.

259. [Done] A script can ask a foreign space to fill [07-09-2026] Nr 252 c5df974

260. [Done] A range reaches both key regions [07-09-2026] Nr 250 c5df974

261. [Done] barch.call returns the value, not the framing [07-09-2026] Nr 251 c5df974

262. [Done] An arena maps its pages back instead of loading them [07-09-2026] Nr 256 c5df974

263. [Done] A file source: a space that fetches a file it does not have [07-09-2026] Nr 257 c5df974

    All three parts are done: the source itself Nr 257, listing Nr 258, eviction
    Nr 259.

264. [Done] Half in memory: leaves from a file, the tree in RAM [08-09-2026] Nr 260 c5df974

265. Two tiers rather than two arenas. `arena_map` (DONE 260) splits by *what a
    page holds* - the tree stays in memory, the leaves go to a file - and the
    split is fixed for the life of the space. What it cannot do is put the hot
    leaves in memory and the cold ones on disk, which is what an archive with a
    working set actually wants: DONE 261 measured 4.6M reads a second when the
    leaves are cached and 27,770 when they are not, and a real workload sits
    between those and would like to choose where.

    The kernel already does a version of this - a mapped page that is read often
    stays in cache - and the numbers say it does it well: 0.74 major faults per
    read for a uniformly random access pattern over five times the memory is
    about what a perfect cache would manage. So the question is not whether to
    reimplement it but whether barch knows anything the kernel does not.

    It does know two things. Which pages hold leaves that a query is about to
    want, because it walked the tree to get there - that is a readahead hint,
    `madvise(WILLNEED)`, and it is cheap to try. And which spaces matter, because
    a space is a unit of configuration and the kernel has no idea that one of
    them is an archive and another is a session store; `arena_dir` per space
    rather than per server would let the archive spill and the session store stay.

    Settle by measuring whether a hint before a leaf read moves the 27,770 at
    all. If it does not, the honest answer is that the kernel's cache is the
    second tier and barch should say so rather than build another one.

266. [Done] Random reads when the leaves do not fit [08-09-2026] Nr 261 c8fe198

267. [Done] Random writes when the leaves do not fit [08-09-2026] Nr 262 c8fe198

268. [Done] arena_dir and arena_map per space [08-09-2026] Nr 263 c8fe198

269. [Done] What a 4 GB machine looks like [08-09-2026] Nr 264 c8fe198

270. The curve, not three points. DONE 264 measured 4 GiB, 2 GiB and "the whole
    box", which is three scattered points with the rest of the machine moving
    underneath them, and it cannot show where the fall-off starts. What is wanted is
    reads/sec and major faults per read against the cap, swept over one fixed
    dataset in one process - `systemctl set-property MemoryMax` on a live scope, so
    nothing but the budget changes between points. The expectation to check is
    whether the fall-off is a knee or a cliff: throughput is roughly
    1/(miss rate x device time), so faults per read should be near linear in how much
    of the dataset does not fit while ops/sec is its reciprocal, which would make the
    cliff arithmetic rather than a property of barch. Settle by sweeping 16G down to
    1G over the 5.8 GB set and plotting both.

271. [Done] Cron on the RESP server's io_contexts [10-09-2026] Nr 266 6a8cbe0

272. [Done] The shop's accounts and ratings, in the spaces that hold them [11-09-2026] Nr 267 ba62a22

273. [Done] A space handle went stale when the next space was opened [11-09-2026] Nr 268 ba62a22

274. [Done] The shop's shards, out of the example directory [11-09-2026] Nr 269 ba62a22

275. [Done] All three spaces in the shop README's "What is where" [11-09-2026] Nr 270 ba62a22

276. [Done] `require`'s argument, in the docs [11-09-2026] Nr 271 ba62a22

277. [Done] The shop, restyled off the docs [11-09-2026] Nr 272 ba62a22

278. [Done] The shop's category bar collapsed when the results were short [11-09-2026] Nr 273 ba62a22

279. [Done] `LOADFS` is additive, and now the docs say so [11-09-2026] Nr 275 ba62a22

280. [Done] A stale sub-category name in the shop's heading [11-09-2026] Nr 274 ba62a22

281. [Done] 992 products, when there are 7,344 [11-09-2026] Nr 276 ba62a22

282. [Done] Folded into 279 - the additive rule is stated in Named Key Spaces [11-09-2026] Nr 275 ba62a22

283. [Done] The file store, in the command index [11-09-2026] Nr 277 ba62a22

284. Sixteen registered commands are still undocumented. With the file store family
    in (TODO 283) the index covers 166 of the 182 names `src/*.cpp` registers. The
    rest: `SETF`, `GETF`, `REMF`, `KEYSF`, `CALLF` and `FUNCTIONS`, which is the
    whole stored-function surface and the one the Luau reference talks about
    without ever giving its commands; `HTTP`, which starts and stops the servers
    every example in `examples/` uses; `DIR`, the composite-key walk the shop's
    ratings list is built on; `FOREIGN` and `FOREIGN_MISS`; and five ordered-set
    commands - `BZPOPMIN`, `BZPOPMAX`, `ZLEXCOUNT`, `ZRANGESTORE`,
    `ZREMRANGEBYRANK`, `ZREVRANK` - that are plain omissions from a family that is
    otherwise complete. The page opens by saying it lists "every command the RESP
    interface accepts", which is the part that needs settling either way. The
    count in the chips has to move with it; it was wrong by three before this and
    the ACL category count was wrong by two.

285. [Done] The category list threw away your place [11-09-2026] Nr 278 ba62a22

286. [Done] Checkout, in three steps [11-09-2026] Nr 279 ba62a22

287. [Done] South African places, in one key space [11-09-2026] Nr 280 ba62a22

288. [Done] An account screen, and orders out of the catalog's space [11-09-2026] Nr 281 ba62a22

289. [Done] Folded into 288 - orders now live in `orders` with their code [11-09-2026] Nr 281 ba62a22

290. [Done] The street line, written the way people write it [11-09-2026] Nr 282 ba62a22

291. [Done] The apt and street inputs, on one line [11-09-2026] Nr 283 ba62a22

292. [Done] A key space viewer [11-09-2026] Nr 284 ba62a22

293. [Done] The integer reply, in the docs rather than in the code [11-09-2026] Nr 288 ba62a22

294. [Done] A range bound is held to the same rule as a key [11-09-2026] Nr 287 ba62a22

295. [Done] The port literals out of routetest.py [11-09-2026] Nr 286 ba62a22

296. [Done] The port literals out of pulltest.py [11-09-2026] Nr 289 ba62a22

297. [Done] `barch.pull` implemented, and the two pull tests registered [11-09-2026] Nr 291 ba62a22
298. [Done] The port literals out of pulldebug.py [11-09-2026] Nr 290 ba62a22

299. What `pull` and `publish` are supposed to be. Not urgent - a note to come
    back to rather than a plan.

    `PULL host port` works now (DONE 291) but it is a proxy, not a transfer: it
    registers a route per shard, and a route forwards the data command to the
    other node and hands back the reply. Nothing is copied and nothing is held
    locally - `barch.size()` does not move, which is what the two pull tests
    assert. That may be all it was ever meant to be. Four things say it might not
    be, and they are what to weigh up before changing anything:

    - **The declaration promises more than the route table does.** `pull` in
      `abstract_shard.h` says "keys can also be retrieved asynchronously becoming
      available later but at greater throughput". Becoming available later is
      caching or prefetch, and neither exists - every routed read is a
      synchronous round trip, every time. Either the comment is the spec and this
      is a third of it, or the comment is older than the design and should go.

    - **`publish` is an empty stub.** `barch::shard::publish` is `return true;`
      with no body, so `PUBLISH`/`B.PUBLISH` does nothing at all. It is the half
      that looks like it was meant to push contents somewhere, and it is the half
      a real transfer would need. Nothing asserts on it today, which is why it has
      been quiet.

    - **The route table is global, not per key space.** `get_routes()` is a
      function-local static indexed by shard number only. `ADDROUTE` validates
      the index against `call.kspace()->get_shard_count()`, which reads as though
      it were per space, but the table it writes is not. With one space that is
      invisible; the shop has five, so a route set while one space is selected
      silently applies to that shard number in all of them.

    - **A dead route drops silently.** On a network error `call_route` sets
      `routes[shard] = nullptr` and stops trying, so a node that goes away turns
      into local misses rather than errors. There is a `TODO: should we` in the
      code at that spot already.

    The question to settle first is which of the two pull is: reading another
    node's keys from here, or fetching its contents into here. Everything else
    follows from that, including whether "pull" is the right name for what it does
    now. A transfer would need a completion point and an answer for keys written
    locally while it runs, neither of which the route mechanism has any notion of.

300. [Done] Per key space compression, deliberate and in the maintenance thread [13-09-2026] Nr 301 abb345b

301. A socket client for Luau. Raw TCP from a stored function, the way
    `http.request` is HTTP and `sql.query` is a database.

    **The parking machinery already exists and is the hard part.**
    `fetch_luau.cpp` shows the shape: `park_call(L)` then `lua_yield`, and the
    completion pushes the return values - the pool thread is free while the call
    is out. It also shows the case that cannot yield: inside a Crow handler the
    coroutine holds a VM slot out of the space's pool, so it blocks the thread
    instead and the pool size is what bounds the damage. A socket client would
    inherit both paths and both explanations.

    **The sandbox argument is weaker than it looks.** `open_safe` opens no `os`,
    no `io` and no `debug`, and the examples lean on that - no clock, no files,
    no way out. But there is already a way out: `http.request` will connect
    anywhere, and there is no allowlist anywhere in the tree for it. So a socket
    client widens the shape of what a script can talk to, not the fact that it
    can talk. Worth deciding whether *both* should be gated rather than treating
    the new one as the dangerous one.

    What to settle. Whether a connection may outlive one call - a pooled socket
    needs an owner and a cleanup path, which is what the VM pool and the space
    handle map already had to grow. Which ACL category it answers to, since
    `function` is what lets a script run at all and this is a different kind of
    reach. Whether it is refused inside `barch.store.locked`, where `barch.call`
    and `sql.query` are already refused because a remote that takes its time
    would hold a shard lock for as long as it takes - that rule applies here
    unchanged and is the easiest part to get right by copying it. And whether a
    read timeout is required rather than optional, given the answer to the last
    one is that there is no safe unbounded wait anywhere in here.

302. [Done] A stored file is never half evicted [12-09-2026] Nr 294 2a2ce48

303. [Done] `allkeys-random` was enabled by asking for `volatile-random` [12-09-2026] Nr 292 2a2ce48

304. [Done] An `outbound` ACL category for `http.request` [12-09-2026] Nr 293 2a2ce48

305. [Done] The leaf LRU bit was never set, so LRU eviction was not LRU [12-09-2026] Nr 295 2a2ce48

306. What DONE 295 turned on, and has not paid for. Fixing the LRU bit made the
    read path a writer, which is what a true LRU costs and what `fs.h:145`
    already says. Five things followed that were dead code while the flag was
    stuck false. Four are done - the flags race (DONE 296), lookups stamping the
    candidates they compared (DONE 297), scans stamping everything they walked
    (DONE 298), and the CoW worry in item 2, which turned out to be unfounded.
    What is left is the measurement in item 3, and the first of the three
    options in item 5, which was looked at and deliberately parked.

    1. [Done] The flags byte race. Confirmed under TSan, then fixed by making
       `leaf::flags` a `std::atomic<flags_t>` with relaxed `fetch_or` on the
       stamp - 39 data races before, 0 after, reads unchanged in the generated
       code. Written up as DONE 296 on [12-09-2026].

       Two things about reproducing it, kept because they cost time. The probe
       has to go through the RESP server: `KeyValue::get` serialises on its own
       mutex (`swig_api.h:288`), so python threads through swig never overlap
       inside the store. And it has to bind its own port and fail if it cannot -
       the first run silently talked to a stale `barchd` left by another session
       and reported a clean zero. Mechanically, `python3` loading a TSan
       `_barch.so` needs `LD_PRELOAD=libtsan.so.2` or it dies on static TLS
       allocation, and `setarch -R` or it dies on an unexpected memory mapping.

    2. [Done - the premise was wrong] **CoW page first touch on a read.** The
       worry was that the stamp makes a GET inside a transaction copy a page it
       used to only read. It does not, because a read was already copying it.

       Under a transaction `cow` is non-null, and
       `arena::get_page_data(logical_address, bool)` (`hash_arena.h:746`) then
       routes *every* access through `get_cow_page` - which copies the page and
       marks it modified on first touch - without ever reading its `bool`
       argument, which is unnamed. Both `logical_allocator::get_page_data`
       overloads pass `true` regardless, so `read<T>` and `modify<T>` are the
       same call underneath. There is no path by which a read avoids the copy,
       with or without the stamp.

       The scope is also narrower than this item assumed. `BEGIN`, `COMMIT` and
       `ROLLBACK` are registered only with the valkey module (`NAME(BEGIN)`,
       `keyspace_api.cpp:677`) and are not in the RESP function map, so they
       cannot be reached over barch's own server at all. The other transaction
       is `staged.cpp`, and only when the space has exactly one shard.

       Honest about the evidence: the code above is conclusive, the measurement
       is not. A counter on `get_cow_page`'s first touch recorded zero across
       200 key writes and 200 reads, and zero again across an `FS SET` on a
       single shard space - so no positive control was ever constructed. "Could
       not reach the path" is weaker than "reached it and the counts matched",
       and if this is ever reopened that is the gap to close first.

    3. **The read path cost in general.** Still unmeasured, and neither DONE 296
       nor 297 changed that. The switch from `read<leaf>` to `modify<leaf>` is
       not the cost - both end at `get_page_data` with modify true and the arena
       ignores the argument - the store to the flags byte is, and it is now a
       `lock orb` rather than an `orb`. The in-process python benchmark cannot
       see it: ~1900 ns/get is swig and interpreter overhead and the difference
       between policy on and off came out inside the noise. A real number wants
       memtier, which is what TODO 192 is already asking for.

       The part of this that *was* about correctness rather than cost - a lookup
       stamping every candidate it compared, not just the key it found - is
       done, as DONE 297. Stamps per GET went from 6.00 on a hit and 4.00 on a
       miss to 3.00 and 0.00.

    4. [Done] Scans stamping everything they walk. Settled the other way: a key
       is marked recently used when someone looks that key up, and never
       otherwise. `range` went from 100,000 stamps over 20,000 keys to zero, and
       a `KEYS *` no longer marks a whole space as hot. DONE 298 on
       [12-09-2026].

    5. [Mostly done - see DONE 299] **The stamp races the bulk page readers, and
       the atomic did not fix it.**
       Found by re-running TSan after DONE 298 with a probe that scans as well
       as reads - six clients GETting one key while two run `KEYS`, two run
       `SCAN` and two run `RANGE` over the space, 15 seconds:

           allkeys-lru     5 data races   348 unlock-of-unlocked-mutex
           eviction none   0 data races   348 unlock-of-unlocked-mutex

       The GET-only probe that found the original 39 is clean now, so DONE 296
       did what it claimed. These five are a different shape and the atomic
       cannot reach them: the other side is not a flags access at all, it is a
       page sized copy of raw bytes.

           3x  shard::page (`shard.cpp:381`), the page copy SCAN does, against
               set_lru from a GET
           2x  shard::glob (`shard.cpp:1299`), the memcpy KEYS does, same other
               side

       `page()` and `glob()` take a shared latch and so does GET, so they run
       together by design - and the stamp is still the only write that happens
       under a shared latch. That is the same root cause as DONE 296, with a
       different victim.

       Benign in effect, and worth writing down *why* rather than leaving it as
       a feeling, because the reasoning is what a suppression would rest on:

       - The write cannot tear. It is `lock orb` on one byte - a single
         indivisible store - so the copy reads that byte as either the old value
         or the new one, never something in between. The static_assert on
         `is_always_lock_free` next to `make_size` is what keeps that true on
         any target.
       - Only one bit can differ between those two values. `set_lru` ORs in
         `leaf_lru_flag` and touches nothing else, so `deleted()`, `expired()`,
         `is_compressed()` and - the one that would actually matter -
         `large()` read identically either way. `large()` decides how
         `key_len()` is interpreted, so a wrong answer there would walk the page
         walker off the end; it cannot get one.
       - Nothing that consumes a copied page reads the LRU bit. `scan_page` and
         glob's `on_page` read keys, values and the other flags.
       - And the stamp really is the only flag mutation taken under a shared
         latch. Every other one - `set_tomb`, `set_compressed`, `set_volatile`,
         `set_hashed`, `set_deleted`, and `unset_lru` in the sweep - is on a
         write path holding the unique latch, so none of them can overlap a page
         copy at all. Checked, not assumed.

       So what is left is the formal UB on the non-atomic read side and TSan
       reporting it, which is enough to keep TODO 220's job red but is not a
       correctness problem on any machine this runs on.

       The better reason to change it is not the race at all. `lock orb` on
       every read of a hot key drags that cache line into Modified state on the
       reading core each time, so eight threads reading one key ping-pong one
       line between eight cores, once per read. Loading before storing - option
       two below - turns the steady state into a shared read that does not move
       the line, with a short flurry after each sweep clears the bit. That is a
       scalability fix with a race window shrink as a side effect, rather than
       the other way round.

       Three ways out. The second and third are done; the first is the real fix
       and is still open:

       - Stop writing under a shared latch. Park the address in a small per
         thread buffer on a read and drain it under the shard's write latch, in
         maintenance. Kills this whole class, and takes the `lock orb` off the
         read path entirely. Costs a buffer and a drain, and the clock gets
         slightly staler.
       - [Done] Load before storing: only `fetch_or` when the bit is actually
         clear. `leaf::set_lru` does this now. The common path compiles to
         `movzbl` + `testb` + a branch, so a hot key costs a plain load and the
         cache line stops moving; a real store happens once after each sweep
         clears the bit. Three TSan runs since - 5k keys for 15s, and 20k for
         25s twice - have not reproduced the race at all, against five reports
         in one run before it.
       - [Done] A suppression naming the two functions, in `ci/tsan.supp` with
         the reasoning above written out. Precaution rather than routine noise
         now: the window still exists in principle - a key read for the first
         time after a sweep clears its bit, while a page copy is in flight -
         and a job that goes red once a fortnight is worse than a reasoned
         suppression.

    None of it is a reason to undo DONE 295: the alternative is an eviction
    policy that says LRU and evicts at random. It is the bill for having one.

307. [Done] The LRU bit is a read bit again: no stamp on creation [12-09-2026] Nr 300 2a2ce48

308. [Done] A leaf shrinks in place instead of being reallocated [13-09-2026] Nr 302 abb345b

309. Check whether compression fragmentation can be improved. A reminder rather
    than a plan, carried out of DONE 301 and 302.

    Compression costs defrag passes - 1,672 with it off against about 65,000
    with it on, on a workload rewriting 20% of keys a round. DONE 302 established
    that this is not waste to be removed: `run_defrag` fires on
    `emancipated.get_added() / allocated`, and compressing moves bytes from the
    denominator to the numerator by definition, so the passes are how the freed
    bytes become whole pages again. Shrinking in place rather than reallocating
    did not change the count, and neither did the two other explanations tried.

    So the question is not "why so much defrag" but whether the freed bytes could
    be reused without waiting for a pass. The lever is `free_list::get`
    (`logical_allocator.h`), which is exact size binned: a 130 byte tail can only
    ever be handed to a request for exactly 130 bytes, and otherwise sits until
    defrag compacts it. Splitting a larger free block to satisfy a smaller
    request, and coalescing neighbours, would let a compression saving be reused
    where it was made. That is a real allocator change and would want its own
    entry, careful thought about the exact size invariant that keeps page walking
    aligned - a block handed out has to be filled by a leaf whose `next_leaf()`
    is exactly its size - and measurement against the numbers above.

    Worth a look when the defrag load actually shows up as a problem. It has not
    yet: the 22% memory saving is realised, the throughput cost is about 7%, and
    nothing is failing. This is here so the question is not lost, not because it
    is urgent.

310. The tests that spawn a valkey-server wait one second and hope. Reported
    from CI: the pull tests failed on one instance and not the others, looking
    like a timeout, with a valkey-server that "did not start".

    `pulltest.py:53`, `pulldebug.py:48` and `routetest.py:44` all do the same
    thing - `subprocess.Popen` the server, `time.sleep(1)`, then `Popen` a
    `valkey-cli --eval` that sets the data everything afterwards depends on.
    Neither the wait nor the cli is checked: the sleep is a fixed guess, and the
    cli's return code is never read.

    **Reproduced.** A readiness probe that starts the server exactly as the
    tests do and polls until it answers PING:

        idle                    ready after 0.04s
        16 busy loops on cpu0   ready after 0.50s
        64 busy loops           ready after 2.01s
        128 busy loops          ready after 4.01s

    Past one second the cli runs against nothing. Timing the same sequence the
    test uses and reading the return code the test throws away:

        idle          cli_rc=0  out='3'   - the lua ran, three keys set
        96 burners    cli_rc=1  'Could not connect to Valkey at 127.0.0.1:20696:
                                 Connection refused'

    So on a loaded runner the source keys are never created, and every assertion
    after that fails for a reason that has nothing to do with what the test is
    about. The 25x headroom on an idle box is why it passes everywhere else.

    **Why it hides locally.** These tests do not call `scale.workdir()`, so they
    run in a directory that already holds the shards from the last run. The
    source keys are still there from before, the assertions pass, and the failed
    cli goes unnoticed - which is exactly what happened here: both the idle and
    the loaded run of `pulltest.py` exited 0 while the loaded one had not run the
    lua at all. On a fresh CI checkout there is nothing to fall back on.

    **Fixed.** `scale.wait_for_port` polls until something answers, watching the
    Popen so a server that dies says so immediately rather than after the
    timeout, and `scale.run_checked` runs the cli and raises with its output when
    it fails. All three tests use both. Where the server cannot come up at all -
    port already taken - the message is now

        RuntimeError: the barch the module started exited with 1 before it
        accepted a connection on 127.0.0.1:20881

    rather than an assertion about a missing key three steps later.

    Under the 96 busy loops that made the cli fail before, all three now pass.
    They are also much faster, because the fixed sleeps are gone: TestBarchPull
    2.20s to 0.30s, TestBarchPullSource 11.14s to 0.37s - the second was
    sleeping ten seconds for something that takes a fraction of one.

    Two things to know if this is revisited. `routetest.py`, `pulltest.py` and
    `pulldebug.py` all killed a `cliProcess` at the end that no longer exists
    now the cli is run to completion - `routetest.py` failed on exactly that
    after the first edit, with the test body having passed. And these tests
    still do not call `scale.workdir()`, which is why a failed cli was invisible
    locally; that is worth doing but is a change to where they keep their data
    and was left alone here.

    Related but different: TODO 39 is about these same three tests *leaking* a
    server when they fail. Still live - there is a `valkey-server` on this
    machine that has been up for a day and twenty hours holding port 7911.

311. A Luau interface for libvips, with JPEG XL. Parked before any code was
    written - this is what the groundwork turned up, so the next attempt does
    not start from nothing.

    **The shape it would take.** `src/foreign/vips_luau.cpp` with a
    `luaopen_vips(lua_State*)` called from `open_safe` in `luau_driver.cpp`,
    beside `luaopen_simdjson`, `luaopen_nk`, `luaopen_crowhttp` and
    `luaopen_fetch`. Optional the way MySQL and Postgres are - `find_package`
    into `BARCH_HAS_VIPS`, an `#ifdef` guard in the source and a no-op
    `luaopen_vips` when it is off, which is exactly what `simdjson_luau.cpp`
    does for `BARCH_HAS_LUAU`.

    **Building it from source with meson fits the existing pattern.** OpenSSL
    and liburing are already done this way: `FetchContent_Declare` for the
    tarball, then `execute_process` to configure and make, then point at the
    static archive - `set(OPENSSL_LIB_PATH ${openssl_SOURCE_DIR}/libssl.a ...)`.
    Nothing is installed system wide and no sudo is involved, and the same shape
    takes `meson setup` and `meson compile`.

    **The trap, and it is the whole point of the entry.** libvips options are
    meson *features* defaulting to `auto` - `jpeg-xl` at `meson_options.txt:120`,
    and 37 others. Auto means use it if found and skip it silently if not. On
    this machine:

        glib-2.0 2.80.0, gobject-2.0, gio-2.0, expat 2.6.1   present (mandatory)
        libjxl, libturbojpeg, libpng, libwebp                MISSING

    so `meson setup` succeeds, `meson compile` succeeds, and the result cannot
    read or write a JPEG XL - or a JPEG, PNG or WebP. It shows up at runtime as
    a missing loader. Pass `-Djpeg-xl=enabled` rather than leaving it auto, so
    configure fails loudly instead, and the same for any other codec that is
    actually required.

    **Three more things that were checked.**

    - `meson` and `ninja` are on neither PATH nor as a python module, and meson
      needs ninja. Both are pip installable, but the project's venv is built by
      a test fixture long after configure time, so where meson comes from is a
      decision rather than a detail.
    - "static" is narrower than it sounds. `--default-library=static` gives a
      `libvips.a` that still links glib, gobject, gio and expat, which are shared
      here. A static libvips against a dynamic glib stack is what comes out
      unless someone goes a lot further.
    - The distro package is a shortcut worth remembering: Ubuntu noble's
      `libvips42t64` depends on `libjxl0.7`, `libheif1` and `libwebp7`, so
      `apt install libvips-dev` gives 8.15.1 with JPEG XL already on. That is the
      cheapest way to get something working to develop the Luau binding against,
      even if the shipped build is not what ends up in CI.

    **What to settle when it is picked up.** Whether images come in and out as
    buffers only - which keeps the property `open_safe` states out loud, that the
    compute libraries it opens have "no files, no clock, no network" - or whether
    a stored function gets to name a path, which is a hole in that sandbox rather
    than an API convenience. And whether a resize runs inline on the pool thread
    or parks the way `fetch_luau.cpp` does for I/O, since a large image is
    seconds of CPU, not microseconds.

312. [Done] `shard::dependencies` published without a lock, read by maintenance [13-09-2026] Nr 303 baf3474

313. [Done] `KSPACE RELEASE` validated the wrong names [13-09-2026] Nr 304 baf3474

314. [Done] Shard count recorded, checked on load, configurable, 7 in the shop [14-09-2026] Nr 305 c908ca1

315. barchd died during the shop's geo load, once, and has not been reproduced.
    Not much to go on, which is why this is written down rather than guessed at.

    **What happened.** A fresh shop at 7 shards, catalog loaded by `setup.sh`,
    then `geo.py --port 14000` to pull the address data. Divisions went in - the
    space reached 12,143 keys - and the process died during `load_roads`, the
    bigger half. `geo.py` then failed with `ConnectionRefusedError` because
    there was nothing listening. At the same time the shop was being used from a
    browser: product searches through `/api/index` and a registration through
    `/api/register`.

    **The one clue.** The log's last line was

        Unexpected error 9 on netlink descriptor 81.

    and nothing else - no barch error, no `barchd saving`, no `barchd stopped`.
    That string is glibc's, from `check_native` in
    `sysdeps/unix/sysv/linux/check_native.c`, which `getaddrinfo` uses to ask the
    kernel which interface addresses exist. Error 9 is EBADF: the netlink socket
    it was using had been closed underneath it. That is the shape of an fd closed
    by one thread while another is in `getaddrinfo` - a double close, or an fd
    reused after close - rather than anything about address data.

    Which fits where it happened: the shop resolves a hostname on every image
    fetch, `http.request(url)` in `imgsource.luau`, so a browse is a stream of
    concurrent `getaddrinfo` calls beside everything else.

    **What it cost.** The process went without saving, so the catalog `setup.sh`
    had loaded was gone and had to be reloaded. Anything since the last save goes
    with it - worth knowing before trusting a long unsaved session.

    **Not reproduced, and these were tried.**

      - `geo.py` alone, twice: 88,004 keys in 359s, exit 0, server fine.
      - `geo.py` with load beside it: four threads on `/api/index`, two on
        `/api/places` for both `street` and `sub`, one pulling random images, and
        a `POST /api/register` part way through, for seven minutes. geo.py exit 0,
        server alive, RSS 139 to 148 MB, no failed requests.

      - page bursts: 85 of them, each requesting the html, a search, the
        categories and 24 images all at once - about 2,000 image requests. This
        was built specifically to close the "curl one at a time is not a browser"
        gap. Survived, RSS 107 to 236 MB.
      - those bursts with `geo.py` writing underneath at the same time.
        Survived, geo.py exit 0, RSS 236 to 344 MB.
      - **a real browser**, driven by hand: searching continuously with different
        prefixes while the import ran. Did not crash. Reported as "just a little
        slow during the import", which is the write load competing with reads and
        is expected, "just loaded more images".

      - **a real browser session, recorded and replayed** - DONE 309. 292 requests
        over 100 seconds, 279 of them images, 226 of 291 arriving within 5ms of
        the one before, which is the browser parallelism no script reproduced.
        Replayed at recorded speed and three times at five times speed with 60%
        jitter: 294 of 294 every time, server alive after all four. So the exact
        traffic shape that was running when it died now runs on demand, and it
        still does not crash.

    So it is a one-off that nothing has reproduced, including the browser that was
    the last plausible difference - and now including a recording of one, replayed
    four times. Left open rather than closed because the
    netlink line is a real signature and worth recognising if it happens again -
    but nobody should go hunting it on this evidence alone.

    The one thing the reproduction attempts did show is unrelated and already
    known: the image cache grows without limit. Those runs plus the browsing took
    the space from 22,101 keys and 8.3 MB of data to 45,238 keys and 183.7 MB,
    with 4,546 images cached, exactly as `fs_cache_bytes` being unset predicts.
    See the note about it in the shop's README.

    **The evidence was destroyed, which is the annoying part.** `startshop.sh`
    redirects with `>` and truncated `data/barchd.log` on the next start, so the
    netlink line is quoted above from having read it and nowhere else. Anything
    looking at this again should copy the log aside first, and append rather than
    truncate.

    What would settle it. Run the same shop under ASan - a double close or a use
    after free on an fd is what it catches best - with a browser driving it
    rather than curl, while geo.py writes. Failing that, `strace -f -e
    trace=close,socket` on the resolver threads would name the thread that closes
    an fd another is still using.

316. [Done] Record incoming traffic into a key space and replay it [14-09-2026] Nr 306 c908ca1

317. [Done] Capture traffic to a file instead of into a key space [14-09-2026] Nr 307 c908ca1

318. [Done] A traffic file per thread, to avoid a shared lock [14-09-2026] Nr 308 c908ca1

319. [Done] Record the shop over HTTP, and replay it [14-09-2026] Nr 309 c908ca1

320. The `users` space is not there after a restart until something names it.

    Found while doing 319. A restarted shop answers `POST /api/register` with

        500  shop:35: FUNCTION no key space called users

    and keeps doing it. The data is on disk - `leaves_users_0.dat` and the rest -
    and `users:DBSIZE` says 12, but only once something has asked for the space by
    name. Asking for it is what builds it, and nothing in the request path does:
    the handler reaches it through `require("users:/modules/accounts.luau")`,
    which resolves a function in a space rather than asking for the space.

    So the shop is quietly half broken after every restart until someone touches
    `users`, which nobody would think to do.

    What is not yet known is which of the two it is: a space that exists on disk
    should perhaps be loaded at startup rather than on demand, or the `space:/path`
    require should build the space the way `get_keyspace` does. The first is a
    bigger decision - it means a startup cost proportional to what is on disk -
    and the second might be a one line fix in the require path.

    What would settle it. Restart the shop, confirm the 500, then try the require
    path: if `require("users:/modules/accounts.luau")` goes through something that
    could call `get_keyspace` and does not, that is the fix and the startup
    question can stay closed.

321. [Done] A recording keeps the headers it is told to [15-09-2026] Nr 318 5487c4f

322. [Done] A directory listing walked the subtrees it stepped over [14-09-2026] Nr 310 c908ca1

323. [Done] `RANGE` with a limit silently dropped keys [14-09-2026] Nr 311 c908ca1

324. [Done] A node prefix was only compared as far as it was stored [14-09-2026] Nr 312 c908ca1

325. [Done] TSan over the day's changes, and a probe in the short set [14-09-2026] Nr 313 c908ca1

326. `CONFIG SET` refused everything in one process, once, and will not come back.

    Seen while writing the TSan probe for 325: in a sanitizer build, a probe's
    first `config_set("traffic_file", ...)` straight after `barch.start()` was
    answered `could not set configuration value`, and from then on every
    `CONFIG SET traffic_capture on|off` in that process was refused too - on a
    fresh connection as well as the one that had been idle - while `PING`, `GET`
    and `CONFIG GET` kept working. Nothing was logged as `cannot set ...`, so it
    was a setter returning an error rather than a name being unknown or read
    only. A `CONFIG GET traffic_*` before the first set made it stick, reliably,
    which is what `test/trafficracetest.py` now does.

    **It no longer reproduces.** What was tried since:

      - 3 runs of the probe with that read removed, under TSan: clean.
      - 5 more with `CONFIG SET` restored to the old C-string argument reading
        through a temporary switch, under TSan: clean.
      - 25 fresh processes, each doing `barch.start()` then an immediate set with
        threads churning underneath, on the ordinary build: clean.
      - 6 runs pinned to one core with four busy loops on that same core - the
        `ci/tsan-stress.sh` trick for forcing unlucky interleavings: clean.

    **Two theories examined and rejected.**

      - *the argument read as a C string.* `CONFIG SET` was the only place in the
        command surface that treated an argument pointer as NUL terminated, which
        works only because the RESP parser writes a NUL over the CR after each
        argument. Measured: 7,200 pipelined and concurrent sets, and the C string
        never differed from the length view. Changed to `to_string()` anyway -
        depending on someone else's side effect is worth removing - but it is not
        the cause, and the switch above proves it.
      - *a startup race.* The 25 process and 6 starved runs above were built to
        catch `barch.start()` returning before the configuration is ready. They
        did not.

    What is left, and it is the likeliest thing: the build it was seen on was
    made before DONE 311 and 312 went in, so that process had the broken range
    walk and the truncated-prefix `lower_bound`. No mechanism has been found that
    connects either to `CONFIG SET` - nothing in `set_configuration_value` reads
    the store - so this is a suspicion, not a finding.

    What would settle it if it happens again: `BARCH_TRACE_CONFIG=1` now prints
    the name and value the server was actually asked to set, with their lengths,
    and the code the setter returned. That is the one instrument this was missing,
    and it is the reason the entry stays open rather than being deleted.

327. [Done] `-fanalyzer`: dropped the -Werror, triaged, one real trap fixed [14-09-2026] Nr 314 c908ca1

328. [Done] Acted on the cloud review: seven findings, seven fixes [15-09-2026] Nr 315 c908ca1

329. [Done] Compression on the sanitizer set, and what it found [15-09-2026] Nr 316 eb88f7f

330. [Done] The compression pass used the dictionary statics after they were destroyed [15-09-2026] Nr 316 eb88f7f

331. [Done] Three route calls built a `string_view` over a temporary [15-09-2026] Nr 316 eb88f7f

332. [Done] libstdc++ preloaded so ASan can see `__cxa_throw` [15-09-2026] Nr 316 eb88f7f

333. [Done] An ASan CI workflow, same set as TSan, compression on [15-09-2026] Nr 316 eb88f7f

334. [Done] The mutex suppressions blamed the wrong syscall [15-09-2026] Nr 319 38c2941

336. [Done] simdjson was pinned to `master`, so no two builds matched [15-09-2026] Nr 317 eb88f7f

337. The lock's per thread state moved behind getters.

    Asked for: `static inline thread_local` members may not be initialised for a
    thread that started before `main`, so wrap them in static getters returning a
    reference to a `thread_local` declared inside the getter.

    The three in `debuggable_server_lock`:

        static inline thread_local int tls_slot = -1;
        static inline thread_local hold_rec held[max_held];
        static inline thread_local int held_n = 0;

    `held` and `held_n` are the ones that matter. They are not diagnostics:
    `our_hold()` and `holds_this_shared()` read them to decide whether this
    thread already holds this lock, and both the acquire and the release paths
    branch on that answer. A thread reading them wrongly could skip an acquire
    and then release - an unbalanced unlock, which is exactly what the CI report
    says. So there is a mechanism, and it fits.

    What honesty requires saying with it: all three are POD with constant
    initialisers, `-1`, `{}` and `0`, so they are part of the module's TLS image
    rather than dynamically initialised, and the standard already promises a
    thread sees those values. Block scope is still the stronger form - it is
    initialised on first use per thread, with a compiler-managed guard, and it
    does not lean on the TLS image being in place in a library that python
    dlopens - and it costs nothing measurable, so this is worth doing whether or
    not it is the CI report's cause.

    One thing it does not fix, noticed on the way: `static inline` in a header
    means one copy per shared library. `_barch.so` and `liblbarch.so` are built
    from the same sources, so a process holding both has two `held` arrays, and a
    lock taken through one library and released through the other would see
    different state. Getters do not change that - a function local thread_local
    is per library too. Whether any process loads both is worth checking, and is
    its own entry if it does.

    What would settle whether it was the cause: CI. The report has never
    reproduced locally - not at `-O0`, not with the latch timeout at 1ms, not on
    one core, not under six busy loops - so the only evidence available is
    whether it recurs.

338. [Done] The module registered 347 shards while barchd used 17 [15-09-2026] Nr 320 38c2941

339. Seventeen shards surfaces the LRU stamp race on paths nothing suppresses.

    Found while measuring TODO 334, not looked for. The default shard count went
    from 347 to 17, and one chaos run under TSan went from reporting nothing to
    reporting 34 data races. Same code, same suppression file, same 8 second run:
    347 shards reports 0, 17 shards reports 34. Fewer shards means more threads
    on each one, so pairs that were spread thin enough never to overlap now do.

    `TestChaos` carries the `short` label, and both sanitizer jobs run that set
    with findings fatal (`SANITIZE_EXITCODE=66`), so as it stands the TSan job
    goes red on this.

    The one traced all the way is the LRU stamp again - DONE 295 put a write on
    the read path, `leaf::set_lru` does a one byte `fetch_or` under the shared
    latch, and the other side is a bulk page copy: `heap::buffer`'s constructor
    inside `logical_allocator::iterate_pages`. That is the same pairing
    `ci/tsan.supp` already argues is harmless for `shard::page` and
    `shard::glob`, and the argument carries over unchanged - single byte atomic
    write, only `leaf_lru_flag` differs between the two values, nothing that
    reads a copied page looks at that bit. What does not carry over is the
    suppression, because it names those two frames and not `iterate_pages`.
    The reader reaching it here is `has_container_of` through
    `sharded_store::with_key_read`, called from `RPOP`.

    So there are three ways out and they are not equally good:
      - name `logical_allocator::iterate_pages` in `ci/tsan.supp` too. Cheapest,
        and it grows the list of frames that have to be enumerated every time a
        new reader path reaches the stamp.
      - move the stamp off the read path, which TODO 306 item 5 already carries.
        That removes the whole family rather than one more frame of it.
      - leave the default at 347 in CI only. Dishonest: it would hide from the
        job the shape that is now the product's default.

    What has not been established: whether all 34 are this one family. The
    counts by frame - 12 `set_lru`, 27 `has_container_of`, 7 `make_leaf`, 6
    `free_leaf_node`, plus `logical_allocator::new_address` and `free` - suggest
    at least a second group around leaf allocation and freeing, and those are
    not obviously harmless. Reading a few of those reports is the next step, and
    it should happen before anything is suppressed.

340. [Done] Resident bytes per arena, with mincore [15-09-2026] Nr 321 0acdd7b

341. [Done] A subtotal of the named arena mappings [16-09-2026] Nr 322 0acdd7b

342. [Done] Luau states allocate through the heap namespace [16-09-2026] Nr 323 94a9e7c

343. [Done] The LRU stamp race, suppressed on the writer [16-09-2026] Nr 324 29b69ad

344. A leaf and node lifecycle race family, left over once the stamp went.

    Found by TODO 343's verification, not looked for. With the LRU stamp gone, a
    chaos run at 17 shards reports 1 and then 9 races in two runs against 34
    before, and none of them mentions the stamp at all - `set_lru`, `set_leaf_lru`
    and `set_flag` appear zero times. So removing it cleared its own family
    completely and uncovered another one underneath, varying run to run.

    By frame across those nine: `logical_allocator::new_address` 8,
    `art::free_leaf_node` 4, `art::encoded_node_content` 4, `art::make_leaf` 3,
    `logical_allocator::read` and `::free` 2 each. Allocation and freeing of
    leaves and nodes, in other words, which is the second group TODO 339 noted
    and called not obviously harmless.

    The one traced all the way is representative of the shape:

      - write, holding the write latch: `allocated += size` in
        `logical_allocator::new_address` (logical_allocator.h:981), under
        `art::make_leaf` from `shard::update`;
      - read, holding no write latch: `if (!allocated)` in
        `basic_resolve` (logical_allocator.h:585), under `read<art::leaf>` ->
        `peek_leaf` -> `art::iterator::key()`.

    `allocated` is a plain `uint64_t` at logical_allocator.h:440, and the read is
    not debug only: `test_memory` is 1 in src/constants.h for every build, so
    every `basic_resolve` tests it. That makes this one of the most travelled
    lines in the tree rather than an instrumentation corner like the
    `capture_writer_stack` family already suppressed here.

    What is not established, and matters before anything is changed: whether the
    reader legitimately lacks exclusion. A writer holds the unique latch, which
    drains readers, so a reader should not overlap it - but barch's readers are
    excluded by `write_intent` and the per-core slots rather than by a mutex TSan
    can see, and the only reason TSan knows about that at all is the
    `__tsan_acquire`/`__tsan_release` pair on the lock object. So this is either a
    real gap in the iterator's locking or a hole in what those annotations
    express, and the two need telling apart before either is called a bug.

    Worth more than the torn counter if it is real: the same `basic_resolve`
    resolves a pointer into the arena, and `new_address` can grow the arena with
    `mremap`, so a reader that is genuinely unsynchronised here is not reading a
    stale number, it is holding a pointer that may have moved.

    What would settle it: whether `art::iterator` holds the shard's shared latch
    across `key()`, and whether the report survives making `allocated` atomic -
    if it does, the exclusion is the problem and not the field.

    Note for CI, and it is the important part. Findings are fatal in the TSan
    job, so this family is enough to fail it - but only sometimes. Five chaos
    runs at the job's own `BARCH_TEST_SCALE=0.05`, same binary, stamp already
    gone: 0, 0, 4, 0, 0. Four runs in five report nothing.

    That explains the thing that looked like a puzzle. 0acdd7b and 94a9e7c have
    the same shard count and the same stamp, 0acdd7b was pushed on its own 39
    minutes earlier so it had its own run (the origin/main reflog shows two
    separate pushes), and 94a9e7c changes nothing that executes in that job -
    TestBarchd is not in the short set and the new functions are only reachable
    through KSRESIDENT. One passed and one failed because the job is a coin flip,
    and the CI failure reported exactly one race, which is the count most likely
    to come out zero instead.

    So a green TSan job is currently weak evidence: at 0.05 it would pass about
    four times in five with this family still present. Before the shard default
    changed the same test reported 0 races at 347 shards, so what happened is
    that a job which was genuinely clean became one that is usually clean. Worth
    deciding whether the job should run the set more than once, or at a larger
    scale, so that a pass means something.

345. [Done] The named subtotal now follows the mapping, not the config [16-09-2026] Nr 325 29b69ad

346. [Done] One function for the arena's usage counters [16-09-2026] Nr 326 29b69ad

347. [Done] The malloc mode for page data is gone [16-09-2026] Nr 327 29b69ad

348. [Done] barchd can bound its own cgroup memory.max [16-09-2026] Nr 328 29b69ad

349. [Done] Off releases the limit, if barch set it [16-09-2026] Nr 329 04794dd

350. [Done] The cgroup limit comes off at shutdown too [16-09-2026] Nr 330 04794dd

351. [Done] queue_file, Tape's QueueFile ported to C++ [16-09-2026] Nr 331 04794dd

352. [Done] aof_durability: none, timer, each or a byte threshold [16-09-2026] Nr 332 04794dd

353. [Done] An AOF record format with a crc32c [16-09-2026] Nr 333 04794dd

354. [Done] aof::log, with checkpoints meaning saved [16-09-2026] Nr 334 04794dd

355. [Done] The AOF write path hook [16-09-2026] Nr 335 6a3ba78

356. [Done] AOF replay on load [16-09-2026] Nr 336 6a3ba78

357. [Done] The change log is opt in per space [16-09-2026] Nr 337 dd325b1

358. [Done] Replay checks the log belongs to this space [17-09-2026] Nr 338 dd325b1

359. [Done] A change log nobody asked for is now said out loud [17-09-2026] Nr 339 dd325b1

360. [Done] Precompiled headers and a unity build for barchd [17-09-2026] Nr 340 dd325b1

361. [Done] Merge the two copies of the local filesystem helpers [17-09-2026] Nr 341 5b3d1bf

362. [Done] Say something on the blind returns in replay_change_log [17-09-2026] Nr 342 4e0dee0

363. [Done] Test the queue_file ring wrap and the truncated write [17-09-2026] Nr 343 ce84dd3

364. A rare SIGABRT in the redispytest runs: run_defrag aborts. CAUGHT.

    One full suite run failed with `33 - TestRespClientLocal (Subprocess
    aborted)`. Nothing since has reproduced it:

    - 200 sequential runs under gdb: clean
    - 40 rounds of 4 concurrent runs under gdb: clean
    - 30 full suite passes with PYTHONFAULTHANDLER=1: clean
    - 150 runs under ASan: no abort (a different failure, see 365)

    So it stands at about 1 in 400 and only ever appeared inside a full suite
    run. The output was lost - `Testing/Temporary/LastTest.log` is overwritten
    by the next pass, which is worth knowing before hunting it again.

    What is known about its shape. "Subprocess aborted" is SIGABRT, and this
    test runs the module in process rather than a separate server, so it is an
    abort inside the module. `redispytest.py` starts and stops the in process
    server four times per run, with a `stop()` immediately after a `start()`
    each time, which makes a teardown race the obvious suspect. Not a port
    clash: ctest hands out ports from 20000 and this test's own default is
    14000, and nothing was listening.

    **Caught on 18-09-2026**, as TestRespClientLocalRESP3, in an ordinary full
    suite run - not by any of the loops. barch aborted itself:

        ordered key not found
        There's a bug and we cannot continue - last reason
          [ key not marked as deleted but it was not found ]
          abort_with
          art::page_iterator_ptr
          art::page_iterator
          barch::shard::run_defrag
          barch::shard::maintenance

    So the teardown race theory was wrong. It is `run_defrag` walking a page
    and finding a leaf that the index says is live and the page says is not,
    on the maintenance thread - nothing to do with the start/stop cycling the
    test does, which is why 360 isolated runs and 30 suite passes never saw
    it. What it needs is a maintenance tick landing on a page defrag at the
    wrong moment, and redispytest only provokes that because it loads shards
    four times in one process.

    That also explains the rarity, and says the hunt was looking in the wrong
    place: no amount of running redispytest was going to find it, because the
    window belongs to defrag and not to the test.

    Still open, and now a different question: what makes a leaf reachable from
    the index but absent from the page it names. Related, and probably the
    same family: 311, 315, 320, 326, 337, 339, 344.

    The full log is kept at
    `<scratch>/abort_catch.log` for this session; `Testing/Temporary/LastTest.log`
    is overwritten by the next pass, which is how the first one was lost.

    Seen again on 19-09-2026, TestRespClientLocalRESP3 in a full suite run at
    -j4: the same "key not marked as deleted but it was not found" from
    `run_defrag` through `art::page_iterator`. Two in two days now, both in
    the RESP3 run of redispytest.

    Possibly again on 24-09-2026: TestRespClientLocal (not RESP3), "Subprocess
    aborted" in one full suite run at -j6. The output wasn't kept, so which abort
    it was isn't known. Six runs of that test on its own and two more full suites
    with --output-on-failure saved were clean.

    Seen again on CI, 24-09-2026 14:06, TestRespClientLocalRESP3: the same
    "key not marked as deleted but it was not found" from run_defrag →
    page_iterator on a maintenance thread. That thread was started in the first
    start/stop cycle and fired about a second later, during the fourth cycle,
    about 13ms after "Loaded 17 shards" lines from a RESP worker. Checked and
    ruled out: a LOAD replacing a shard under defrag. Only a range sharded space
    loads under the space lock alone (repl_api.cpp LOAD, is_stateful_sharding);
    a hash sharded one, which is what redispytest uses, loads under each shard's
    own latch, and defrag takes that latch per page.

365. The ASan build directory is not trustworthy, and shares test fixtures.

    Three separate things found while trying the initialisation-order flags in
    it, none of them about barch's code:

    - It could not configure at all. Its simdjson clone predates the commit
      pin from TODO 336, so a reconfigure died with `fatal: reference is not a
      tree: f9c973a`. Fixed by fetching that commit into
      `cmake-build-asan/_deps/simdjson-src`, which means the ASan build had
      not been runnable since the pin changed.

    - It is configured `CMAKE_BUILD_TYPE=RelWithDebInfo`, the same as the
      ordinary build, so `TEST_BUILD_DIR` is `RelWithDebInfo` for both and
      both use the one shared `test/RelWithDebInfo` in the source tree.
      Building the ASan directory while the ordinary suite is running breaks
      the running suite: one pass failed with `Could not find executable
      /home/test/barch/test/RelWithDebInfo/TestStarter` for twenty odd tests,
      at exactly the minute the ASan reconfigure was replacing it.

    - Its `redispytest` run reports `shards [ 347 ]`, the old default, and
      creates 347 shard spaces, while the ordinary build reports 17. That is
      not explained. Ruled out: stale data in the working directory (moved
      aside, came back freshly written and still 347), a config file, the
      environment, and a leftover 347 in the sources. `CONFIG GET
      internal_shards` from that same module answers 17, and starting it by
      hand in an empty directory prints 17, so the two cannot both be right.
      Mixed vintage objects in that swig target is the suspicion - `--target
      barch` did almost no work when rebuilt - but it is a suspicion.

    The 347 matters because the only failure the ASan runs produced is tied to
    it: `assert(r.dbsize() == 4)` at redispytest.py:59, in about a third of
    runs, always in round 1, the round that sees that state. Four keys are
    expected - a, b, c from a pipeline plus hello.

    What would settle it: wipe `cmake-build-asan` and configure and build it
    from scratch, then see whether 347 and the dbsize failure survive. Nothing
    should be concluded about barch from that directory until then.

    On the flags themselves there is nothing to settle: 40 alternating pairs
    with and without `check_initialization_order`, `strict_init_order` and
    `intercept_tls_get_addr` failed 15 and 14 times respectively, all of them
    the dbsize assert, and not one sanitizer report of any kind in either arm.

366. [Done] A "queue" transport kind, backed by queue_file [18-09-2026] Nr 344 8a12090


367. [Done] Static libcurl bumped to 8.19.0 [18-09-2026] Nr 345 778b704

368. [Done] Send email from Luau over SMTP [18-09-2026] Nr 346 778b704

369. [Done] Range offsets by node counts [18-09-2026] Nr 347 053cb05

370. [Done] Range offsets documented [18-09-2026] Nr 348 d3463c9

371. [Done] Files routes can serve another key space [18-09-2026] Nr 349 d3463c9

372. [Done] Offset on the Luau range [18-09-2026] Nr 350 d3463c9

373. [Done] Offset on directory listings [18-09-2026] Nr 351 d3463c9

374. [Done] barch.fs.space for another space's files [18-09-2026] Nr 352 d3463c9

375. [Done] Coverage badge push survives main moving [18-09-2026] Nr 353 d3463c9

376. TestHashBenchy (zwbenchy.lua) sometimes hangs in the coverage job until
    ctest kills it at 600 s: runs 35138016417 (16-09-2026) and 35361987568
    (18-09-2026), 2 of the last 30 coverage runs. Otherwise it takes 9-22 s
    there and 4-6 s in the plain CI jobs, and it passed on the same commit
    in every other job, so it's a hang, not slowness. It's a hash mode
    benchmark - a million B.SET and B.GET with ordered_keys off, then
    B.CLEAR and B.SAVE, run through TestStarter against the valkey module -
    and touches none of the range code from 369. Nothing yet says where it
    stops: the log ends at the timeout with no output from the script, and
    the core dump step found no core because ctest's kill isn't a fault. What
    would settle it: a stack from the hung process, for instance ctest's
    timeout signal changed to one that dumps core, or a watchdog in the job
    that runs gdb on TestStarter's children after a few minutes.

377. [Done] RANDOMKEY walked a shard with no lock [18-09-2026] Nr 354 5ac19d6

378. [Done] sp:call runs a command in another space [19-09-2026] Nr 355 4e11e3b

379. [Done] Pooled async RESP client for Luau [19-09-2026] Nr 356 0a7593b

380. [Done] Coverage badge push fails on a dirty worktree [19-09-2026] Nr 357 8e0a1b0

381. [Done] SETF GETF KEYSF QUEUE REMF CALLF FUNCTIONS in the RESP index [19-09-2026] Nr 358 8e0a1b0

382. A graph API beside FS, keyed by edges rather than paths. Paths are not
    stored literally in key names. The store is the classic recursive
    relational tuple {parent_id, name, id}: interior nodes hold structure
    only (like directories), leaf nodes hold content like FS files, and any
    node may occur >= 1 times along different paths. Leaf content reuses the
    FS inode/chunks machinery verbatim (fs:i:/fs:c: keys); only the edge
    table is new. Both doors: GRAPH * over RESP mirroring FS
    (LS/STAT/GET/PUT/MV/CP/RM/MKDIR/RMDIR plus LINK/UNLINK and BFS/DFS) and
    barch.graph in Luau. Closed world: no HTTP files routes, no require()
    out of it, no fs_source fetching.

    Settled by question (20-09-2026): duplicate (parent_id, name) edges are
    allowed, each with its own edge id; path resolution takes the
    first-created edge and STAT shows edge ids for precise ops. Cycles are
    allowed between interior nodes, with BFS/DFS carrying a visited set and
    a max-depth guard; leaves are terminal and cannot have children. Both
    verbs: UNLINK removes one edge (node data goes when its last edge is
    gone, refcounted), RM deletes the node and every edge pointing at it.
    CP always duplicates bytes, LINK adds a second edge to the same node
    id. BFS/DFS start from a path (or id), return node id plus the path
    taken, first visit wins. Traversal queues/visited sets live in
    ephemeral per-command 1-shard scratch spaces with a named mapped file,
    destroyed at command end, not in std C++ containers.

    What is still uncertain: the exact key layout for nodes vs edges
    (graph:n:/graph:e:/graph:layout marker names, edge id allocation via
    ids.h blocks, refcount storage), how a 1-shard ephemeral scratch space
    with arena_dir/arena_map is created and torn down per command without
    leaking or racing the maintenance thread, whether graph leaves share
    fs:layout or need their own marker, the GRAPH subcommand argument shapes
    (BFS/DFS depth/LIMIT/OFFSET, LINK/UNLINK arity, STAT line format with
    edge ids), barch.graph function parity and ACL categories, eviction
    interplay (may_evict must refuse graph keys the way it refuses fs: ones,
    plus whole-node eviction story), and version/type metadata on leaves.
     Settle by building src/graph.h + src/graph_api.cpp onto staged/ids.h,
     registering GRAPH like FS, adding barch.graph beside barch.fs, and
     checking with a graphtest.py exercising duplicates, cycles, LINK/UNLINK
     vs RM, and BFS/DFS visit-once order.

383. [Done] Rewrite the user-facing prose in docs/index.html [20-09-2026] Nr 359 520be28

384. [Done] Rephrase remaining negative-logic sentences in docs/index.html [20-09-2026] Nr 360 520be28

385. [Done] Update limitations for the current roughly 512 KiB key/value budget [20-09-2026] Nr 361 520be28

386. [Done] Correct persistence wording in docs/index.html [20-09-2026] Nr 362 520be28

389. [Done] Document queue and cron services in docs/index.html [20-09-2026] Nr 365 520be28

390. [Done] Remove audience-irrelevant contrastive explanations from docs/index.html [20-09-2026] Nr 366 520be28

391. [Done] Create the documentation standard for future agents [20-09-2026] Nr 367 520be28

387. [Done] Graph edge ids widened to 16 hex [20-09-2026] Nr 363 afd983b

386. [Done] The unity build holds with the graph files in it [20-09-2026] Nr 362 afd983b

    The unity build did not hold with the graph files in it. Adding
    src/graph.cpp and src/graph_api.cpp forced a SKIP_UNITY_BUILD_INCLUSION
    carve-out in CMakeLists.txt, because the new files collided with names
    already taken in other translation units that land in the same unity
    batch. Renaming the helpers so every file compiles inside the batch is
    the fix; the carve-out goes away with it.

    Known collisions, all `static` or anonymous-namespace helpers that were
    fine alone and break once concatenated:

      - queue_file.cpp vs message_queue.cpp: both define put_u32 / get_u32 /
        put_u64 / get_u64 in an anonymous namespace - little endian on disk
        in one, big endian in the other, so they cannot share one either.
      - graph_api.cpp vs dir_api.cpp and fs_api.cpp: as_text, upper,
        option_at, ls_line are the same names with compatible but not
        identical shapes (ls_line takes a graph edge/node here, an fs
        entry there).
      - graph_api.cpp's own whole_number, stat_line, read_page and
        page_opts are unique today but sit in a bare anonymous namespace,
        so the next file with a helper of the same name breaks the same
        way.
      - graph.cpp already moved its helpers into `graph_anon`, which is
        why only the layout constant needed qualifying at the use site
        (graph_anon::LAYOUT_KEY / LAYOUT in batch::commit).

    Settle by giving each file's helpers a named home - graph_api_anon,
    queue_file_anon, message_queue_anon, following the graph_anon shape -
    deleting the SKIP_UNITY_BUILD_INCLUSION block, and rebuilding barchd
    with unity on. graphtest.py is the check that nothing renamed broke.

    Closed 20-09-2026 without the carve-out after all (see DONE 362): the
    named namespaces alone were not enough. `using namespace` at file scope
    still pulls both candidate sets into the unity TU, so the call sites go
    ambiguous again instead of resolving. The rename is what holds -
    qf_/mq_ prefixed codec helpers and graph_ prefixed api helpers - with
    the     namespaces kept as documentation of ownership. graph.cpp keeps its
    `using namespace ::graph_anon` because its helpers were already
    uniquely named (ghex16, gunhex, gquoted...).

387. Widen the graph edge id to 16 hex. Edge keys are `graph:e:<16 hex
    parent>:<8 hex edge>` and `graph:r:<16 hex child>:<8 hex edge>`
    (graph.h, graph.cpp `edge_key` / `rev_key` / `parse_edge`), so the edge
    half of the shared `ids.h` "graph" sequence caps at 2^32 while node ids
    run the full 2^64. A leaf PUT burns three ids (node, inode, edge) and a
    MKDIR two, so edge exhaustion arrives well before anything near 2^32
    writes. 16 hex costs 8 bytes per edge and reverse key - the two key
    prefixes stay the same length ratio as the FS chunk keys, which already
    carry `fs:c:<16 hex>:<8 hex>` without complaint at 524 KiB values.

    Settle by switching both key builders and every parser/scan that
    assumes the 8 (`parse_edge`, the `prefix.size() + 8` guards in the RM
    reverse-index walk, the shared-child probe and the lost[] recount, plus
    the graph.h layout comment) to a single shared constant, bumping
    `graph:layout` "1" to "2" since old 8-hex edge keys would otherwise half
    read as truncated 16-hex ones, and extending graphtest.py with a case
    that writes an edge id past 0xFFFFFFFF (via LINK with a forced high
    edge, or by driving the sequence there) and reads it back through LS,
    STAT, BFS and RM.

388. [Done] GRAPH LS of an empty directory, correctly empty [20-09-2026] Nr 364 afd983b

    MKDIR /x then MKDIR /wide read as a listing bug: `GRAPH LS /x` and
    `GRAPH LS /wide` each came back `[]` while `GRAPH LS /` named both. It
    is the correct answer - LS names a node's children, and two freshly
    made directories hold nothing (probed 20-09-2026: both edge keys in
    the store, `/` listing both, RESP RANGE over the parent-0 prefix
    returning both, and PUT /x/f.txt listing `[leaf 5 3 1 f.txt]`
    immediately after). No fault in `children_of`, the prefix range, or
    the page cap. Pinned in graphtest.py: two MKDIRs at the root, LS of /
    naming both, LS of each naming nothing.

389. [Done] NumKong matrix multiplication in stored Luau [21-09-2026] Nr 367 b5dee39

    `nk.matrix` userdata per dtype (f64/f32/f16/bf16) in nk_luau.cpp:
    nested-table or rows x cols construction, rows/cols/get/set, and
    `matmul` through unpacked `dots_unpacked` (C = A x B-transpose, the
    kernel's contract). Pinned in nkluautest.py with a hand-verified
    2x3 x 2x3 case.

390. [Done] SIMD packed GEMM behind nk.matrix matmul [21-09-2026] Nr 368 b5dee39

    `numkong_dispatch` static library (c/numkong.c + c/dispatch_*.c,
    dispatch 1, -march=native) linked into barch/lbarch/barchd/barchlua;
    `matrix_matmul` packs B once per call with serial fallback. 128x64 x
    256x64 went 0.8 to 0.5 ms/rep, same answers.

391. [Done] Error statistics for external monitoring [21-09-2026] Nr 369 b5dee39

    Five counters in statistics.h, all RESETSTAT-reset: function_timeouts
    + function_errors (pump_call finish), repl::refused_connections (both
    server.cpp refusal sites), repl::accept_errors, repl::net_errors.
    request_errors now bumps on every failed call reply (was repl-only).
    STATS + INFO MEMORY extended, new INFO ERRORS section carries the
    alert set. Pinned in errstatstest.py (TestErrStats).

392. [Done] SETF ... AOT: native code per stored function [21-09-2026] Nr 370 b5dee39

    Flag in either trailing position; AOT set in function_api.cpp per
    space-qualified key, read on cold compile, compile() on call().
    Native miss falls back to interpreter. ~12% on a float loop, same
    answers. Pinned in aottest.py (TestAot).

393. Silent death after SETF ... AOT + CALLF on a transport() function [21-09-2026]

    Probed 21-09-2026 on Release b5dee39 (0.5.8, dirty): a `vectors`
    space holding a 7,343-point HNSW graph (MiniLM-384, ~84 MB across
    25 arena snapshots in /tmp/opencode/vec-real/data, kept) plus a
    scratch `vecaot` space. `SETF VGRAPH ... AOT` then
    `SETF VECTORS ... AOT RELOAD` both answered OK; the first
    `CALLF VECTORS` (the call() usage listing) answered, the following
    `PARAMS` saw `Connection reset by peer`, and the server was gone:
    no crash/fatal/assert line in barchd.log, no snapshot write (data
    files untouched), no core. Same after restart: AOT reinstall OK,
    `CALLF VECTORS` OK once, dead on the next command.

    NOT reproduced 21-09-2026 on the installed release binary: the exact
    SETF/CALLF/PARAMS sequence plus SET/CLOSEST/TUNE/error paths, REMF
    cycles, fresh-connection PARAMS, double CALLF, pre-loaded 300-point
    graph flipped to AOT, and the literal bench setup() order all run
    clean against a copy of the kept data dir (probes /tmp/crashprobe*.py,
    14781-14796). Park+watchdog races (slow fills, tight deadlines,
    repeated and concurrent parks) also clean (14797-14849). Cause found
    22-09-2026 without reproducing the vectors sequence itself, see 394:
    the per-resume watchdog thread. Repinned as test/aotparkracetest.py
    (TestAotParkRace), which parks a hookless native frame on a slow
    foreign fill with the deadline inside the fill.

394. [Done] The AOT watchdog is a thread per resume [22-09-2026] Nr 371 b5dee39

395. [Done] nkf32vector took a binary buffer for a length [22-09-2026] Nr 372 b5dee39

396. [Done] Native code for transport methods and required modules [22-09-2026] Nr 373 b5dee39

397. [Done] The instruction budget raised where it could not yield [22-09-2026] Nr 374 b5dee39

398. [Done] The deadline watch asks for a slice as well as a deadline [22-09-2026] Nr 375 b5dee39

399. [Done] Nothing reported how much of the arena is reusable [22-09-2026] Nr 376 b5dee39

400. [Done] Every Luau state registers JIT unwind info with libgcc [22-09-2026] Nr 377 fb336e0

401. The AOT deadline watch writes a hook another thread is writing

    Found while checking 400 under TSan, and separate from it: TestAot and
    TestAotParkRace both report a write-write race on an 8 byte word, the
    watch thread at `deadline_watch::run` luau_driver.cpp:832 setting
    `lua_callbacks(w->L)->interrupt = function_interrupt` against
    `pump_call` luau_driver.cpp:4304 setting `cbs->interrupt = nullptr` on
    the same state as it arms a hookless resume. Both from 394/DONE 371,
    and DONE 371 describes the window itself - "a late firing writes the
    same hook the resume just restored".

    Not seen in CI because neither test carries the `short` label, so the
    TSan job has never run either of them.

    Luau sanctions the cross-thread write outright: lua.h:597 says
    "interrupt is safe to set from an arbitrary thread but all other
    callbacks are only safe to set from the main thread". So the VM's own
    contract says this is fine and TSan has no way to know it, which makes
    it a suppression rather than a fix - done in ci/tsan.supp, named on
    `deadline_watch::run` since the hook write is the only cross-thread
    access in it.

    What is not settled is whether `short` should pick these two up. They
    are the only coverage of the hookless path and they are the newest
    code in the tree, which is an argument for labelling them; against it,
    the pair costs about 9 s under TSan. Settled by deciding that, not by
    more probing.

402. [Done] A foreign flight's state read under one lock and written under another [22-09-2026] Nr 378 fb336e0

403. [Done] finish_fetch wakes a blocked session without the shard's latch [22-09-2026] Nr 379 fb336e0

404. [Done] NumKong's AMX kernels ask GCC 11 for an ISA it does not have [22-09-2026] Nr 380 fb336e0

405. NumKong is the last dependency still on a moving branch

    404 broke a green CI run with no change on our side: NumKong is
    declared `GIT_TAG main`, upstream grew AMX kernels, and the 22.04 job
    stopped compiling. The bf16 workaround a few lines above it arrived
    the same way.

    This is the argument TODO 336 already made for simdjson, written into
    CMakeLists right below the numkong block: "Every other dependency here
    is pinned and this one was not", with three consequences - build
    directories on different commits, a reconfigure that can fail outright,
    and a CI run that is not reproducible from the tree. All three apply
    here now.

    Not done as part of 404 because it is a choice rather than a fix, and
    because the same comment records the trap: `GIT_SHALLOW TRUE` cannot
    reliably fetch an arbitrary SHA, only a branch or tag tip, so pinning
    a commit means dropping GIT_SHALLOW or using a tag. NumKong's current
    checkout is in the build directories if a known-good commit is wanted.

    Settled by picking a commit or tag and deciding about GIT_SHALLOW, or
    by deciding that tracking `main` is worth the breakage and leaving it.

    cofetch was the other one and is no longer fetched at all - DONE 381
    vendored it to external/include, for this reason among others.

406. [Done] Concurrent http.request to one host serialises, via CURLOPT_PIPEWAIT [22-09-2026] Nr 381 fb336e0

407. [Done] Everything the GRAPH review found, fixed [22-09-2026] Nr 382 00e4d98

408. [Done] getJson and setJson on the store and on space handles [22-09-2026] Nr 383 00e4d98

409. [Done] parseJson for a subset of a stored object [22-09-2026] Nr 384 00e4d98

410. [Done] The reference page catches up with 382-384 [22-09-2026] Nr 385 00e4d98

411. GRAPH ids on a replica. GRAPH is registered write+data, so
    asio_resp_session.h forwards the whole command through `repl::call`.
    `LINK <node-id> <path>` and the `EDGE <id>` option on STAT, GET, UNLINK,
    RM and MV name things by id, and ids come out of blocks `reserve_ids`
    (ids.cpp) caches in process memory. A restart abandons the rest of a
    block, so after either side restarts the primary and the replica can hand
    out different ids for the same commands, and a replayed `LINK 42 /x`
    links whatever the replica's node 42 is. Settle by reproducing with two
    barchd instances: write nodes, restart the primary, write more, LINK and
    UNLINK EDGE by id, compare both graphs. If it reproduces, propose a fix
    and talk it through before changing how replication works.

    Reproduced 22-09-2026 with two release barchd instances, the primary
    PUBLISHing to the replica. Before the restart both sides agree (/c is 5
    on both). After it, the primary's next MKDIR /d is 65 and the replica's
    is 9, because the primary starts a new block at the stored counter while
    the replica keeps going through its old one. Then:

      - `LINK 65 /b/d_again` links /d on the primary and /n26 on the replica.
        Wrong node, no error anywhere. With fewer nodes after the restart
        the replica just has no 65 and the LINK fails there instead.
      - With /c2/x linked 56 times (first to /b, the rest to /a), `UNLINK
        /c2/x EDGE 152` drops the /b edge on the primary and one of the /a
        edges on the replica. Both still have 56 edges, so a count doesn't
        show it. An EDGE id the replica doesn't have fails there instead.
      - Neither shows up in the replica's log. `distribute()` in
        rpc/server.cpp only acts on net_error, so a replayed command that
        fails, or does the wrong thing, goes unnoticed.

    Found along the way: GRAPH is one command registered write+data, so
    LS, STAT, GET, BFS and DFS get replicated too. And the dispatcher calls
    `repl::call` before the command runs, not under the graph write lock,
    so two connections writing at once can queue in a different order from
    the one they committed in.

    Every write a commit makes goes through the one `staged ops` in
    `batch::commit` (graph.cpp), and the ids a commit takes are
    `first`..`first+wanted` (plus `first_inode` for leaves) in plan order.
    So there are two fixes that don't need GRAPH itself to change:
      A. replicate the effect: once `ops.commit` works, still holding the
         write lock, queue one record with the staged sets and removes plus
         the two counters as "at least". The replica applies it through one
         `staged` under its own graph write lock, and the dispatcher stops
         forwarding GRAPH.
      B. replicate the ids: forward the command from inside commit with
         `first`/`first_inode` attached, and have the replica take those
         instead of reserving.
    A is the recommendation, still to be agreed. The repro script is
    graphrepl.py in the session scratchpad and would become
    test/graphrepltest.py along with the fix.

412. [Done] barch.store.size(), and the float getters and setters [23-09-2026] Nr 387 f79c9f1

413. [Done] `--!native` as the lasting form of SETF … AOT [23-09-2026] Nr 386 a05774e

414. A string a stored function returns loses a leading `$`. A var_string
    that came in over RESP as a bulk string is held as `$` + the value, the
    marker being how a bulk string is told from a simple one, and every
    reader strips one leading `$`: Variable::to_string, bulk_vt (fs.cpp
    fetch, keys.cpp's valkey reply, luau_driver.cpp's push back into Luau)
    and the wire writer. `to_variable` in luau_driver.cpp stores a Luau
    string (and a buffer) raw, without the marker, so a value that really
    starts with `$` is taken for a marked one and loses its first byte.
    Found 23-09-2026 through barchex's S3 file source: a bucket object
    `$100 price` (10 bytes) was stored as a 9 byte file, through both the
    body-only and the {body, type} return of an fs_source, and
    `return "$x"` from CALLF answers `x`. A name starting with `$` from an
    fs_source_list comes out without it too. A plain SET/GET is fine.
    Settle by having to_variable mark what it makes from LUA_TSTRING and
    LUA_TBUFFER as bulk (`$` + bytes), so the readers' strip takes off the
    marker and not the data, after checking that no reader of a script's
    result uses the string raw (anything that did would now see the `$`).
    {ok = "..."} is a simple string and stays unmarked. functiontest.py
    should return "$x", "$", "$$" and "" through CALLF and get them back
    unchanged, and an fs_source whose body starts with `$` should store
    every byte, for both return shapes.

415. [Done] DICTIONARY GET and SET [23-09-2026] Nr 388 f79c9f1

416. [Done] Page walks over BEGIN-time pages, with free lists and stats, for backups [23-09-2026] Nr 389 f79c9f1

417. [Done] ROLLBACK puts back the allocator, not only the tree [23-09-2026] Nr 390 f79c9f1

418. [Done] Streaming save and load of a whole space [23-09-2026] Nr 391 f79c9f1

419. Streaming load of a range-sharded space. stream_load_space and
    StreamLoad refuse a space that is_stateful_sharding(), because a shard
    at a time isn't safe there: the range sweep moves keys between shards,
    which is why LOAD (repl_api.cpp) holds the whole space's write lock and
    rebuilds the routes before it lets go. Settle by collecting every
    shard's blocks first, then loading them all under
    `lock_space_write()` and rebuilding the routes, the way LOAD does. Then
    a round trip on a range-sharded space, saved while the sweep is
    running.

420. [Done] The lua test harness hung when valkey was slow to start [23-09-2026] Nr 392 f8fbff4

421. [Done] The range sweep stuck at a tie [24-09-2026] Nr 393 f8fbff4

422. Secondary indexes built on demand: numbered covers of a record's
    fields, so a query has an exact index to use without knowing in
    advance which one. Decided 24-09-2026: indexes are eventually
    consistent (see 423 for a synchronous flag), a query uses them only
    when a flag asks it to, and a space's indexes live in `<space>_ix`.

    Idea. For equality lookups on up to 8 fields, every subset of the
    fields has to be a prefix of some ordering of them. The fewest
    orderings that do that is C(n, n/2), from a symmetric chain
    decomposition (Greene-Kleitman): 6 for 4 fields, 20 for 6, 70 for 8,
    against 40,320 permutations. Each ordering is a chain, numbered in one
    byte. A 256-entry table maps an equality mask to its chain and the
    order of the fields in it. A range or sort field after the equality
    set needs more chains, added per query shape. Sliding windows (n-grams)
    are another strategy under the same path format.

    Pieces:
      - Registry: `<space>.index.<name>` in the configuration space, a JSON
        definition giving the source (composite, json or function), up to 8
        fields, the pinned tail count, the strategy (chains or window) and
        the state of each materialized chain (building or ready). Commands
        INDEX CREATE | DROP | LIST | STATUS | BUILD <chain>, and
        barch.index(name) in Luau.
      - Extractors: (key, value) -> up to 8 comparable_keys plus a tail.
        composite: the key's components after key_split, with the last
        `pinned` ones (the doc id in n-gram keys) kept last and never
        permuted. json: paths read from the value with simdjson, the source
        key as the tail. function: a stored Luau function that returns the
        fields.
      - Path: [strategy][chain id][fields in chain order][pinned tail or
        source key], written into <space>_ix. Fields are encoded once per
        record, and building a path is copying their bytes. A composite path
        holds every component, so the source key can be rebuilt from it
        without a lookup.
      - Upkeep, eventually consistent: shard::insert and shard::remove, the
        change log's two call sites, append {op, key, old value, new value,
        sequence} to a per-space index queue while under the shard latch,
        for spaces that have indexes. The maintenance thread drains it with
        no source latch held and writes the path differences through staged,
        for chains that are ready or building. Writing the index from inside
        the hook would take another space's latch while holding this one,
        the inversion ids.h warns about.
      - Build on existing data: mark the chain building, so the hook starts
        queueing changes for it, then walk the source in bounded chunks on
        the maintenance thread and write paths. Queued changes for a key the
        walk hasn't passed yet wait until it has, so an old value the walk
        read can't land after the change that replaced it. Ready when the
        walk ends. The alternative is a BEGIN snapshot plus the queue, which
        holds a whole-space transaction open for the length of the build.
      - Check on read: a hit through a chain rechecks the source record's
        fields and deletes the path when it no longer matches. That covers
        races, TTL expiry and anything the hook missed.
      - Queries, only when the flag asks for them: composite KEYS, SCAN and
        COUNT with wildcards in the leading components (the fixed ones
        become an equality mask, then one prefix range, then the source
        keys rebuilt), and a new FIND <index> field=value ... [LIMIT n]
        [WITHVALUES], with :find in Luau. With no ready chain, use the one
        whose prefix covers the most equality fields and filter the rest, or
        scan. Count the demand per mask, and past a threshold schedule BUILD
        for its chain: that's the on-demand part.

    Phase 1 done 24-09-2026, DONE 395: INDEX CREATE, LIST, CHAINS, BUILD,
    FIND and DROP over composite keys with a pinned tail.
    Phase 2 done 24-09-2026, DONE 401: writes and erases are queued by the
    shards and applied by the maintenance thread (and by FIND first), BUILD
    follows writes during its walk, and FIND checks its answers against
    the source. Bulk loads still bypass the hook. Phases 3 and 4 are still
    to come.

    Phases: (1) registry, composite extractor with a pinned tail, path
    builder, INDEX CREATE/LIST/BUILD with an explicit backfill, tried on
    n-grams. (2) The hook and queue, drained on the maintenance thread, and
    check on read. (3) The planner in composite KEYS/SCAN/COUNT behind the
    flag, and FIND. (4) json and function extractors, demand counting,
    automatic BUILD.

    Uncertain: the write cost per record at 6 and 8 fields in practice
    (paths share prefixes in the tree), what an index queue costs when
    writes are heavy, and the flag's name and form. Settle each phase with a
    test. The first measurement: bytes per record and inserts per second
    for n-grams, with 0, 1 and all chains built.

423. A flag to make an index synchronous. 422 keeps indexes eventually
    consistent, for reliable write performance. Some uses need a write and
    its index paths to land together. Settle by a per-index flag that
    writes the paths in the same call as the source write: after the
    source shard's latch is released, or holding both, source space
    before index space in the canonical order keyspace_locks.h sets. Then
    measure the write cost against the queued path, and test that a
    lookup through the index right after the write finds it with no wait.

424. [Done] The streaming save leaves BEGIN and COMMIT to the caller [24-09-2026] Nr 394 7d6c29c

425. Copy-on-write memory during BEGIN ... COMMIT. begin() maps a CoW
    region as large as the whole arena, and update_usage_stats counts all
    of it, although only pages that get written take physical memory. So
    a transaction on a big space looks like it doubles the space's memory
    as soon as it opens, which can trip max_memory checks and eviction.
    The first write to a page copies the whole 512K page, even for a few
    bytes, and commit copies whole pages back. A long BEGIN for a backup
    pays both for as long as it runs. Options: count only the pages
    actually copied; track and copy at 4K granularity inside a page (the
    modified flags are per page now, TODO 416); give pages back with
    MADV_DONTNEED at commit/rollback instead of munmap plus remap; or keep
    only the touched pages rather than a mirror of the arena. Settle by
    measuring RSS and the reported memory during a BEGIN on a large space
    with a steady write load, before and after, with pagewalktest and
    streambackuptest still passing.

426. A range from "an existing key plus a 0x00 byte" finds nothing.
    perm_index.cpp paged through a space by starting each page at the
    last key of the one before with a zero appended. That is the key's
    immediate successor, since stored keys are prefix free and end in 0.
    sharded_store::range answered nothing from there, so every index lost
    all but its first 1,024 records until the paging restarted inclusive
    and dropped the repeat. Something in the tree's lower bound (or
    make_merged / art::iterator) seems to mishandle a bound that extends a
    stored key. Settle by reproducing it directly: range(K + "\0", hi) on
    a space holding K and later keys, on both hash and ordered shards. Fix
    it if it's the tree, and check which other callers build successors
    that way (fs.cpp's past(), key_range.cpp).

427. [Done] Space handles kept past their call pointed into a freed interface [24-09-2026] Nr 396 65f4666

428. [Done] An oversized RESP argument gets a protocol error, not a hang [24-09-2026] Nr 397 65f4666

429. [Done] The function deadline does stop a save; the setting was the question [24-09-2026] Nr 398 65f4666

430. [Done] The Luau interfaces keep far fewer pointers stable by hand [24-09-2026] Nr 399 65f4666

431. [Done] A walk over barch.art() read freed memory once its handle was collected [24-09-2026] Nr 400 65f4666

432. GET and EXISTS answer for a key that has expired but hasn't been
    swept yet. `SET t v PX 50`, then after 300ms GET t still answers "v"
    and EXISTS t answers 1, while KEYS t leaves it out.
    sharded_store::exists goes through shard::search, which doesn't look
    at expiry. search_state does (it treats `expired()` as absent), and
    GET seems to take a similar route. Found while making INDEX FIND check
    its answers against the source (TODO 422 phase 2, which uses
    search_state for that reason). Settle by a test of GET, EXISTS, TTL
    and the store_access reads a script uses on a key just past its PX,
    then honouring expiry in the shared read paths so it doesn't depend on
    the sweep having run.

433. [Done] rangebalancetest wasn't built on CI [24-09-2026] Nr 402 7718822

434. [Done] A function's own deadline and slice, and deadlines that count running time [24-09-2026] Nr 403 2795c83

435. [Done] A blocking http/resp/mail wait comes off the deadline and stops at the wall ceiling [24-09-2026] Nr 404 2795c83

436. [Done] The queue consumer no longer holds a thread while a handler runs [24-09-2026] Nr 405 2795c83

437. [Done] Queue, cron and fs source calls keep their Luau states between calls [24-09-2026] Nr 406 2795c83

438. [Done] Process exit could hang with a maintenance thread in pindex::tick [24-09-2026] Nr 407 2795c83

439. [Done] A saved space counts as existing, so it's opened instead of refused [24-09-2026] Nr 408 c75e8e7

440. [Done] net_errors no longer counts a clean disconnect [24-09-2026] Nr 409 c75e8e7

441. [Done] barchd stops when it can't listen, and START says so [24-09-2026] Nr 410 c75e8e7

442. [Done] --bind is the address RESP listens on [24-09-2026] Nr 411 c75e8e7

443. [Done] TestFunctionLimits' crowd check no longer depends on the machine's speed [24-09-2026] Nr 412 c75e8e7

444. [Done] TestFunctionLimits' crowd and TestStreamBackup's SLOWSAVE, sized for slow CI runners [24-09-2026] Nr 413 640e0b9

445. [Done] The function deadline counts the call's CPU time [24-09-2026] Nr 414 640e0b9
