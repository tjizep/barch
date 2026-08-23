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

98. User-defined Luau RESP functions.

    Register a RESP command whose body is a Luau script, so a client
    can call `MYTHING a b` and the script sees those arguments and
    answers through the same reply path as a built-in. This is not
    `foreign=luau`, which only fills a miss. It is a command.

    A function is a stored value, not a configuration string. It
    lives under `art::tfunction`, so it persists, replicates and
    exports the way every other key does, and a space's functions
    have the same lifetime as the space. Functions in the
    `configuration` space are global: defined once, callable from any
    client in any space, exactly like a built-in, which is the same
    scope the barch module itself has. Functions in one space may
    include each other. A cycle is an error, never a silent drop.

    A. Where a function lives: an ordinary key with a lead of its
       own.

    A function key is a composite key like any other multi-part key,
    with `art::tfunction` as its lead where a caller's key has
    `art::tplain`: `composite{ts_function, "KS1", "PRINT_NAME"}`.
    A composite component is `{lead, 0x00}` followed by the parts -
    which is why every decoder starts reading at [2] - so this needs
    a `ts_function` beside `ts_plain` in nodes.h and nothing else
    new in the encoding.

    `is_composite_lead` then has to say yes to 12, and explicitly
    rather than by widening the range it already tests, because
    `is_container_lead` shares that range and a function is not a
    container: no kind is claimed for it and
    `claim_container_kind` must never see one.

    That turned out to be the whole audit. Every site that reasons
    about a lead byte asks the predicate: keys.cpp:133, :202 and
    :382, `script_key` in luau_driver.cpp, the three in
    foreign/sql.cpp, and - the one that looked like separate work -
    conversion.h:244, where `is_composite_lead` is already one of
    the disjuncts in `comparable_key`'s accept list. So the throw
    and the abort both go away with the predicate, and the comment
    at `is_composite_lead` that says "one predicate, so the next
    type is one edit" turned out to be telling the truth.

    `is_container_lead` is the one that must not change. It tests
    the same span, and a function is not a container - nothing
    claims a kind for one - so tfunction is named explicitly in
    `is_composite_lead` rather than folded into the range.

    The AOF and RDB writers, the exporter and replication all treat
    it as an ordinary key, which is the whole point of doing it this
    way: a SETF persists and reaches a replica with no machinery of
    its own.

    First class also means the ordinary commands answer for these
    keys rather than hiding them. All of this is now observed
    rather than predicted - see test/functiontest.py:

      - KEYS, SCAN, RANGE and the bounds see them. DBSIZE counts
        them, so a SETF changes it.
      - TYPE cannot say `function`, because barch has no TYPE
        command - a grep finds no handler and no table entry, and
        containers have no answer there either. Adding one is its
        own job, redis-shaped, and it would have to answer for
        lists, hashes and ordered sets before it answers for
        functions. Nothing here depends on it.
      - GET, SET and DEL cannot address them at all - see below,
        it falls out of the encoding rather than being a rule.
        SETF, GETF, REMF and KEYSF are the commands that can; see
        K for why they are their own commands and not a flag.
      - FLUSHDB drops a space's functions with the space. FLUSHALL
        reaching `configuration` would drop the global ones, which
        is either right or wants refusing.
      - EXPIRE, and the whole TTL family, is refused on the range.
        The original reason was the generation counter, which is
        gone - but the refusal stands on its own now: a session
        holds what it compiled for as long as it is connected, so
        a TTL would expire the key while every connected client
        went on calling the function. A setting that visibly does
        not do what it says is worse than not having it.

    Writing one. The precedent is a container key: hash_api.cpp:60
    builds `query.create(art::ts_hash, {container})` and inserts
    through the store. A function is the same move with a different
    lead:

        composite q;
        auto key = q.create(art::ts_function, {conversion::convert(name)});
        store.insert(opts, key, source, ...);

    with `fits_in_leaf(key.size, source.size)` checked first the way
    HSET does. The ceiling is `maximum_allocation_size`, a little
    under 256KB, which no sane script reaches - but that check is
    where `too_large_message` comes from and it belongs there.

    What makes the range safe is that nothing else can reach it. A
    client's key goes through `key_space::encode_key` and
    `conversion::as_composite`, which produce tstring, tinteger,
    tplain and the rest, never tfunction. So SET cannot address a
    function key and there is no guard to write for it: the refusal
    is the encoding itself.

    The same fact costs something, and the bullets above had it
    wrong first time round: GET and DEL cannot reach the range
    either, for exactly that reason. They do not read or delete a
    function. `GETF` and `REMF` are the surface, and they build the
    composite themselves - see K. KEYS and SCAN still
    show function keys, since the reply path decodes any composite,
    so the asymmetry to live with is that a name KEYS printed cannot
    be handed back to GET. That wants saying in the docs rather than
    being discovered.

    Where a guard *is* needed is the paths that write an already
    encoded key: replication, the AOF and RDB load, and
    import/restore. A forged tfunction key from a peer or a crafted
    dump is a function body nobody loaded, so those validate the
    lead instead of trusting it.

    Two more consequences of writing a new lead into a shard file:

      - `storage_version` in constants.h is bumped, 15 to 16, but
        with SETF rather than with the key type. The bump protects
        an old binary from a new *file*, and no file can hold a
        function key until something can write one - so it belongs
        with the first write path, not with making the type
        representable. Bumping it earlier only invalidates shard
        files that contain nothing new.
      - the value is the source text, not bytecode. Bytecode is
        tied to the Luau build, so storing it turns a Luau upgrade
        into a migration, and the compiled instance already lives in
        the session per C.

    The key is `{ts_function, name}`. The space is implied by the
    space the command runs in - `space:SETF` or `USE space` then
    `SETF` - so it is not in the key, and there is no way for a key
    in KS2 to claim it belongs to KS1.

    A stored function answers through a global `call`, where a
    foreign fill answers through `resolve`. That is the first place
    the two Luau contracts part company, and it cost a bug already:
    `compile_source` in luau_driver.cpp had "resolve" written into
    it, so pointing SETF at it would have refused every function
    ever written. The entry point is a parameter now -
    `compile_entry(source, bytecode, entry, err)` - and the two
    callers pass their own.

    SETF is SET with a different type byte: same name in, same
    value, `ts_function` where `encode_key` would have picked
    tstring or tinteger. At the byte level that is a one component
    composite, `{0x0c, 0x00}` then the name, rather than a literal
    swap of the lead on a tstring key. The two sort the same and
    both render as the bare name through `reply_encoded_key`, so no
    client can tell them apart - but the composite is already
    handled by every decoder through `is_composite_lead`, where a
    literal swap would need the tstring branch taught about
    tfunction in keys.cpp three times over, in conversion.h, in
    `script_key` and in sql.cpp. Same behaviour, none of the
    work.

    The ordering is the interesting part, and it cuts both ways.
    tfunction is 12, after every other lead, so function keys sort
    together at the end of an ordered space. In favour: FUNCTION
    LIST is a range scan over one contiguous span rather than an
    index that has to be kept in step, dropping every function in a
    space is one range delete, and a session rebuilding its cache
    reads them in a single walk. Against: MAX and `maximum()` now
    answer with a function key, `KEYS *` includes them, RANDOMKEY
    can hand one back, and on a range-sharded space every function
    lands on the last shard. None of that is wrong, but each is a
    change to a command that has nothing to do with functions, and
    each wants a test saying which way it went.

    Measured once SETF existed, on a space holding one string and
    one function: DBSIZE goes 1 to 2, `KEYS *` and `SCAN 0` both
    name the function, and `MAX` answers with it rather than with
    the string. `GET` on the same name answers null, which is the
    encoding keeping the two ranges apart. So the prediction held
    in both directions, including the awkward half.

    B. Naming, scope and resolution.

    A function is addressed `SPACE.NAME`, so `KS1.PRINT_NAME` is
    PRINT_NAME as defined in KS1. An unqualified `PRINT_NAME`
    resolves in the selected space first, then in `configuration`.

    That sits next to the `space:CMD` prefix `run_params` already
    parses, and the two are orthogonal rather than redundant: the
    colon says which space the call *runs against* - what
    `call.kspace()` returns - and the dot says where the definition
    is *loaded from*. So `KS1:KS1.PRINT_NAME` is valid and says
    both, and `KS2:KS1.PRINT_NAME` runs KS1's function against KS2,
    which is worth having if the function is written against an
    interface rather than against particular keys.

    Parsing goes in the same place and in this order: split the
    colon prefix, split at the first dot, then upper-case what is
    left. It has to be that way round because a space name is case
    sensitive and a command name is not, which is already why
    `run_params` splits the colon before folding. It also assumes a
    space name contains no dot - configuration keys are
    `<name>.foreign`, so that is assumed elsewhere too, but it is
    worth saying out loud rather than discovering.

    Built-ins are never overloaded. A SETF that takes the name of a
    built-in command is refused there and then, so dispatch
    looks the built-in table up first and never has to arbitrate
    between the two. A dotted name cannot collide by construction.

    C. Where the compiled function lives: one state per session and
       space.

    Threading is solved by not sharing. A RESP session keeps a
    `lua_State` per key space it has called into, and a string map
    of the functions compiled in it. The first call to
    `KS1.PRINT_NAME` on a session reads the tfunction key out of
    KS1, compiles it into that session's state for KS1 and caches
    it under that name; every later call on that session goes
    straight to it. Nothing is shared between sessions, so there is
    no mutex, no pool of states, and no state whose ownership moves
    between threads.

    The invariant this rests on is that one session never runs two
    calls at once, and today it does not: `run_asynch_batch` posts
    one call at a time and does not read again until the batch is
    done, and a parked call resumes through the same chain. That is
    worth writing down beside the cache, because the day something
    runs two calls of one session in parallel this stops being safe
    without looking any different.

    The space keeps the definitions; the session keeps one compiled
    instance of them, and once compiled it keeps that one. A
    session does not notice a redefinition: new code reaches new
    sessions. Reconnecting is how a client picks up a change, and
    that is the documented behaviour rather than a gap. Live
    reload and versioning are 137.

    That is worth what it saves. There is no generation counter, no
    atomic on the call path, nothing that has to reach into a live
    session from a write, and no rule about what happens to a call
    already running when its definition changes underneath it.

    Note what it does *not* cost: a function is compiled on first
    use, so SETF followed by a call on the same connection works -
    that session had nothing cached for the name and reads the key
    fresh. Only a redefinition of something the session has already
    run is stale.

    Teardown is the leftover. A session must key its states on the
    space name and never hold a `key_space_ptr`, or an UNLOADed
    space stays alive in every session that ever called into it.
    Size is the other one. A client that calls a hundred functions
    has a hundred of them compiled, a thousand connections have a
    thousand copies of the popular ones, and a session that roams
    over spaces holds a state for each. That wants a cap with LRU
    eviction - on the states as well as on the functions in them -
    and a line in the memory statistics, the same as the scan
    cursors in caller.h already have. Eviction is not invalidation:
    an evicted function is compiled again from whatever the key
    says now, so a long-lived session can pick up a redefinition by
    accident. Either that is fine and it is written down, or the
    cache keeps the source it compiled from.

    Per session and space, rather than per function or one for the
    whole session, because of what `require` costs and how memory
    comes back. The three were:

      - one per function. Hard isolation, and closing the state
        frees everything exactly, which is what makes an LRU cap
        and a per-function memory ceiling actually mean something.
        But it defeats includes: two functions in different states
        cannot share a module, so every function carries its own
        copy of everything it requires, plus its own base
        libraries and string table.
      - one per session, holding every function that session has
        called. Cheapest, one module cache, and `require` is free
        after the first use. Isolation is `luaL_sandbox` plus a
        per-call thread rather than a separate heap, which is
        enough here - the functions all belong to the same client
        and the same ACL. Reclaiming memory for one evicted
        function is vaguer: it needs a collection, not a close.
      - one per session and space. `require` resolves within a
        space (and `configuration`), so this puts the module cache
        exactly where the resolution already is. It also makes the
        two awkward cases in the list above trivial: a generation
        bump or an UNLOAD closes that space's state and nothing
        has to be picked out of a map. The duplication it costs is
        the `configuration` globals, once per space a session
        touches.

    The third is what this entry now assumes, with the second as
    the fallback if per-space states turn out to cost more than the
    bookkeeping they remove. Either way the *cache* stays keyed by
    function name; the choice is only which heap the compiled thing
    sits in, which is why falling back later is cheap.

    Inside each of those states, globals are frozen once with
    `luaL_sandbox` and each call runs on a `luaL_sandboxthread` with
    its own writable global proxy, so one call cannot leave
    anything behind for the next. Includes are natural here: a
    required function compiles into the same session state and is
    cached next to its caller.

    The other contexts have no session to hang this on. SWIG, RPC
    and the valkey module either grow an equivalent cache or answer
    `-ERR FUNCTION not supported on this path`, which is the shape
    of foreign's context check and is where the first cut should
    start.

    D. Includes, and cycles.

    Include is `require("NAME")`, resolved against the same space
    then `configuration`, with the result cached per state. That
    gives back the global `open_safe` currently blocks outright,
    restricted to a resolver that can only see functions.

    Cycles are refused, at load, with the path in the message:
    `-ERR FUNCTION cycle A -> B -> A`. The check is a DFS over the
    declared edges, and it has to run on every LOAD and every
    DELETE, not only the first, because A can include B today and B
    can be redefined to include A tomorrow. Whether the edges can
    be read off the source statically is a question: `require(x)`
    with a computed name cannot be, so there is a runtime backstop
    too - a "loading" mark on each module, which turns a cycle the
    static pass could not see into an error instead of a stack
    overflow.

    Deleting a function that something else includes is refused,
    with the dependents named. `FORCE` can override it and leave
    them broken, which is at least visible.

    E. Calling other commands.

    `call("NAME", "p1", "p2")` with the same string-vector shape the
    caller classes use everywhere else. The implementation is a
    derivative of rpc_caller: `callv(params, f, def)` already runs a
    command and hands back a `Variable`, which is one conversion
    away from a Luau value.

    It must be a *sub*-caller, not the caller that is answering the
    client. `rpc_caller::call` clears `results`, `errors` and `args`
    on entry, so calling through the outer one destroys the reply
    being built. `finish_call_buffer` has the pattern already -
    `collecting_exec` plus a buffer - and a script's nested call
    wants the same treatment.

    Things that need an answer before this ships, and the cheap
    answer for the first cut is to refuse them:

      - a blocking command inside a script. BLPOP parks the caller
        through `add_block`; a script cannot park half way through
        a Lua stack and still be resumable.
      - an asynchronous command inside a script. KEYS is
        `is_asynch` and expects to own a worker.
      - a command that parks on a foreign fill - the same problem,
        arriving by a different route.
      - MULTI/EXEC from inside a script.

    ACL is not optional here: the script runs as the calling user
    and `call` checks that user's acl vector, or a function becomes
    a way to launder a command past the check that would have
    refused it. There is also a nesting depth limit, and the
    instruction budget and deadline are shared across the whole
    tree of nested calls rather than reset per call.

    `_G["NAME"]` is a convenience on top of `call`, and it brings
    its own questions, which is why `call` is the primitive and
    stays available: a command name can collide with a base global
    (barch has `KEYS`, which is also the name redis's own Lua API
    uses), `_G` cannot express the `space:CMD` form, and the set of
    names would have to be rebuilt whenever a function is loaded.
    Populate the per-call global proxy, skip any name that would
    shadow something already there, and say so in KEYSF.

    F. The store interface.

    `sharded_store` is the object to expose - it is already the
    per-call, cheap-to-construct front door to the shards, the hash
    and the art, and it already has the read and write scopes,
    the container scopes, the bounds, `range`, `glob` and `scan`.
    Plus a read-only view of the space's own configuration.

    "Safe" has one rule behind it: script code never runs while a
    shard lock is held. Two ways to keep that true, and they can
    coexist:

      - iteration hands back bounded batches. The cursor takes the
        lock, copies n entries, drops the lock, returns them. That
        is what `scan_cursor` already does.
      - where a callback under a lock is genuinely wanted, mark the
        region on the run context: no yield, no `call`, no
        `sql.query`, and a hard instruction cap inside it, with the
        interrupt raising an error rather than yielding. A yield
        under a shard lock is the deadlock the first draft of this
        entry was worried about, and this is what makes it
        impossible rather than merely discouraged.

    The goal being most of the built-ins re-implementable in Luau is
    a good test of the interface: pick three of different shapes -
    say GETRANGE, HRANDFIELD and ZRANGEBYSCORE - write them in Luau
    against this interface, and see what is missing.

    G. SQL, and later HTTP.

    `sql.query` exists and is scoped to the space's driver; a
    function gets it on the same terms. The asynchronous HTTP client
    is entry 99, and when it lands the same object shows up here.
    Both are calls that can take a while, which is the next section.

    H. The slice, and what holds the worker.

    `run_ctx`, `interrupt` and `pump` in luau_driver.cpp are the
    machinery and should be lifted into something both callers share
    rather than written twice. The budget is its own setting
    (`function_script_insns`), not `foreign_script_insns`, because a
    fill and a client-invoked command are not the same risk, plus a
    wall-clock deadline as foreign has.

    First cut registers the command with `is_asynch`, so it runs on
    the worker pool out of the asynchronous batch and never on a
    service thread. That still holds one worker for the whole
    script, bounded by the budget and the deadline, which is the
    deal KEYS already has. The proper answer is to park: pump on the
    foreign pool with requeue and wake the session when it finishes.
    A block with no key is never woken by the shard waiter registry -
    `add_block` keys on (session, key) and `call_unblock` fires from
    a write - so the completion holds the `abstract_session_ptr` and
    calls `do_block_continue` itself, resuming through
    `suspend_for_blocks` / `resume_after_blocks` so replies behind it
    in a pipeline do not overtake.

    I. Order to build it in.

      1. [done] tfunction as a key type: `ts_function` beside
         `ts_plain`, and `is_composite_lead` taught about the lead.
         That was the whole audit - see A.
      1b. [mostly done] SETF / GETF / REMF / KEYSF in
         function_api.cpp, the `function` ACL category, and what
         KEYS / MAX / DBSIZE do now the range exists, pinned in
         test/functiontest.py. SETF compiles before it writes, so a
         script that will not run is refused rather than stored.
         Left: the `storage_version` bump, and the lead check on
         the paths that write an already encoded key - replication,
         AOF and RDB load, import/restore.
      2. [part done] CALLF runs a stored function: arguments in as a
         1-based array of strings, the reply conversion in J below,
         a hard instruction cap and deadline, `is_asynch` so it
         runs on the worker pool. Covered in test/functiontest.py,
         including a spin being cut off with the connection still
         usable afterwards.

         Shortcuts taken, both deliberate and both wanting undoing
         before this is called finished:
           - the budget is `get_foreign_script_insns()`, a million
             instructions, rather than the `function_script_insns`
             of its own that H asks for, and the deadline is the
             space's `foreign_query_timeout_ms`. A fill and a
             client-invoked command are not the same risk.
           - no session cache yet: every CALLF compiles the source
             again. C is the design; this is correctness first, and
             the cache is a measurable change on its own rather
             than two hard things at once.

      2b. Calling one by name. Dispatch: the
         colon-then-dot parsing, the resolution order, and the two
         places in asio_resp_session.h that answer "unknown command"
         - the cached `ic` iterator in `run_params`, which skips the
         lookup entirely when the name repeats, so a name that
         missed once stays unknown for the life of the session, and
         the string re-lookup in `run_asynch_batch`. The per-session
         table snapshot at :614 stops mattering, since functions
         never enter that table. The negative result is the one to
         be careful with: a name tried before it existed must not
         stay unknown, or SETF followed by a call on the same
         connection fails for a reason that has nothing to do with
         the function.
      3. Arguments in, value out, arity, ACL, the reply conversion.
         No includes, no `call`, no store.
      4. The session cache: the state, the generation check,
         sandboxed globals, `require` and the cycle check.
      5. `call` into other commands, with blocking and asynchronous
         ones refused.
      6. The sharded_store interface, and the three re-implemented
         built-ins as its test.
      7. Parking, replacing `is_asynch`.

    J. Still open.

      - whether `KS2:KS1.PRINT_NAME` - one space's function run
        against another space - is allowed or refused. Refusing it
        means the dotted form only ever repeats what the colon
        already said.
      - what the session cache costs at a thousand connections, and
        where the cap goes - and whether an evicted-then-recompiled
        function picking up newer source is acceptable or has to be
        prevented.
      - one `argv` table or varargs.
      - whether globals in the `configuration` space may be
        redefined by a space-local function of the same name, or
        whether that is refused rather than shadowed.

    K. The commands that address the range: SETF, GETF, REMF, KEYSF.

    Functions get their own commands rather than a flag on the redis
    ones. `SETEX fname "fbody" FUNCTION` and `REM keyname FUNCTION`
    would work, but they change the arity and syntax of commands a
    redis client thinks it knows, and the differential tests exist
    to catch exactly that. Keeping the two sets apart means the
    redis clones stay bit-compatible and nothing about functions has
    to be argued for in terms of what redis does.

    There is a second reason that matters more than compatibility:
    rights. SET is `{"write","keys","data"}`, so a flag on SET would
    make "may write a string" and "may define a function" the same
    permission, and a function is code. Separate commands carry a
    separate category, which is the only way that right can be
    granted on its own.

    So a `function` category is appended to `categories()` in
    barch_apis.cpp - appended, never inserted, because
    `get_category_map()` numbers them by position and
    `is_authorized` compares by index. Stored ACLs are keyed by
    name (`user:cat:<user>:<cat>`) and re-vectorised at AUTH, so a
    name added at the end costs nothing and one added in the middle
    silently reassigns everyone's rights.

    The set, all of them building the composite themselves:

      - `SETF <name> <source>` - compiles, refuses a built-in's
        name, refuses a script that will not compile, writes the
        key, bumps the space's generation.
      - `GETF <name>` - the source back.
      - `REMF <name>` - with the dependent check from D, and FORCE
        to override it.
      - `KEYSF [pattern]` - the range walk from A. Cheap because
        the functions are contiguous.

    Cats are `{"write","data","function"}` for SETF and REMF, and
    `{"read","data","function"}` for GETF and KEYSF. `data` has to
    be in there or the write never replicates - `run_params` only
    calls `repl::call` when the command `is_write() && is_data()` -
    and `data` is granted to every user anyway
    (auth_api.cpp always emplaces it), so the category that
    actually gates these is `function`.

    That also settles what to do about redis's own FUNCTION command:
    nothing. The name stays free, so a real redis-compatible
    FUNCTION - libraries, shebang, `register_function` - can be
    written later without having to unpick a barch-shaped one
    squatting on it.

    Reply conversion, settled and asserted in test/functiontest.py
    over both protocols: nil is null; a string is a bulk string; an integral
    number is an integer and anything else is a double on RESP3 and
    a bulk string on RESP2; a boolean is 1 or null, as in redis; an
    array table is an array converted the same way per element and
    nests up to eight deep; `{err = "..."}` is that error verbatim
    and `{ok = "..."}` that simple string; anything else is an
    error saying so.

    The RESP2 double is the one that surprises: `return 2.5` comes
    back as the bulk string "2.5" on a RESP2 connection and as a
    real double on RESP3, which is the rule working rather than a
    fault. Both halves are asserted, because a client that has to
    guess which it is getting cannot be written against.

    Tests follow test/foreign_luau.py, and the first of them are
    about the key range rather than about Luau: a function key
    through KEYS, SCAN and TYPE, MAX and RANDOMKEY on a space that
    has one, SET and EXPIRE into the range refused, SETF then GETF
    round-tripping, and the range surviving an export and a reload. Then: load and call, arity error,
    redefinition while a call is in flight, the instruction budget,
    the deadline, delete, the name going unknown again after delete,
    a call refused by ACL, a cycle refused at load, a global in
    `configuration` called from another space, and a function
    surviving a restart and reaching a replica.

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

    Still to answer:

      - `~pattern` filters, now refused rather than silently
        dropped - see 136. Implementing key-level rights for real
        is a separate job, and per-space rights should land first.
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

137. Live reload and versioning for Luau functions.

    98 settles that a session compiles a function once and keeps
    it, so a redefinition reaches new sessions and no others. That
    is deliberate - it removes the generation counter, the atomic
    on the call path, and every question about a call that is
    already running when its definition changes. Reconnecting is
    how a client picks up new code.

    What it costs is worth writing down, because it is the reason
    this entry exists:

      - a long-lived connection pool never picks up a fix. That is
        most production clients, so the practical answer to "I
        deployed a new function" is "restart your clients", which
        is not much of an answer.
      - two connections can be running different versions of the
        same function at the same time, indefinitely, with nothing
        that reports it.
      - eviction from the session cache recompiles from whatever
        the key holds now, so a version can change by accident
        under memory pressure while a redefinition on purpose does
        nothing.

    The shapes worth weighing when this is picked up: a generation
    counter per space, checked on the call path and closing the
    session's state for that space when it moves; per-key
    versioning, which is finer and means picking entries out of a
    state rather than dropping it; or an explicit command that
    tells a session to drop what it has cached, which puts the
    choice with the client rather than guessing.

    Not urgent until functions are actually being used, and the
    right time to decide is when there is a real script being
    edited against a real client, since which of the three is
    tolerable depends on how often that happens.

