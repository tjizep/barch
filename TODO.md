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

239. `hash_arena` should be able to back its pages with a named memory mapped
    file, not only anonymous memory. It already mmaps when `use_vmm_memory` is
    on (`hash_arena.h:495` and `:529`), but always `MAP_PRIVATE|MAP_ANONYMOUS`,
    so every page costs RAM or swap. Backing a named file instead would let a
    set of files larger than memory be held - which is exactly what an atomic
    LOADFS of a big directory needs (238), and what a file store that is meant
    to hold images and video wants in general. Open questions: whether the
    mapping is MAP_SHARED over a real file or MAP_PRIVATE over one, where the
    file lives and who cleans it up, and what it means for the existing save
    and load path, which already writes shards of its own.

240. [Done] A key space for the boot imports [05-09-2026] Nr 231 527bfe8

241. Setting `server_port` or `server_binding` starts a server as a side
    effect. `configuration.cpp:422` and `:450` call
    `restarter::asynch_restart`, which stops and starts the listener on a
    thread of its own, so a caller that only meant to record a port gets a
    running server - and a caller that then starts one itself gets two. DONE
    231 works around it in barchd by keeping `--port` and `--bind` local
    rather than writing them through the configuration, but the wart is still
    there for anything that sets those from the environment
    (`BARCH_SERVER_PORT`) or `--config server_port=`, and for the python
    binding, which applies the environment on import.

    It also leaves a thread starting a server while the process is exiting,
    which is where `failed to start server std::bad_alloc` on a refused
    start-up came from. Settle by deciding whether recording a port should
    restart anything at all, or whether the restart belongs behind an explicit
    call that CONFIG SET makes and start-up does not.

242. `require` should be able to load from the file store, not only from
    function keys. Today `function_require` (luau_driver.cpp:2221) takes
    `require("space.NAME")` or `require("NAME")` in the current space, folds
    the name to upper case and asks the loader for a *function key*. The
    proposal is a second form for a path in the `fs:` layout:

        require("somespace:extensions/hnsw.luau")
        require(":extensions/hnsw.luau")        -- the default space

    The colon is a good separator for it: the existing form splits on a dot
    and a dot cannot start a name, so the two cannot be confused.

    **The naming question is already answered by what require does now.** It
    returns the module's environment table - `lua_getref(L, envt)` - so a
    stored function that defines `add` and `closest` as globals is already
    used as `local hnsw = require(...)` then `hnsw.add(...)`, exactly like a
    normal lua module. The fs form should return the same shape and nothing
    new needs inventing.

    What does need deciding:

    - The cache key. `st->functions` is keyed by `qualified(space, NAME)`, and
      a path is not a folded name, so the two namespaces have to be kept
      apart or `:a/b.luau` and a function called `B` will collide. Paths keep
      their case; function names do not.
    - Invalidation. A function key that changes has a hook that drops the
      compiled copy (function_api.h:92). A file in `fs:` has no such hook, so
      a required module would stay compiled in the VM until the state is
      recycled - which for the HTTP pool is a long time. Either the fs writes
      grow the same hook, or an fs require documents that it is a boot time
      thing.
    - Rights. `require` reaches keys; the fs read has to go through the same
      ACL the store does, or it becomes a way to read a space a caller cannot
      otherwise see.
    - Cycles. The existing `st->loading` stack should cover it as long as the
      path is what goes on the stack, not the folded name.

    And the one that is genuinely open: **wildcards**, `require(":extensions/*.luau")`.
    A single require returns one module table; a wildcard would have to return
    a table of them keyed by stem, which is a different shape from the same
    call - and `hnsw` in `{hnsw = {...}, other = {...}}` reads differently
    from `hnsw` being the module itself. Worth settling that before building
    it, or leaving wildcards out and letting a caller require what it names.

    Settle with the single path form working, a module used as `hnsw.add`,
    the cache and rights questions answered in code, and a decision recorded
    on wildcards either way.
