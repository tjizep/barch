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

67. A flush() on the caller interface, so a reply does not have to fit in memory.

    Every reply is assembled whole before any of it is sent. That is behind most of the
    memory limits barch has: KEYS and SCAN buffer the entire answer, HRANDFIELD and
    ZRANDMEMBER cap a negative count at a million because the reply would be built before
    it was written, LCS holds its whole table, and the glob's memory ceiling (DONE 63)
    exists to stop a walk that would otherwise be bounded only by the key space. redis has
    none of these because it writes as it goes.

    What is wanted is `flush()` on `caller`: push what has accumulated to the socket and
    carry on. The rpc_caller writes to a socket and can do it; vk_caller hands its reply to
    valkey and probably cannot, so the interface has to allow a flush that does nothing.

    The obstacle is the array length, and it is worth understanding before starting. RESP2
    prefixes an aggregate with its element count, so nothing inside an array can be sent
    until the count is known - which is exactly what these commands do not know until they
    finish. Three ways out, in the order they are worth trying:

      - **RESP3 streamed aggregates.** `*?` opens an array of unknown length and `.` closes
        it. A connection that has said HELLO 3 can be streamed to today, and one that has
        not cannot - so this is per connection and the buffering path stays for RESP2.
      - **Flush between top level replies.** Commands that answer with many independent
        items rather than one array - the export, replication catch up - can flush at each
        boundary with no protocol change at all.
      - **Count first, then stream.** Two passes over the data: one to count, one to send.
        Doubles the walk and is only sane where the walk is cheap relative to the reply.

    Whichever lands, `caller::flush()` is the thing to add first, because the commands can
    then be converted one at a time and the ones that are not converted keep working.

68. [Done] A failed EXPORT leaves an empty file where the old one was [12-08-2026] Nr 64

69. [Done] Per shard statistics, clear and load no longer rewrite the globals [12-08-2026] Nr 63

70. `bloom_t` in `abstract_shard.h` is `std::vector<bool>`. Set it to `heap::vector<bool>`
    and some of the tests fail.

    The bloom is a presence filter on the shard: `add_bloom` / `is_bloom` hash a key and
    set or read one bit. `create_bloom` replaces the vector and, when enabled, resizes it
    to `static_bloom_size`. `std::vector<bool>` is the bit-packed specialization with
    proxy references. `heap::vector<bool>` is used elsewhere (`acl`, command categories)
    as a real sequence of bools, and it goes through the tracking allocator.

    What is uncertain is why that substitution breaks tests: a different `operator[]`,
    a size or resize that no longer matches `static_bloom_size`, a filter that then
    answers "missing" for a key that is there, or something the allocator counts that
    `std::vector<bool>` never did. Settle by reproducing with the typedef changed, noting
    which tests fail, and tracing one failure through `is_bloom` / `add_bloom`.

71. [Done] The command index in docs/index.html does not know the commands from DONE 65 [13-08-2026] Nr 66

72. [Done] Claude-isms in docs/index.html [14-08-2026] Nr 67

73. [Done] User and developer sections in docs/index.html [14-08-2026] Nr 68

74. [Done] art_* tree functions moved into namespace art [14-08-2026] Nr 69