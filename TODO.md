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

40. Translate valkey's tests: the harness, and incr.tcl and scan.tcl as its proof.

    First of four - 45, 46 and 47 are the rest. This one carries the machinery, so it is
    the smallest in tests and the largest in work; do it first and the other three become
    mechanical.

    The suite is already in the tree and unused. test/CMakeLists.txt FetchContents valkey
    8.1 and builds it, so test/{build,Debug,RelWithDebInfo}/_deps/valkey-src holds the
    whole thing - 665 tests over the eight files that touch surfaces we implement.

    The tcl is regular enough to translate mechanically. Nearly every case is one of

        test {name} { body } {expected}          - last value of body compared
        assert_equal {expected} [r cmd args]
        catch {r cmd} err ; format $err          - matched against {ERR*}

    so a translation is `r.cmd(...)` plus an equality assert, with the catch form becoming
    an expected error. Write a script that walks the tcl and emits the obvious cases,
    leaving anything it cannot parse as a commented stub for a human. Do not translate by
    hand at volume - the script keeps the translation reviewable and re-runnable when
    valkey is bumped.

    The part that makes this safe rather than dangerous, and it has a good answer here: a
    translated test that is subtly wrong either passes when it should not, or fails for a
    reason about the translation rather than about barch. The check is differential, and
    the same FetchContent already builds a working valkey-server next to the tests:

      - run the translated test against valkey-server first. It must pass. That is what
        makes it a faithful translation rather than an assertion someone invented.
      - then run it against barch. Anything failing now is a real difference, and the
        translation is not a suspect.

    So each translated file is two tests, and the valkey one is the fixture for the other.
    That also gives a natural place to record deliberate differences: a case that passes on
    valkey and fails on barch is either a defect or an accepted divergence, and the
    accepted ones want a list with a reason each, not a quietly deleted assertion.

    Prove it on incr.tcl (29 tests) and scan.tcl (20). incr.tcl is the right first file
    because it is small and because its case "INCR fails against key with spaces" expects
    an error where barch coerced and overwrote until DONE 37 - those 214 lines would have
    caught that on their own.

    Progress: translate.py and differential.py are written and working. incr.tcl gives 21
    of 29 cases; scan.tcl gives none, because it is loops and `populate` from end to end -
    which is the honest answer and is what the stubs record. The first run found DONE 38,
    command names being case sensitive, which had made every lower case command an
    unknown one. Left to do here: raise incr.tcl's yield by teaching the translator the
    `set res {}` / `append res` form and the `list [r ...] [r ...]` form it currently
    stubs, and decide whether scan.tcl is worth hand translating or dropping from this
    entry - its twenty tests are mostly about cursor guarantees under rehashing, which is
    valkey's internals rather than a promise barch makes.

41. Static destruction order in shared_mutex.cpp, and whether it is behind the stalls.

    `rh_state` is the registry every rh_shared::shared_mutex reads - init_thread and
    release_thread mutate it, and both unlock() and unlock_shared() walk its active thread
    set on the way out. It was a file scope `static rh_state s;`, holding a std::mutex, an
    unordered_set and a thread_set.

    That is a destruction order hazard rather than an initialisation one, and the shape of
    it fits the symptom exactly. The threads holding these locks - accept threads, the
    resp workers, the session collector - are not all joined before static destruction
    runs. A thread that reaches unlock() or release_thread() after `s` is gone locks a
    destroyed pthread mutex, and that does not fault, it blocks. A process in that state
    sits at zero cpu forever, which is what the leftover valkey-servers looked like.

    Changed to the canonical form: reached through a function so the creation order is
    fixed, and deliberately never destroyed, so it outlives every thread that might still
    be unlocking on the way out. The allocation does not grow - it is one object kept
    alive on purpose.

    This was NOT the cause of the stalls. A stuck valkey-server was caught while still
    running and every thread traced, and the answer was somewhere else entirely - see
    entry 42. The change above is still right on its own terms and is worth keeping, but
    it settled nothing, and it is a good reminder that a hypothesis which fits the symptom
    is not evidence. Attaching to the process took one command and answered in one screen:

        gdb -p <pid> -batch -ex "set pagination off" -ex "thread apply all bt"

    Two neighbours worth the same treatment if this turns out to be the pattern:

      - `art/art.cpp` has a file scope `static std::mutex glob_queue{}`. Destroying a
        locked mutex is undefined, and the glob commands run on their own threads.
      - `repl_api.cpp` has a file scope `static restarter restart;`. If its destructor
        stops or joins anything, it runs during static destruction with the same exposure,
        and a destructor that joins a thread which is itself blocked is a stall that hides
        behind a different symptom.

    Also noticed while reading the file, and unrelated to the above: lines 233 to 294 are a
    four thread, ten million iteration stress test of the lock, ending in
    `static int tested = test();` so that it would run at library load. It is inside
    `#if 0` and therefore dead. It is worth either deleting it or moving it into the test
    directory where it can be run on purpose - left where it is, enabling it would run a
    long threaded benchmark before main on every load of the module, which is a surprising
    thing for a one character change to do.

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

45. Translate valkey's tests: string.tcl and keyspace.tcl.

    Second of four, and the one closest to what barch is for. 91 tests in string.tcl and
    59 in keyspace.tcl, against SET, GET, APPEND, MSET, MGET, EXISTS, DEL, TTL, EXPIRE,
    KEYS, SELECT and the rest of the key/value surface. Depends on the harness in 40.

    Expect this one to find the most, because it is the surface DONE 33 and DONE 37 changed
    most heavily and the surface every client touches. SET's options in particular now
    parse in any order and reject contradictory pairs, which is exactly what string.tcl
    exercises at length.

    Known divergences to expect rather than treat as defects: `SETRANGE`, `GETRANGE`,
    `GETDEL`, `GETEX`, `SETEX`, `PSETEX`, `SUBSTR` and `LCS` are not implemented, and
    `SELECT` takes a name as well as a number, which is a superset. Filter those cases out
    with a reason recorded rather than deleting them silently.

46. Translate valkey's tests: hash.tcl and expire.tcl.

    Third of four. 71 tests in hash.tcl and 79 in expire.tcl, against the fifteen H*
    commands and the expiry surface - EXPIRE, TTL, EXPIREAT, PERSIST and the hash field
    TTLs. Depends on the harness in 40.

    The hash surface is worth care: DONE 33 changed HGET from a one element array to a bulk
    string, HEXISTS from a nested array to an integer, and HSET to count fields added, and
    DONE 37 made HINCRBY refuse a non numeric field instead of overwriting it. Those are
    exactly the assertions hash.tcl makes, so it is a direct check of that work rather than
    a search for something new.

    expire.tcl is the harder half because it is timing sensitive - several cases sleep and
    then assert a TTL band. Translate those with the same tolerances valkey uses rather
    than tightening them, or the test will be flaky on a loaded machine and get ignored,
    which is worse than not having it.

    Known divergences: `PERSIST`, `PEXPIRETIME` and `OBJECT FREQ` are not implemented, and
    the hash field TTL commands take the FIELDS form only.

47. Translate valkey's tests: zset.tcl and list.tcl, filtered.

    Last of four, and the one where the work is filtering rather than translating. 168
    tests in zset.tcl and 148 in list.tcl, but a large fraction cover commands barch does
    not implement, so the first job is deciding what applies and recording why the rest
    does not. Depends on the harness in 40.

    Not implemented on the ordered set side: ZSCORE, ZMSCORE, ZUNION, ZUNIONSTORE,
    ZREVRANK, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZRANDMEMBER, ZLEXCOUNT, ZSCAN, BZPOPMIN
    and BZPOPMAX. On the list side: LRANGE, LINSERT, LSET, LREM, LTRIM, LPOS, LMPOP,
    RPOPLPUSH and LMOVE - which is most of list.tcl, so expect only a fraction to survive.

    What does apply is worth having, because both surfaces moved under DONE 37: ZRANGE and
    ZREVRANGE read positions now, ZRANK reports a member's position, ZPOPMIN and ZPOPMAX
    answer member first, and LPUSH, RPUSH, LPOP and RPOP were all on the wrong ends. Those
    are the cases these two files spend most of their length on.

    Ignore the encoding tests - valkey checks listpack against quicklist and skiplist
    against listpack conversions, which are its internals and say nothing about barch.

48. INCRBYFLOAT is not implemented. `HINCRBYFLOAT` exists for hash fields but there is no
    plain key equivalent, so `INCRBYFLOAT k 1.5` answers `unknown command`. Found by the
    translation harness (TODO 40) - incr.tcl spends a third of its cases on it.

    The work is small: BarchModifyInteger already takes the type as a template parameter
    and hash_api's HNUMERIC does the double form, so this is mostly registration plus the
    reply formatting. The part worth care is what redis promises about the reply - it is a
    bulk string, not a double, and it is rendered without a trailing zero, so 3.0 comes
    back as "3". incr.tcl asserts exactly that in several places, which is a good reason to
    do this one after the harness rather than before.

49. There is no WRONGTYPE. Redis refuses a command whose key holds a different type -
    `RPUSH mylist 1` then `INCR mylist` answers `WRONGTYPE Operation against a key holding
    the wrong kind of value`. barch does not check, and because a list stores its entries
    under composite keys derived from the name, `INCR mylist` quietly operates on a
    different key altogether and succeeds.

    Found by the translation harness (TODO 40); incr.tcl, string.tcl and list.tcl all
    assert it, so it will keep appearing as the other files are translated.

    This is a bigger decision than it looks and should not be taken by whoever hits it
    next in a test. barch's types are a property of how a key is encoded rather than a
    tag on a value, so answering WRONGTYPE means being able to ask "is there a list called
    mylist" cheaply when someone calls INCR on it, for every command and every type. That
    is a real cost on the hot path for a check that only matters when a caller has already
    made a mistake. The alternatives are to accept the divergence and document it, or to
    check only where the composite key makes it cheap.

    Until it is decided, differential.py carries both cases in its ACCEPTED list with this
    entry as the reason, so the harness stays green and the divergence stays visible.
