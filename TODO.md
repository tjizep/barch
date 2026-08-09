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
38. Redis compatibility, the part that is not just a wrong answer. DONE 33 fixed the
    commands that returned the wrong value or the wrong RESP type. What is left are the
    ones where the behaviour itself differs under a shared name, so fixing them breaks
    whatever is built against the current behaviour. The direction is settled - redis
    compatibility is the aim - so these are a question of sequencing and of what to do
    with existing callers, not of whether.

    Semantics that differ under a redis name. Each of these needs a decision about the
    existing behaviour before the name can be moved:

      - [Done 09-08-2026] `LPUSH` prepends and `RPUSH` appends, and `LPOP`, `RPOP`, `BLPOP`
        and `BRPOP` follow the same ends. All six were the other way round. The flag they
        pass was called `left` while it actually selected the high index end, which is
        most of why it went unnoticed; it is `at_tail` now.

        The migration is smaller than this entry feared. The stored layout does not change
        - the header still records start and end and the entries still sit between them -
        so a list saved before this reads back in the same order it was written. What
        changes is which command built it: a caller who used LPUSH to append has to use
        RPUSH now, and `LFRONT` on an old list answers with what that caller thought of as
        the most recent entry. Nothing needs rewriting on disk, but a client that relied
        on the old direction has to be looked at.
      - [Done 09-08-2026] `LPOP` and `RPOP` take an optional count and reply with what was
        removed - one bulk string without a count, an array with one. The bytes are copied
        before the entry is removed, since the leaf goes with it.
      - [Done 09-08-2026] `ZRANGE` and `ZREVRANGE` read start and stop as positions unless
        BYSCORE or BYLEX says otherwise, so `ZRANGE key 0 -1` is the whole set. Negative
        positions count from the end and out of range ones are clamped, as in redis. Done
        as its own walk rather than by rank lookup - one pass in score order, then the
        slice - which is simpler and is what REV already needed anyway.
      - [Done 09-08-2026] `ZRANK key member [WITHSCORE]` reports that member's position
        from the low end, nil when it is not there. The range count it used to do is left
        to `ZFASTRANK`, which answers it in constant time.
      - [Done 09-08-2026] `SELECT` is its own handler. A number is a database - 0 is the
        default space, n above zero is `db<n>` - so the numbered databases a redis client
        switches between are real rather than spaces whose names happen to be digits. A
        name still selects that space, which barch has always allowed here and
        spacethreadtest.py exercises deliberately ("Yes! we can select strings too"), so
        this is a superset of redis rather than a departure. A first attempt refused names
        and broke that test, which is a fair argument for reading what a test asserts
        before deciding a behaviour was accidental.
      - [Done 09-08-2026] `SET`'s dead `H` option is gone and is now refused like any other
        unknown word.
      - [Done 09-08-2026] `SET`'s options are order free. The parser loops over what follows
        the key and value instead of walking fixed positions, and refuses an unknown word,
        a repeat, or a contradictory pair such as NX with XX or KEEPTTL with EX.
      - [Done 09-08-2026] The eight increment commands no longer overwrite a value that is
        not a number. leaf_numeric_update now reports why it declined - not_numeric,
        overflowed or compressed - and the callers tell a decline from a missing key
        instead of reading both as "insert", which is what destroyed the value.
        respshapetest.py updated in the same change.

    Arity checks that are missing rather than wrong. Cheap, no compatibility question:

      - [Done 09-08-2026] `ZDIFFSTORE` and `ZINTERSTORE` check their arity before reading
        argv[1].
      - [Done 09-08-2026] `EXPIRE` refuses a word in the condition position that is not
        NX, XX, GT or LT, rather than consuming it and ignoring it.

    ACL categories that do not match what the command does. These matter more than they
    look: DONE 32 established that a category a command does not declare cannot be
    required, and the mirror of that is that one it declares wrongly demands a permission
    the caller should not need. A read only user cannot currently call these:

      - [Done 09-08-2026] All seven recategorised: `LBACK`, `ZCARD`, `ZRANGE`, `ZDIFF` and
        `ZINTERCARD` are read rather than write, `HEXPIRETIME` is read, and `CLIENT` moved
        from stats to connection. `ZINTER` had been listed here in error - it was already
        read. `ZDIFFSTORE`, `ZINTERSTORE`, `ZPOPMIN`, `ZPOPMAX` and `ZREMRANGEBYLEX`
        genuinely write and were left alone.

    [Done 09-08-2026] `LFRONT`, `LBACK` and `LLEN` take a read lock rather than a write
    one, which is what DONE 19 did for SIZE and HEAPBYTES.

    Worth doing before any of it: there is still no test that asserts a reply shape. DONE
    33 was verified by a throwaway script that spoke RESP at a scratch server, and that
    script is the thing that should become a test - the defects it caught were all
    invisible to the existing suite, which tests through the embedded interface and never
    sees the wire.

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

40. Translate valkey's own tests and run them against barch. The suite is already in the
    tree and nobody has used it: test/CMakeLists.txt FetchContents valkey 8.1 and builds
    it, so test/{build,Debug,RelWithDebInfo}/_deps/valkey-src holds the whole thing.
    tests/unit/type has string.tcl (810 lines), hash.tcl (864), incr.tcl (214),
    list.tcl (2490), zset.tcl (2744), and tests/unit adds expire.tcl (1075),
    keyspace.tcl (571) and scan.tcl (470). That is a specification of the behaviour we
    have just decided to aim for (TODO 38), written by the people who define it.

    The tcl is regular enough to translate mechanically. Nearly every case is one of

        test {name} { body } {expected}          - last value of body compared
        assert_equal {expected} [r cmd args]
        catch {r cmd} err ; format $err          - matched against {ERR*}

    so a translation is `r.cmd(...)` plus an equality assert, with the catch form becoming
    an expected error. It is worth doing incr.tcl first: it is the smallest, and its case
    "INCR fails against key with spaces" expects an error where barch currently coerces
    and overwrites, which is the data loss item in TODO 38. One 214 line file would have
    caught that on its own.

    The part that needs thinking about is the one that makes this worth doing rather than
    dangerous, and it has a good answer here. A translated test that is subtly wrong is
    worse than no test: it either passes when it should not, or fails for a reason that is
    about the translation rather than about barch, and either way someone burns an
    afternoon. The check is differential, and it is available because the same
    FetchContent already builds a working valkey-server next to the tests:

      - run the translated python test against valkey-server first. It must pass. That is
        what makes it a faithful translation rather than an assertion someone made up.
      - then run it against barch. Anything that fails now is a real difference, and the
        translation is not a suspect.

    So each translated file is really two tests, and the valkey one is the fixture for the
    other. That also gives a natural way to record deliberate differences: a case that
    passes on valkey and fails on barch is either a defect or an accepted divergence, and
    the accepted ones want a list of their own with a reason each, not a quietly deleted
    assertion.

    Scope, since the whole suite is not the goal: only the commands we register. set.tcl
    and the stream files are out - there is no S* or X* surface. list.tcl and zset.tcl are
    large and full of cases for options we do not implement, so those want filtering
    rather than translating whole. string.tcl, incr.tcl, hash.tcl, expire.tcl and
    keyspace.tcl are the ones that map closely.

    Do not translate by hand at volume. A small script that walks the tcl and emits the
    obvious cases, leaving anything it cannot parse as a commented stub for a human, will
    get most of the way and keeps the translation itself reviewable.

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

44. Array replies come back wrong through a remote binding.

    Every SWIG binding that answers with more than one value gives the right answer
    against a local store and the wrong one against a remote one. Measured on the same
    build, same data, the only difference being whether the handle was constructed with a
    host and port:

        local  HMGET f1 f2 f3   -> ['v1', 'v2', 'v3']
        remote HMGET f1 f2 f3   -> ['v1', 'false', '0.0']

        local  OrderedSet.range -> ['one', 'two', 'three']
        remote OrderedSet.range -> ['one', 'false']

        local  List.pop(k, 2)   -> ['a2', 'a1']
        remote List.pop(k, 2)   -> ['a2', 'false']

        remote KeyValue.range   -> []            (three keys in range)

    The first element survives and the rest arrive as something else - usually a boolean,
    sometimes a double - so it is not truncation, it is the reply being decoded against
    the wrong types from the second element on. A single element reply is correct, which
    is why nothing noticed: every binding that returns an array was only ever exercised
    remotely with one value in it, or not at all.

    This is not new. It was found because LPOP was changed to answer with the values it
    removed instead of the length left behind (TODO 38), so List.pop started returning an
    array where it used to return one integer, and remotetest.py is the only test that
    drives a binding over RPC. The bug was already there behind every other array
    returning call.

    Where to start: the local and remote paths differ in how the flat result is filled -
    `sc.call` then `append_flat` against a caller that is either local or an rpc_caller -
    so the disagreement is in how the rpc reply is turned back into Values, not in the
    commands. A reply with three bulk strings that decodes as string, bool, double looks
    like a reader taking its type from the wrong position, one field out of step.

    Worth writing a test for the shape of it first: the same command run through a local
    handle and a remote handle, asserting the two agree. That is a stronger check than any
    single expectation and would have caught this the day it appeared.
