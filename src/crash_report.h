#pragma once
//
// What barchd says when it crashes - TODO 646.
//
// Without this a crash left nothing behind: on Windows the process just went, and
// TestLoadResult's barchd died in CI with a log that stops mid-test (TODO 644). Now a
// std::terminate, an unhandled Windows exception, or a fatal signal writes what
// happened and where to stderr before the process goes. barchd's stderr is its log.
//

namespace barch {
    /**
     * Install the handlers. Call once, first thing in main. Under ASan or TSan the
     * signal handlers are left out, since they report those themselves.
     */
    void install_crash_report();

    /**
     * Test knob: BARCH_TEST_CRASH=terminate throws from a thread nothing catches it
     * on, and =segv writes through a null pointer. =dump (Linux) times out a latch
     * wait three seconds in, so dump_threads writes a dump, and barchd carries on.
     * Anything else does nothing.
     */
    void crash_if_asked();

    /**
     * What this thread holds, filled in by the caller of dump_threads: up to `cap`
     * latches and their modes ('R', 'U', 'W'), returning how many in all. It runs
     * in a signal handler on each thread, so it mustn't allocate or lock.
     */
    using held_latches_fn = int (*)(const void** lks, char* modes, int cap);

    /**
     * A latch wait timed out: write every thread's stack to stderr, and which
     * latches each one holds, marking the ones that hold `latch` - TODO 741.
     * Without this, "read lock wait time exceeded" said nothing about who had it.
     * At most one dump every 30 seconds, since a stuck latch times out a whole
     * queue of waiters at once. Linux only; elsewhere it writes one line.
     */
    void dump_threads(const char* why, const void* latch, held_latches_fn held);
}
