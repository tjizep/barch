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
     * on, and =segv writes through a null pointer. Anything else does nothing.
     */
    void crash_if_asked();
}
