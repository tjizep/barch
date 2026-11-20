#ifndef BARCH_COMMIT_GATE_CORE_H
#define BARCH_COMMIT_GATE_CORE_H
/*
 * The commit gate's rules, with nothing else in them - TODO 938.
 *
 * commit_gate (commit_gate.h) lets a thread in shared, in commit mode or alone. Who may
 * come in, and whether one leaving must wake the waiters, is decided here from its
 * counts. The gate keeps the counts under its mutex and waits on its condition.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it starts
 * to. test/commitgatetest.cpp runs every interleaving of a few threads through it.
 */
#include <cstddef>

namespace barch::commit_gate_core {
    enum class mode { shared, commit, alone };

    struct counts {
        size_t readers{0};
        size_t committers{0};
        bool alone{false};
        size_t waiting_readers{0};
        size_t waiting_committers{0};
        size_t waiting_alone{0};
    };

    /*
     * Shared holders go together, and so do commit holders, never one of each, and an
     * alone holder goes with nobody. A shared one comes in whenever no commit or alone
     * holder is in, so a thread holding it shared can take it shared again. A commit
     * doesn't come in while a shared or alone holder waits, so commits overlapping
     * without a gap can't keep them out for good.
     */
    inline bool may_enter(const counts& c, mode m) {
        switch (m) {
            case mode::shared:
                return !c.alone && c.committers == 0;
            case mode::commit:
                return !c.alone && c.readers == 0 && c.waiting_readers == 0 && c.waiting_alone == 0;
            case mode::alone:
                return !c.alone && c.readers == 0 && c.committers == 0;
        }
        return false;
    }

    /** whether one leaving `m`, with `c` the counts after, must wake the waiters */
    inline bool wakes(const counts& c, mode m) {
        const bool anyone = c.waiting_readers > 0 || c.waiting_committers > 0 || c.waiting_alone > 0;
        switch (m) {
            case mode::shared:
                // only commits and alone holders wait on readers
                return c.readers == 0 && (c.waiting_committers > 0 || c.waiting_alone > 0);
            case mode::commit:
                return c.committers == 0 && anyone;
            case mode::alone:
                return anyone;
        }
        return false;
    }
}
#endif //BARCH_COMMIT_GATE_CORE_H
