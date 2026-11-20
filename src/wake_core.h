#ifndef BARCH_WAKE_CORE_H
#define BARCH_WAKE_CORE_H
/*
 * When the function sync poller and the queue consumer run, with nothing else in
 * them - TODO 941.
 *
 * The poller in function_sync.cpp runs each repository on its own interval, or
 * when asked, and sleeps until the next one is due. The consumer in
 * queue_service.cpp posts its work to a strand on a server's io_context, which a
 * restart destroys. Both decide from plain values here. The callers hold the
 * locks, read the clock and post.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it
 * starts to. test/waketest.cpp runs every small schedule and every order of the
 * wake and post steps through it.
 */
#include <cstddef>
#include <cstdint>

namespace barch::wake_core {
    /** the poller sleeps at most this long, so a configuration change lands */
    constexpr uint64_t idle_wait_ms = 60000;
    /** each repository's first run is this much after the one before it */
    constexpr uint64_t stagger_ms = 200;

    /** whether the poller stops waiting: told to stop, or asked for a sync */
    inline bool poller_wakes(bool running, bool kick_all, bool kick_named, bool kick_rescan) {
        return !running || kick_all || kick_named || kick_rescan;
    }

    /** one repository, as read from the configuration this pass */
    struct repo {
        bool enabled{true};
        /** its interval. 0 runs only when asked */
        uint64_t ms{0};
        /** FUNCTIONS SYNC asked for it, or for every repository */
        bool asked{false};
    };

    /** what the poller keeps about one repository between passes */
    struct schedule {
        /** the interval `due` was worked out with - TODO 584 */
        uint64_t ms{0};
        bool due_set{false};
        /** in steady clock milliseconds */
        int64_t due{0};
    };

    /** one repository's part of a pass */
    struct pass {
        bool run{false};
        /** how long the poller may sleep for this repository's sake. 0 sets no bound */
        uint64_t wait{0};
    };

    /*
     * One repository in one pass at `now`. `index` counts the first runs given out
     * this pass, for the stagger. The whole pass sleeps for the least non-zero wait
     * of its repositories, or idle_wait_ms.
     *
     * A run's wait is its interval: left at the idle cap, a repository's interval
     * held for its first run only - TODO 583. A wait is never 0 while a run is
     * ahead: a wake a fraction short of `due` found it not due and slept the idle
     * minute - TODO 788. The clock is in whole milliseconds, read as one value per
     * pass, so `due - now` is exact.
     */
    inline pass plan(const repo& r, schedule& s, int64_t now, size_t& index) {
        if (!r.enabled)
            return {};
        if (r.ms == 0)
            return {r.asked, 0};
        // a new interval starts a new schedule - TODO 584
        if (s.ms != r.ms) {
            s.ms = r.ms;
            s.due_set = false;
        }
        if (!s.due_set) {
            s.due = now + (int64_t) (stagger_ms * index++);
            s.due_set = true;
        }
        if (r.asked || now >= s.due) {
            s.due = now + (int64_t) r.ms;
            return {true, r.ms};
        }
        return {false, (uint64_t) (s.due - now)};
    }

    /** the least of a pass's waits, folded in one at a time from idle_wait_ms */
    inline uint64_t shorter(uint64_t wait, const pass& p) {
        return p.wait != 0 && p.wait < wait ? p.wait : wait;
    }

    /** who is posting to the queue consumer's strand */
    enum class poster {
        /** a publish, a settle or a Raft entry woke it */
        wake,
        /** a handler finished, and its count goes back to the strand */
        completion
    };

    /*
     * Whether the consumer may post to its strand. The caller decides and posts
     * under the consumer's post lock, and stop sets the flags under it - TODO 941.
     * Apart, a post decided before a stop landed after it, on an io_context that
     * server::start had destroyed.
     *
     * A wake stops at `stopping`. A completion still goes back while stop() waits
     * for handlers, and stops at `abandoned`, which is set when stop gives up.
     */
    inline bool may_post(poster who, bool stopping, bool abandoned) {
        if (who == poster::wake)
            return !stopping;
        return !abandoned;
    }
}
#endif //BARCH_WAKE_CORE_H
