#ifndef BARCH_LUAU_PARK_CORE_H
#define BARCH_LUAU_PARK_CORE_H
/*
 * What a Luau call does when it yields, and when the work it parked on finishes -
 * TODO 942.
 *
 * A C function that waits on something that is not Lua parks the call: park_call
 * marks the call parked, the C function starts its work, and park_yield yields. The
 * pump in luau_driver.cpp then sees the yield, and complete_call sees the work
 * finish. Either may come first. A raise between park_call and park_yield is a Luau
 * error that the script's pcall can catch, so the call can run on, and yield for
 * its budget, with the park still marked.
 *
 * The decisions are here, from plain values. The driver holds the park's lock and
 * does the pushes and the posts.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it
 * starts to. test/luauparktest.cpp runs every order of a script's parks, raises and
 * budget yields against every order of the completions through it.
 */

namespace barch::luau_park_core {
    /** what the pump does with a yield */
    enum class on_yield {
        /** the work is not done: wait, and the completion resumes the call */
        wait,
        /** the work finished before the yield was seen: resume the call now */
        resume_now,
        /** a budget yield: the slice is used up */
        budget,
        /** a park that never yielded: drop it, then a budget yield */
        stale_budget,
    };

    /*
     * A yield. `parked` is the call marked parked, `park_yielded` that park_yield
     * made this yield, `done` that the parked work has finished. `done` is read
     * under the park's lock, and `wait` sets the park's `waiting` under the same
     * hold.
     *
     * Before TODO 942 `parked` alone made a yield a park's. A raise after park_call,
     * caught by the script, left `parked` set, and the next budget yield waited for
     * a completion that would never come, or came for a yield it was not.
     */
    inline on_yield yield(bool parked, bool park_yielded, bool done) {
        if (!parked)
            return on_yield::budget;
        if (!park_yielded)
            return on_yield::stale_budget;
        return done ? on_yield::resume_now : on_yield::wait;
    }

    /** where the next slice of a budget yield runs */
    enum class slice_at {
        /** on this thread: a caller waits for it here, or a handler holds its slot */
        here,
        pool,
        /** past its deadline, in the overtime pool - TODO 836 */
        overtime_pool,
    };

    inline slice_at next_slice(bool inline_only, bool http, bool overtime) {
        if (inline_only || http)
            return slice_at::here;
        return overtime ? slice_at::overtime_pool : slice_at::pool;
    }

    /** what complete_call does, under the park's lock */
    enum class on_complete {
        /** a second completion */
        ignore,
        /** sets `done`. The pump has not seen the yield and will see `done` */
        record,
        /** sets `done` and clears `waiting`. The pump waits, so this resumes it */
        record_and_resume,
    };

    inline on_complete complete(bool done, bool waiting) {
        if (done)
            return on_complete::ignore;
        return waiting ? on_complete::record_and_resume : on_complete::record;
    }
}

#endif
