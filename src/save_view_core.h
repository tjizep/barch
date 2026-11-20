#ifndef BARCH_SAVE_VIEW_CORE_H
#define BARCH_SAVE_VIEW_CORE_H
/*
 * A shard's save view rules, with nothing else in them - TODO 939.
 *
 * A save freezes a view of the shard's arenas under the write latch, writes the
 * files from it with no latch held, and takes the latch again to end it. Writes go
 * on meanwhile, into CoW pages. A clear or a load drops the view and moves the
 * generation. What each step may do is decided here from plain values; shard.cpp
 * holds the latch, the mutex and the arenas.
 *
 * This header includes no locks and no files, and TestCoreBoundary fails if it
 * starts to. test/saveviewtest.cpp runs every interleaving of a few saves, clears
 * and BEGINs through it.
 */
#include <cstdint>

namespace barch::save_view_core {
    enum class freeze {
        frozen,     // a view is up now, this save's
        direct,     // inside a transaction: the CoW maps are its, so save the old way
        busy        // another save's view is up: wait for it, then try again
    };

    /** what a freeze does, under the write latch */
    inline freeze on_freeze(bool view_up, bool transacted) {
        if (view_up)
            return freeze::busy;
        if (transacted)
            return freeze::direct;
        return freeze::frozen;
    }

    /*
     * Whether a writer takes the view that is up, under a shared latch: only if no
     * other writer has. save() lets the latch go between its freeze and its write,
     * and a clear and another save's freeze in that gap leave it that save's view.
     * Two writers on one view, and the first to end it merges the CoW pages under the
     * other - TODO 939.
     */
    inline bool may_take(bool view_up, bool claimed) {
        return view_up && !claimed;
    }

    /*
     * Whether a save writes from the view it took, under save_load_mutex. Not when a
     * clear or a load moved the generation since: the view's pages are gone. Not when
     * the change log didn't reach the disk first - TODO 523, or after a load failed -
     * TODO 501.
     */
    inline bool may_write(uint64_t generation_taken, uint64_t generation_now, bool log_first,
                          bool refused_after_failed_load) {
        return generation_taken == generation_now && log_first && !refused_after_failed_load;
    }

    /*
     * Whether a save ends the view that is up, under the write latch: only its own. A
     * clear may have ended it already, and another save frozen a new one since.
     */
    inline bool ends_view(const void* up, const void* mine) {
        return up != nullptr && up == mine;
    }

    /** whether a BEGIN may start now: never while a view is up, which it would drop */
    inline bool may_begin(bool view_up) {
        return !view_up;
    }
}
#endif //BARCH_SAVE_VIEW_CORE_H
