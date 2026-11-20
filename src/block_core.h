#ifndef BARCH_BLOCK_CORE_H
#define BARCH_BLOCK_CORE_H
/*
 * The rules of a blocked client's lists - TODO 944.
 *
 * A blocking command parks its session on a key's list in a shard. A write to the key
 * wakes the first session on it, and takes it off. The woken session runs its handler
 * in the space it parked in. The handler answers, or asks to park again, and then the
 * session goes back on its keys. DROP and UNLOAD take a space out of the registry and
 * wake every session parked in it.
 *
 * The decisions are here, from plain values. shard.h holds the lists and their latch,
 * and rpc_caller.h runs the handler and keeps the park's space.
 *
 * This header includes no locks and no clock, and TestCoreBoundary fails if it starts
 * to. test/blocktest.cpp runs two clients, each a BLPOP or a CALLF, against every order
 * of parks, pushes, unloads, job ends and wakes.
 */
#include <cstddef>
#include <string_view>

namespace barch::block_core {
    /*
     * A session goes on a key's list at most once, however many times it named the
     * key. Twice, one wake signalled it twice - DONE 124.
     */
    inline bool adds(bool already_there) {
        return !already_there;
    }

    /** a list nobody waits on goes, or the map grows for the life of the shard */
    inline bool drops_list(size_t left) {
        return left == 0;
    }

    /** a write to a key wakes its first waiter, and the rest stay queued - DONE 127 */
    inline bool wakes(size_t waiting) {
        return waiting > 0;
    }

    /*
     * A key a write finds by its name. A key that starts with \x01 is the server's own:
     * CALLF's wake key, which its job wakes through a pointer it kept.
     */
    inline bool by_name(std::string_view key) {
        return key.empty() || key[0] != '\x01';
    }

    /** what a woken session does once its handler ran */
    enum class after_wake {
        /** it answered: write the reply */
        answer,
        /** back on its keys, in the space it parked in */
        park_again,
        /** the space went: answer an error, not park where nothing wakes it */
        refuse,
    };

    /*
     * Before TODO 944 a handler that asked to park again always did. Woken by DROP or
     * UNLOAD, a BLPOP found the old list still empty and parked on the old space's
     * lists. A push goes to the space opened under its name, so nothing woke it.
     * A CALLF parks again on its own key: its job still owns the session's state, and
     * ends with a wake through the pointer it kept.
     */
    inline after_wake after(bool retry, bool space_gone, bool any_by_name) {
        if (!retry)
            return after_wake::answer;
        if (space_gone && any_by_name)
            return after_wake::refuse;
        return after_wake::park_again;
    }

    /*
     * Whether the caller keeps the park's space once the handler ran. Before TODO 944
     * it kept it until the next park. The last holder of a dropped space deletes its
     * files, so an idle client that once blocked deleted the files of a space made
     * again under that name, when it closed. A timeout erases the blocks before the
     * handler runs, so a retry with no blocks is over too.
     */
    inline bool keeps_space(after_wake what, bool has_blocks) {
        return what == after_wake::park_again && has_blocks;
    }
}

#endif
