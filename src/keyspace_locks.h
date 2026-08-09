//
// Created by teejip on 10/30/25.
//

#ifndef BARCH_KEYSPACE_LOCKS_H
#define BARCH_KEYSPACE_LOCKS_H
#include <optional>
#include <string>
#include "key_space.h"

/**
 * Locking more than one thing at a time, and the single rule that makes it safe.
 *
 * Deadlock between two callers needs them to take the same two locks in opposite orders.
 * The way to prevent it is not care at each call site - care is what produced
 * `KSPACE DEPENDS` locking source then dependent while `KSPACE RELEASE` locked dependent
 * then source, which is exactly the pattern that deadlocks - but a total order that
 * everything obeys regardless of what it calls the things it is locking.
 *
 * The order is:
 *
 *   1. key spaces, by canonical name, byte order.
 *   2. shards within a space, by shard number. `ordered_lock` already does this when it
 *      locks a whole space, and sharded_store's two key helper does it for a pair.
 *
 * Both are total, both are stable for the lifetime of the objects being locked, and
 * neither depends on which argument the caller happened to write first. A caller that
 * wants "the source shared and the dependent exclusive" says so; which one is taken first
 * is not its business and it must not decide.
 *
 * The rule is only worth anything if it is the only rule. Anything that takes two locks
 * goes through here or through sharded_store::with_two_keys_write - a site that hand rolls
 * its own order is a deadlock waiting for the load to be high enough to find it.
 */

struct ks_shared {
    ordered_lock<read_lock> locks;
    ks_shared() = delete;
    explicit ks_shared(barch::key_space_ptr s) : locks(s) {
    }

    ~ks_shared() = default;
};
struct ks_unique {
    ordered_lock<storage_release> locks;
    explicit ks_unique(barch::key_space_ptr s) : locks(s) {
    }
    ~ks_unique()  = default;
};

/** how a key space is to be held */
enum class ks_mode {
    shared,   ///< readers may share it
    unique    ///< exclusive, for anything that changes the space
};

/**
 * Two key spaces at once, taken in canonical order whatever order they are named in.
 *
 * Use it wherever two spaces are held together - dependency, merge, release - so that two
 * callers naming the same pair the other way round cannot deadlock. Naming the same space
 * twice locks it once, with the stronger of the two modes, because taking a space's locks
 * twice would deadlock against itself.
 *
 *     ks_two held(source, ks_mode::shared, dependent, ks_mode::unique);
 *
 * The locks are released in reverse when it goes out of scope, which ordered_lock already
 * guarantees within each space.
 */
struct ks_two {
    ks_two() = delete;
    ks_two(const ks_two&) = delete;
    ks_two& operator=(const ks_two&) = delete;

    ks_two(const barch::key_space_ptr& a, ks_mode ma,
           const barch::key_space_ptr& b, ks_mode mb) {
        if (!a && !b) return;
        if (!a) { take(b, mb); return; }
        if (!b) { take(a, ma); return; }

        // the same space twice: one set of locks, and the stronger mode wins. Taking it
        // twice would wait on itself
        if (a == b || a->get_canonical_name() == b->get_canonical_name()) {
            take(a, (ma == ks_mode::unique || mb == ks_mode::unique)
                        ? ks_mode::unique : ks_mode::shared);
            return;
        }

        // canonical order, not argument order. This is the whole point of the type
        if (a->get_canonical_name() < b->get_canonical_name()) {
            take(a, ma);
            take(b, mb);
        } else {
            take(b, mb);
            take(a, ma);
        }
    }

private:
    // held in the order they were taken; destruction unwinds them in reverse
    std::optional<ordered_lock<read_lock>> shared_first, shared_second;
    std::optional<ordered_lock<storage_release>> unique_first, unique_second;
    bool first_taken = false;

    void take(const barch::key_space_ptr& s, ks_mode mode) {
        if (!s) return;
        if (!first_taken) {
            if (mode == ks_mode::shared) shared_first.emplace(s);
            else unique_first.emplace(s);
            first_taken = true;
        } else {
            if (mode == ks_mode::shared) shared_second.emplace(s);
            else unique_second.emplace(s);
        }
    }
};

#endif //BARCH_KEYSPACE_LOCKS_H
