//
// Created by teejip on 8/1/26.
//
// The sharding layer. Every algorithm here was previously open coded inside whichever
// command needed it - see the class comment in sharded_store.h for why they moved.
//

#include "sharded_store.h"
#include "abstract_shard.h"

#include <algorithm>
#include <optional>

#include "art/iterator.h"
#include "keys.h"
#include "statistics.h"
#include "aof_log.h"
#include "lzr_log.h"

namespace barch {

/**
 * route to the shard owning `key` and lock it, making sure the route is still true once
 * the lock is held.
 *
 * Routing and locking cannot be one step, so there is always a gap between deciding
 * which shard owns a key and holding the lock that stops it changing. Hash routing does
 * not care, because a key's shard is a pure function of the key and nothing can move it.
 * A range sharded space rebalances, so between the two the boundary can move and leave
 * the caller holding a lock on a shard the key no longer belongs to - which would insert
 * a key where nothing will ever look for it, or report a live key as absent.
 *
 * Re-reading the routing table under the lock closes that. If it still says this shard,
 * no rebalance can take the key out of it while the lock is held, because a rebalance
 * changes the table only while holding a write lock on every shard whose span changes.
 * If it says another shard, the boundary moved in between: drop the lock and go again.
 * The retry cannot spin for long - it takes another rebalance of this key's boundary to
 * lose a second time, and those are bounded by the maintenance interval.
 */
template<typename Lock>
static shard_ptr route_locked(const sharded_store& store, art::value_type key,
                              std::optional<Lock>& held) {
    for (;;) {
        // through shard_for rather than the key space, because that is the routing
        // primitive the class is meant to be specialised on
        auto t = store.shard_for(key);
        if (!t) return t;
        // a script's explicit lock is already holding this shard, so taking it again
        // would be EDEADLK on this thread - and a *different* shard is the cross shard
        // case that has to fail loudly rather than deadlock. See TODO 98 F6
        if (shard_already_held(store.space().get(), t.get()))
            return t;
        held.emplace(t);
        if (!store.space()->route_moved(key, t)) {
            return t;
        }
        held.reset();
    }
}

shard_hold& shard_hold::current() {
    static thread_local shard_hold held{};
    return held;
}

bool shard_already_held(const void* space, const void* shard) {
    const auto& held = shard_hold::current();
    if (held.covers(space, shard))
        return true;
    if (held.conflicts(space, shard)) {
        // naming both is the point: "a second shard" means nothing to someone who
        // cannot see the routing, and the whole reason this is an error rather than a
        // wait is that there is no way out of it once two scripts have one each
        throw cross_shard_lock(
            "FUNCTION a locked region reached a second shard. A script may hold one "
            "shard, or the whole space, never two - put the keys in one container if "
            "they have to be locked together");
    }
    return false;
}

art::merge_iterator make_merged(const shard_ptr& shard, art::value_type lower) {
    auto s = shard->sources();

    art::merge_iterator m({art::iterator(shard, lower), art::iterator(s, lower)});

    return m;
}

sharded_store::sharded_store(key_space_ptr space) : spc(std::move(space)) {
    if (!spc) {
        throw_exception<std::runtime_error>("sharded_store: no key space");
    }
}

size_t sharded_store::shard_count() const {
    return spc->get_shard_count();
}

size_t sharded_store::size() const {
    return spc->get_local() ? spc->get_local()->get_size() : 0;
}

shard_ptr sharded_store::shard_for(art::value_type key) const {
    return spc->get(key);
}

const heap::vector<shard_ptr>& sharded_store::shards() const {
    return spc->get_shards();
}

// ---- single key ----

bool sharded_store::search(art::value_type key, const node_cb& cb) const {
    // a shard with sources cannot answer from its own bloom filter alone, because the
    // key may only exist upstream
    auto ruled_out = [&key](const shard_ptr& t) {
        return !t->sources() && t->has_static_bloom_filter() && !t->is_bloom(key);
    };
    auto t = shard_for(key);
    if (!t) return false;

    const bool moves = spc->routes_move();
    // where routing cannot move a key the filter is worth reading before taking the
    // lock, because a miss saves the lock entirely. Where it can, a filter read off the
    // shard that used to own the key says nothing, so it waits until the route is settled
    if (!moves && ruled_out(t)) {
        return false;
    }
    std::optional<read_lock> release;
    t = route_locked<read_lock>(*this, key, release);
    if (!t) return false;
    if (moves && ruled_out(t)) {
        return false;
    }
    auto r = t->search(key);
    if (r.null() || r.cl()->is_tomb()) {
        return false;
    }
    cb(r);
    return true;
}

sharded_store::read_state sharded_store::search_state(art::value_type key,
                                                     const node_cb& cb) const {
    auto ruled_out = [&key](const shard_ptr& t) {
        return !t->sources() && t->has_static_bloom_filter() && !t->is_bloom(key);
    };
    auto moves = space()->opt_range_sharded;
    auto t = shard_for(key);
    if (!t) return read_state::absent;
    if (!moves && ruled_out(t)) {
        return read_state::absent;
    }
    std::optional<read_lock> release;
    t = route_locked<read_lock>(*this, key, release);
    if (!t) return read_state::absent;
    if (moves && ruled_out(t)) {
        return read_state::absent;
    }
    // local_leaf rather than search, because `shard::search` erases a tomb to null
    // before a caller can see it - which is right for a command that wants the value
    // and is exactly the difference this function exists to report. The foreign path
    // reads it the same way, for the same reason
    auto r = t->local_leaf(key);
    if (r.null())
        return read_state::absent;
    auto cl = r.const_leaf();
    if (cl->expired())
        return read_state::absent;      // a lapsed tomb is askable again, so unknown
    if (cl->is_tomb())
        return read_state::tombed;
    cb(r);
    return read_state::present;
}

bool sharded_store::exists(art::value_type key) const {
    auto ruled_out = [&key](const shard_ptr& t) {
        return !t->sources() && t->has_static_bloom_filter() && !t->is_bloom(key);
    };
    auto t = shard_for(key);
    if (!t) return false;
    const bool moves = spc->routes_move();
    if (!moves && ruled_out(t)) {
        return false;
    }
    std::optional<read_lock> release;
    t = route_locked<read_lock>(*this, key, release);
    if (!t) return false;
    if (moves && ruled_out(t)) {
        return false;
    }
    return !t->search(key).null();
}

bool sharded_store::insert(const art::key_options& opts, art::value_type key,
                           art::value_type value, bool update, const art::NodeResult& fc) {
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->opt_insert(opts, key, value, update, fc);
}

bool sharded_store::add(const art::key_options& opts, art::value_type key,
                        art::value_type value, const art::NodeResult& fc) {
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->insert(opts, key, value, false, fc);
}

bool sharded_store::remove(art::value_type key, const art::NodeResult& fc) {
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->remove(key, fc);
}

bool sharded_store::update(art::value_type key, const updater_fn& updater) {
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->update(key, updater);
}

void sharded_store::with_key_write(art::value_type key, const shard_fn& fn) {
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return;
    fn(t);
}

/**
 * Two keys at once, for the commands that move a value from one to another.
 *
 * The order is by shard number, never by which key the caller named first - see the rule
 * in keyspace_locks.h. RENAME a->b and RENAME b->a run concurrently often enough to find
 * a hand chosen order, and when both keys land on the same shard it is locked once, since
 * taking a shard's write lock twice waits on itself.
 *
 * The route is re-checked after the locks are held, the way route_locked does it: on a
 * range sharded space a key's shard can move under a router, so a route that changes
 * between deciding and locking means dropping both and trying again. Two locks make that
 * slightly more likely and no harder - the retry cannot spin, because moving a key needs
 * the very locks being held.
 */
void sharded_store::with_two_keys_write(art::value_type a, art::value_type b,
                                        const two_shard_fn& fn) {
    for (;;) {
        auto sa = shard_for(a);
        auto sb = shard_for(b);
        if (!sa || !sb) return;

        if (sa == sb) {
            std::optional<storage_release> one;
            auto t = route_locked<storage_release>(*this, a, one);
            if (!t) return;
            if (shard_for(b) != t) {
                continue;   // b moved out from under us
            }
            fn(t, t);
            return;
        }

        // lowest shard number first, whatever order the caller named them in
        auto first = sa->get_shard_number() <= sb->get_shard_number() ? sa : sb;
        auto second = first == sa ? sb : sa;
        storage_release lock_first(first);
        storage_release lock_second(second);
        if (shard_for(a) != sa || shard_for(b) != sb) {
            continue;   // a boundary moved between routing and locking
        }
        fn(sa, sb);
        return;
    }
}

void sharded_store::with_key_read(art::value_type key, const shard_fn& fn) const {
    std::optional<read_lock> release;
    auto t = route_locked<read_lock>(*this, key, release);
    if (!t) return;
    fn(t);
}

// the same route, lock, re-route rule as route_locked above, for the callers that need
// to hold the lock past the end of a callback

sharded_store::write_locked_shard sharded_store::write_locked(art::value_type key) const {
    for (;;) {
        auto t = shard_for(key);
        write_guard g(t);
        if (!spc->route_moved(key, t)) return {t, std::move(g)};
    }
}

sharded_store::read_locked_shard sharded_store::read_locked(art::value_type key) const {
    for (;;) {
        auto t = shard_for(key);
        read_guard g(t);
        if (!spc->route_moved(key, t)) return {t, std::move(g)};
    }
}

sharded_store::write_guard sharded_store::lock_key_write(art::value_type key) const {
    for (;;) {
        auto t = shard_for(key);
        write_guard g(t);
        // the callers of this route again under the lock to get at the shard, so the
        // route has to be settled before it is handed over
        if (!spc->route_moved(key, t)) return g;
    }
}

sharded_store::write_guard sharded_store::lock_space_write() const {
    return write_guard(spc);
}

sharded_store::read_guard sharded_store::lock_space_read() const {
    return read_guard(spc);
}

void sharded_store::with_container_write(art::value_type container, const shard_fn& fn) {
    with_key_write(container, fn);
}

void sharded_store::with_container_read(art::value_type container, const shard_fn& fn) const {
    with_key_read(container, fn);
}

// ---- whole space ----

void sharded_store::each_shard(const shard_fn& fn) const {
    for (const auto& t : shards()) {
        fn(t);
    }
}

void sharded_store::each_shard_write(const shard_fn& fn) const {
    for (const auto& t : shards()) {
        if (shard_already_held(space().get(), t.get())) {
            fn(t);
            continue;
        }
        storage_release release(t);
        fn(t);
    }
}

void sharded_store::each_shard_read(const shard_fn& fn) const {
    for (const auto& t : shards()) {
        if (shard_already_held(space().get(), t.get())) {
            fn(t);
            continue;
        }
        read_lock release(t);
        fn(t);
    }
}

void sharded_store::each_shard_parallel(const shard_fn& fn) const {
    // shard_thread_processor caps how many run at once. one thread per shard would be
    // 347 of them on a default space, which is what LOAD and RELOAD used to do
    const auto& all = shards();
    shard_thread_processor(all.size(), [&all, &fn](size_t i) {
        fn(all[i]);
    });
}

size_t sharded_store::save_space() const {
    std::atomic<size_t> errors = 0;
    const sharded_store& store = *this;
    const auto& save_log = store.space()->get_change_log();
    uint64_t saved_through = 0;
    const auto count_failure = [&](const shard_ptr& shard, bool ok) {
        if (!ok) {
            err({"could not save", shard->get_shard_number()});
            ++errors;
        }
    };

    if (!store.space()->is_stateful_sharding()) {
        /*
         * Where the log is before anything is saved. Each shard freezes on its
         * own while writes carry on, so a write to a shard that's already frozen
         * is in the log and in no file. The checkpoint below covers this mark and
         * not "everything so far", or the trim right after it would drop those
         * writes and a crash would lose them - TODO 452.
         */
        saved_through = save_log ? save_log->mark() : 0;
        store.each_shard_parallel([&](const shard_ptr& shard) {
            count_failure(shard, shard->save(true));
        });
    } else {
        /*
         * A stateful method moves keys between shards, so the files have to be
         * one moment or a key can land in two of them or in neither. Every shard
         * is frozen under one space write lock - a short one, for the CoW maps and
         * the state that isn't pages - and the files are written after it's let
         * go, while writes carry on into the CoW pages - TODO 465. Under that
         * lock no write is half done, so the log mark is exactly what the files
         * hold.
         */
        bool in_transaction = false;
        for (;;) {
            shard_ptr busy;
            {
                write_guard all = store.lock_space_write();
                store.each_shard([&](const shard_ptr& t) {
                    if (!busy && t->frozen_for_save()) busy = t;
                    if (t->in_transaction()) in_transaction = true;
                });
                if (!busy && !in_transaction) {
                    saved_through = save_log ? save_log->mark() : 0;
                    store.each_shard([&](const shard_ptr& t) {
                        // nothing else is frozen and nothing is in a transaction,
                        // so every one of these freezes
                        if (t->freeze_for_save_holding_lock(true) != abstract_shard::save_freeze::frozen)
                            abort_with("a shard would not freeze under the space lock");
                    });
                }
            }
            if (!busy)
                break;
            // another save's freeze; it merges when its files are written
            busy->wait_for_frozen_save();
        }
        if (!in_transaction) {
            store.each_shard_parallel([&](const shard_ptr& shard) {
                count_failure(shard, shard->write_frozen());
            });
        } else {
            /*
             * A transaction's CoW maps are its own, so there's nothing to freeze
             * with. Every shard held shared for the whole write, the way it was
             * before TODO 465: slower for writers, but it's one moment. The
             * workers save on this thread's hold rather than their own: the latch
             * prefers writers, so a worker asking for it again waits behind a
             * queued writer, which waits on this - TODO 462.
             */
            read_guard held = store.lock_space_read();
            saved_through = save_log ? save_log->mark() : 0;
            store.each_shard_parallel([&](const shard_ptr& shard) {
                count_failure(shard, shard->save_holding_lock(true));
            });
        }
    }
    /*
     * The checkpoint goes here and nowhere else - TODO 355.
     *
     * It says every write up to `saved_through` is in the shard files, so it can
     * only be written when every shard of the space is on disk. That is true here and
     * only here: the interval save in shard.cpp saves one shard at a time, so a
     * checkpoint after one of those would claim the whole space was saved when
     * fifteen of seventeen shards had not been. (A range-sharded space doesn't
     * save one shard at a time - its maintenance calls this instead.)
     *
     * And only when nothing failed. A checkpoint after a partial save is a lie
     * that survives a crash, and a replay believing it skips records that are
     * still the only copy of what they describe.
     *
     * The trim right after is what bounds the file: everything the checkpoint
     * covers is in the shard files now, so it does not need to be in the log
     * as well.
     */
    if (errors == 0) {
        if (save_log) {
            try {
                save_log->checkpoint(store.space()->space_name(), saved_through);
                save_log->trim_to_last_checkpoint();
            } catch (const std::exception& e) {
                // the save worked; the log bookkeeping did not, and saying so is
                // better than failing a save that is already on disk
                err({"saved, but could not checkpoint the change log:", e.what()});
            }
        }
    }
    return errors;
}

void sharded_store::clear_space() const {
    write_guard all = lock_space_write();
    if (const auto& change_log = space()->get_change_log())
        change_log->append_clear(space()->space_name(), (uint32_t) shards().size());
    each_shard([](const shard_ptr& shard) { shard->clear_holding_lock(); });
}

// ---- ordered fan out ----

/**
 * true when the shards are a partition of the key order, so that shard number and key
 * order agree and the operations below can walk shards instead of merging them.
 *
 * A space with a pull source is excluded even when it is range sharded: the keys it
 * answers for are not all its own, and the ones upstream are not part of the partition.
 */
bool sharded_store::ordered_shards() const {
    return spc->is_range_sharded() && !spc->source();
}

bool sharded_store::minimum(const key_cb& cb) const {
    art::value_type the_min;
    // both shared, but still a pair, so it takes the same canonical order everything
    // else does rather than source-then-self by habit
    ks_two held(spc->source(), ks_mode::shared, spc, ks_mode::shared);
    if (ordered_shards()) {
        // the smallest key in the space is the smallest key of the first shard that has
        // one - no other shard can hold anything below it
        for (const auto& t : shards()) {
            if (!t->get_tree_size()) continue;
            art::node_ptr r = t->tree_minimum();
            if (!r.is_leaf) continue;
            cb(r.const_leaf()->get_key());
            return true;
        }
        return false;
    }
    for (const auto& t : shards()) {
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->tree_minimum();
        if (!r.is_leaf) continue;
        auto cur = r.peek_leaf()->get_key();
        if (the_min.empty() || cur < the_min) {
            the_min = cur;
        }
    }
    if (the_min.empty()) return false;
    cb(the_min); // still under the locks above, which is what keeps the_min alive
    return true;
}

bool sharded_store::maximum_below(art::value_type bound, const key_cb& cb) const {
    // the candidate has to be copied: key() points into the page the iterator is
    // reading, and both moving it and looking at the next shard can take that away
    std::string best;
    bool found = false;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    auto consider = [&](const shard_ptr& t) -> bool {
        if (!t->get_tree_size()) return false;
        art::iterator it(t, bound);
        art::value_type cur;
        if (it.ok()) {
            // the constructor is a lower bound, so this landed on or past the bound
            // and the one before it is what we want. A shard holding nothing but keys
            // at or past the bound has no answer, which is what a false previous says
            if (!it.previous() || !it.ok()) return false;
            cur = it.key();
        } else {
            // nothing here is at or past the bound, so this shard's last key is the
            // candidate. This is the only caller of last() that a thinly spread space
            // exercises, so it is also what keeps TODO 138 fixed
            if (!it.last() || !it.ok()) return false;
            cur = it.key();
        }
        if (!(cur < bound)) return false;
        if (!found || art::value_type{best} < cur) {
            best.assign(cur.chars(), cur.size);
            found = true;
        }
        return true;
    };
    if (ordered_shards()) {
        // shards hold contiguous spans in order, so the first one from the top with
        // anything below the bound holds the largest
        const auto& all = shards();
        for (size_t s = all.size(); s-- > 0;) {
            if (consider(all[s])) break;
        }
    } else {
        for (const auto& t : shards()) {
            consider(t);
        }
    }
    if (!found) return false;
    cb(art::value_type{best});
    return true;
}

bool sharded_store::maximum(const key_cb& cb) const {
    art::value_type the_max;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    if (ordered_shards()) {
        const auto& all = shards();
        for (size_t s = all.size(); s-- > 0;) {
            if (!all[s]->get_tree_size()) continue;
            art::node_ptr r = all[s]->tree_maximum();
            if (!r.is_leaf) continue;
            cb(r.const_leaf()->get_key());
            return true;
        }
        return false;
    }
    for (const auto& t : shards()) {
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->tree_maximum();
        if (!r.is_leaf) continue;
        auto cur = r.peek_leaf()->get_key();
        if (the_max.empty() || the_max < cur) {
            the_max = cur;
        }
    }
    if (the_max.empty()) return false;
    cb(the_max);
    return true;
}

bool sharded_store::lower_bound(art::value_type key, const key_cb& cb) const {
    art::value_type the_lb;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    if (ordered_shards()) {
        // a lower bound over the boundaries, then a lower bound over one art. Nothing
        // below the shard that owns key can answer at all, so no other shard is asked.
        //
        // The owning shard can still miss, which is why there is a second step: key
        // falling inside a shard's span is not a promise that the shard holds anything
        // at or above key within it. Shard 0 holding {a, b} and shard 1 holding {m, n}
        // owns "c" and has no answer for it - "m" does. When that happens the index
        // already names where to look, because every entry in it is a shard's minimum:
        // the entry just above key is the next non empty shard, and its minimum is the
        // answer, since every key above that one is larger still.
        const auto& all = shards();
        auto table = spc->routes().get();
        size_t at = range_index::upper(*table, key);
        size_t owner = at ? (*table)[at - 1].shard : 0;
        art::node_ptr r = all[owner]->lower_bound(key);
        if (r.is_leaf) {
            cb(r.const_leaf()->get_key());
            return true;
        }
        for (size_t e = at; e < table->size(); ++e) {
            // the leaf's key rather than the boundary held next to it: a key handed to
            // cb has to be the stored form, and the table keeps boundaries with the
            // terminator already stripped so that they compare against either form
            auto m = all[(*table)[e].shard]->tree_minimum();
            if (!m.is_leaf) continue;
            cb(m.const_leaf()->get_key());
            return true;
        }
        return false;
    }
    for (const auto& t : shards()) {
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->lower_bound(key);
        if (!r.is_leaf) continue;
        auto cur = r.peek_leaf()->get_key();
        if (the_lb.empty() || cur < the_lb) {
            the_lb = cur;
        }
    }
    if (the_lb.empty()) return false;
    cb(the_lb);
    return true;
}

bool sharded_store::upper_bound(art::value_type key, const key_cb& cb) const {
    art::value_type the_ub;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    if (ordered_shards()) {
        // the same two steps as lower_bound above, and the same reason for the second
        // one. The boundaries the index holds are strictly above key, so a shard reached
        // that way needs no equal key skipped - only the owning shard does
        const auto& all = shards();
        auto table = spc->routes().get();
        size_t at = range_index::upper(*table, key);
        size_t owner = at ? (*table)[at - 1].shard : 0;
        art::iterator ilb(all[owner], key);
        if (ilb.ok() && ilb.key() == key) {
            ilb.next();
        }
        if (ilb.ok()) {
            cb(ilb.key());
            return true;
        }
        for (size_t e = at; e < table->size(); ++e) {
            auto m = all[(*table)[e].shard]->tree_minimum();
            if (!m.is_leaf) continue;
            cb(m.const_leaf()->get_key());
            return true;
        }
        return false;
    }
    for (const auto& t : shards()) {
        if (!t->get_tree_size()) continue;
        art::iterator ilb(t, key);
        if (ilb.ok() && ilb.key() == key) {
            ilb.next();
        }
        if (ilb.ok()) {
            if (the_ub.empty() || ilb.key() < the_ub) {
                the_ub = ilb.key();
            }
        }
    }
    if (the_ub.empty()) return false;
    cb(the_ub);
    return true;
}

int64_t sharded_store::count(art::value_type lo, art::value_type hi) const {
    int64_t total = 0;
    // locks per shard rather than over the whole space: this only measures a distance
    // between two iterators and never hands a key back, so it does not need the space
    // to hold still as a whole
    auto space_count = [&lo, &hi](const key_space_ptr& spce, size_t shard) -> int64_t {
        if (!spce) return 0;
        int64_t count = 0;
        auto t = spce->get(shard);
        read_lock release(t);

        art::iterator i(t, lo);
        art::iterator j(t, hi);
        if (i.ok() && !j.ok()) {
            j.last(); // last key in the range
            ++count;
        }
        if (i.ok() && j.ok()) {
            count += i.fast_distance(j);
        }
        return count;
    };
    if (ordered_shards()) {
        // only the shards whose spans overlap [lo, hi) can contribute, and they are
        // consecutive: the one that owns lo through the one that owns hi. Both ends come
        // out of the routing table, so this needs no lock and no tree walk to decide
        // which shards to open - unlike asking the shards themselves, which would be
        // reading a tree that a rebalance is entitled to be changing
        const auto& all = shards();
        auto table = spc->routes().get();
        size_t last = range_index::route(*table, hi);
        for (size_t s = range_index::route(*table, lo); s <= last && s < all.size(); ++s) {
            total += space_count(spc, s);
        }
        return total;
    }
    for (const auto& t : shards()) {
        total += space_count(spc, t->get_shard_number());
        total += space_count(spc->source(), t->get_shard_number());
    }
    return total;
}

/*
 * Keys in [lo, x) of one tree, measured from an iterator already standing at lo.
 * Same arithmetic count() uses, including its care for a tree of one key, which
 * has no trace for fast_distance to read.
 */
static int64_t keys_before(const shard_ptr& t, const art::iterator& at_lo, art::value_type x) {
    if (!at_lo.ok())
        return 0;
    art::iterator j(t, x);
    if (!j.ok()) {
        j.last();
        return 1 + at_lo.fast_distance(j);
    }
    return at_lo.fast_distance(j);
}

/*
 * Whether a tree's node counts say what a walk of it would see. The merge drops
 * tombstones and folds in a pull source, and `descendants` knows about neither.
 * With no source, the tree size and the live size only differ by the tombstones.
 */
static bool counts_match_walk(const shard_ptr& t) {
    return !t->sources() && t->get_tree_size() == t->get_size();
}

/*
 * Where each shard's part of [lo, hi) starts once `offset` keys of the whole
 * range have gone by - the k-way version of iterator::skip. See TODO 369.
 *
 * Every shard has a window [a, b) of positions, counted from lo, that its split
 * point must lie in, starting as the whole of its part of the range. Take a key x
 * from inside the widest window and count, in every shard, the keys below it.
 * Their sum says which side of x the offset falls on, and every window shrinks to
 * that side. Hash sharding spreads keys evenly, so taking x at the point the
 * offset would land on if the spread were perfect gets close in a few rounds.
 * Whatever is left over is a short walk, which `drop` says how long to be.
 *
 * `at` comes back holding an iterator per shard at its start position.
 * @return false when the offset is past the end of the range
 */
static bool select_start(const heap::vector<shard_ptr>& trees, art::value_type lo,
                         art::value_type hi, int64_t offset,
                         heap::vector<art::iterator>& at, int64_t& drop) {
    const size_t k = trees.size();
    heap::vector<art::iterator> at_lo;
    heap::vector<int64_t> a(k, 0), b(k, 0), r(k, 0);
    at_lo.reserve(k);
    int64_t total = 0;
    for (size_t i = 0; i < k; ++i) {
        at_lo.emplace_back(trees[i], lo);
        b[i] = keys_before(trees[i], at_lo[i], hi);
        total += b[i];
    }
    if (offset >= total)
        return false;

    std::string pivot;
    // a round costs a lower bound and a count in every shard, so stop narrowing
    // once what's left to walk is about what one more round would cost
    const int64_t walk_is_cheaper = (int64_t) k * 2 + 32;
    for (int round = 0; round < 64; ++round) {
        int64_t below = 0, width = 0;
        size_t j = 0;
        for (size_t i = 0; i < k; ++i) {
            below += a[i];
            width += b[i] - a[i];
            if (b[i] - a[i] > b[j] - a[j])
                j = i;
        }
        if (width <= walk_is_cheaper)
            break;
        const int64_t w = b[j] - a[j];
        int64_t m = a[j] + (int64_t) ((double) (offset - below) / (double) width * (double) w);
        if (m < a[j]) m = a[j];
        if (m >= b[j]) m = b[j] - 1;

        art::iterator pj = at_lo[j];
        if (pj.skip(m) != m || !pj.ok())
            break;
        auto x = pj.key();
        pivot.assign(x.chars(), x.size);
        art::value_type px{pivot.data(), pivot.size()};

        int64_t sum = 0;
        for (size_t i = 0; i < k; ++i) {
            int64_t ri = i == j ? m : keys_before(trees[i], at_lo[i], px);
            ri = std::max(a[i], std::min(b[i], ri));
            r[i] = ri;
            sum += ri;
        }
        if (sum == offset) {
            // x is the key the offset lands on: every shard starts at its count below x
            a = r;
            b = r;
            break;
        }
        if (sum < offset) {
            // x and everything below it come before the offset
            a = r;
            a[j] = m + 1;
        } else {
            // the offset lands below x
            b = r;
        }
    }

    int64_t start = 0;
    at.clear();
    at.reserve(k);
    for (size_t i = 0; i < k; ++i) {
        art::iterator it = at_lo[i];
        if (a[i] > 0)
            it.skip(a[i]);
        at.push_back(std::move(it));
        start += a[i];
    }
    drop = offset - start;
    return true;
}

void sharded_store::range(art::value_type lo, art::value_type hi, int64_t limit,
                          const key_cb& cb, int64_t offset) const {
    ks_shared kss(spc->source());
    ks_shared ksl(spc);

    if (ordered_shards()) {
        // this is most of the reason for ordering the shards in the first place.
        //
        // Because the shards are a partition of the key order, the keys come out sorted
        // from walking the shards that overlap [lo, hi) in shard number order and each
        // of those in key order. There is no striation, no merge, nothing collected and
        // nothing sorted: a key is handed to cb as it is found. The walk begins at the
        // shard that owns lo, because nothing below it can hold a key at or above lo,
        // and stops at the first key not below hi, because nothing after that one is
        // either.
        const auto& all = shards();
        auto table = spc->routes().get();
        size_t last = range_index::route(*table, hi);
        for (size_t s = range_index::route(*table, lo); s <= last && s < all.size(); ++s) {
            art::iterator i(all[s], lo);
            if (offset > 0) {
                // a whole shard's worth of the range is skipped by its count, and the
                // shard the offset lands in is entered with a skip. This walk counts
                // every leaf, tombstones too, and so do the node counts, so it's exact
                const int64_t here = keys_before(all[s], i, hi);
                if (offset >= here) {
                    offset -= here;
                    continue;
                }
                i.skip(offset);
                offset = 0;
            }
            for (; i.ok(); i.next()) {
                auto k = i.key();
                if (!(k < hi)) return;
                if (k < lo) continue;
                cb(k);
                if (--limit == 0) return;
            }
        }
        return;
    }

    /*
     * A k-way merge across the shards - see TODO 323.
     *
     * Every shard is ordered, so the smallest unseen key overall is the smallest
     * of the shards' current keys: take it, hand it out, advance that shard. A
     * limit costs exactly that many steps, nothing is collected and nothing is
     * sorted.
     *
     * What was here before walked all the shards in lockstep in "striations" -
     * one pass took the next key from each shard - collected the lot unsorted,
     * sorted it at the end and handed out the first `limit`. The invariant it
     * claimed was that after N passes the N globally smallest keys must have been
     * seen, which is true. What it actually broke on was the optimisation on top:
     * the pass counter was jumped forward to however many keys had been collected
     * whenever a key beat everything seen so far, and collecting 347 keys (one
     * per shard) is not the same as holding the 347 smallest. So it stopped
     * early, and `RANGE` over a thousand keys with a limit of 255 came back with
     * 255 keys in order, no duplicates, and four of them missing - filled up from
     * beyond the window instead, which is what made it quiet. It had collected
     * 786 of the 1,000 keys and four of the first 255 were not among them.
     *
     * The heap holds one entry per shard that has a key in range, ordered by that
     * key. The keys are views into leaves and stay valid because a shard is only
     * advanced after it has been popped, and re-pushed with its new key before
     * anything else looks at it.
     */
    heap::vector<art::merge_iterator> iters;
    iters.reserve(shards().size());
    // keys the merge still has to throw away before handing any out
    int64_t drop = offset > 0 ? offset : 0;
    bool placed = false;
    if (drop > 0) {
        bool countable = true;
        for (const auto& t : shards()) {
            if (!counts_match_walk(t)) {
                countable = false;
                break;
            }
        }
        if (countable) {
            heap::vector<art::iterator> at;
            if (!select_start(shards(), lo, hi, drop, at, drop))
                return;
            for (auto& it : at)
                iters.emplace_back(heap::vector<art::iterator>{std::move(it)});
            placed = true;
        }
    }
    if (!placed) {
        for (const auto& t : shards()) {
            iters.emplace_back(make_merged(t, lo));
        }
    }

    /** move this shard to its next key in [lo, hi), false if it has none left */
    auto ready = [&](size_t i) -> bool {
        auto& it = iters[i];
        while (it.ok()) {
            if (it.current().peek_leaf()->is_tomb()) {
                it.next();
                continue;
            }
            auto k = it.key();
            if (k < lo) {
                it.next();
                continue;
            }
            // ordered, so the first key at or past hi ends this shard
            return k < hi;
        }
        return false;
    };
    // a min-heap, which std::*_heap builds from a greater-than comparison
    auto greater = [&](size_t a, size_t b) { return iters[b].key() < iters[a].key(); };

    heap::vector<size_t> pending;
    pending.reserve(iters.size());
    for (size_t i = 0; i < iters.size(); ++i) {
        if (ready(i))
            pending.push_back(i);
    }
    std::make_heap(pending.begin(), pending.end(), greater);

    while (!pending.empty()) {
        std::pop_heap(pending.begin(), pending.end(), greater);
        const size_t i = pending.back();
        pending.pop_back();
        if (drop > 0) {
            --drop;
        } else {
            cb(iters[i].key());
            // 0 or less means no limit, which is what the striation walk did too
            if (limit > 0 && --limit == 0)
                return;
        }
        iters[i].next();
        if (ready(i)) {
            pending.push_back(i);
            std::push_heap(pending.begin(), pending.end(), greater);
        }
    }
}

// ---- scan ----

bool sharded_store::open_scan(scan_cursor& cursor) const {
    cursor.space = spc;
    for (size_t i = 0; i < spc->get_shard_count(); ++i) {
        auto s = spc->get(i);
        cursor.shards.push_back(s);
        if (s->sources()) {
            cursor.shards.push_back(s->sources());
        }
    }
    return !(spc->source() && cursor.shards.size() != 2 * spc->get_shard_count());
}

/**
 * walk what is left of the page the cursor is sitting on.
 * @return false if cb asked to stop
 */
static bool scan_page(barch::scan_cursor& cursor, const barch::shard_ptr& shard,
                      const art::scan_spec& spec, const sharded_store::scan_cb& cb) {
    if (!shard) throw_exception<std::runtime_error>("null shard");

    barch::shard_ptr dest = cursor.space->get(shard->get_shard_number());
    bool is_source = shard == dest->sources();
    read_lock release(is_source ? dest : nullptr);
    if (is_source && !dest) throw_exception<std::runtime_error>("scan_page: dest not found");

    bool keep_going = true;

    // a key from a pull source is only emitted when the shard shadowing it does not
    // have one of its own, or the scan would report it twice
    auto emit = [&](const art::leaf* l) {
        if (is_source) {
            if (!dest->is_present(l->get_key())) {
                keep_going = cb(l->get_key());
            }
        } else {
            keep_going = cb(l->get_key());
        }
    };

    // the position advances past every leaf, matched or not, so a resumed scan does
    // not re-examine what this pass already rejected
    auto advance = [&](const art::leaf* l, uint32_t pos) {
        cursor.pos = pos + l->next_leaf();
        return keep_going;
    };

    if (cursor.pos < cursor.bytes && cursor.page > 0) {
        if (spec.is_match) {
            std::string tmp;
            art::page_iterator_ptr(cursor.buffer.data(), cursor.buffer.size(),
                [&](const art::leaf *l, uint32_t pos) -> bool {
                    if (l->is_tomb() || l->expired()) return true;
                    art::value_type td;
                    if (art::tstring == *l->key()) {
                        td = l->get_clean_key();
                        // get_clean_key steps over the leading type byte but keeps the
                        // stored length, so the trailing terminator is still on the end -
                        // leaving it there stops any pattern anchored at the end of the
                        // key from matching
                        if (td.size) --td.size;
                    } else {
                        // matched by the name the container belongs to, decoded rather
                        // than sliced - see DONE 62
                        tmp = encoded_container_name(l->get_key());
                        if (tmp.empty()) {
                            if (art::is_container_lead(*l->get_key().bytes)) {
                                return advance(l, pos);   // the member index
                            }
                            tmp = encoded_key_as_string(l->get_key());
                        }
                        td = tmp;
                    }

                    if (1 == glob::stringmatchlen(spec.glob_expr, td, 0)) {
                        emit(l);
                    }
                    return advance(l, pos);
                }, cursor.pos);
        } else {
            art::page_iterator_ptr(cursor.buffer.data(), cursor.buffer.size(),
                [&](const art::leaf *l, uint32_t pos) -> bool {
                    if (l->is_tomb() || l->expired()) return true;
                    emit(l);
                    return advance(l, pos);
                }, cursor.pos);
        }
    }
    return keep_going;
}

bool sharded_store::scan(scan_cursor& cursor, const art::scan_spec& spec, const scan_cb& cb) const {
    while (!cursor.shards.empty()) {
        auto t = cursor.shards.back();

        if (t->get_size() == 0) {
            cursor.page = 0;
            cursor.shards.pop_back();
            continue;
        }

        cursor.shard = t->get_shard_number();
        do {
            if (cursor.bytes == 0) {
                cursor.buffer.clear();
                read_lock release(t);
                cursor.bytes = t->page(cursor.page, cursor.buffer);
                cursor.pos = 0;
            }
            if (!scan_page(cursor, t, spec, cb)) {
                // TODO: landing exactly on the limit with no pages left still costs one
                // more call to discover there is nothing further
                return false;
            }
            cursor.page = t->next_page(cursor.page);
            cursor.pos = 0;
            cursor.bytes = 0;
            cursor.buffer.clear();
        } while (cursor.page > 0);

        cursor.shards.pop_back();
    }
    return true;
}

void sharded_store::glob(const art::keys_spec& spec, art::value_type pattern, bool by_value,
                         const leaf_cb& cb, const glob_pages *only, glob_pages *hits) const {
    // deliberately unlocked - each shard copies the page it is matching into a working
    // buffer first, so a leaf is only valid inside cb, and cb runs on worker threads
    auto all = shards();
    if (hits) {
        hits->clear();
        hits->resize(all.size());
    }
    static const art::glob_page_list none{};
    for (size_t i = 0; i < all.size(); ++i) {
        const art::glob_page_list *shard_only = nullptr;
        if (only)
            shard_only = (i < only->size()) ? &(*only)[i] : &none;
        art::glob_page_list *shard_hits = hits ? &(*hits)[i] : nullptr;
        all[i]->glob(spec, pattern, by_value, cb, shard_only, shard_hits);
    }
}

}
