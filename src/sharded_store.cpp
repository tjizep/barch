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
        auto cur = r.const_leaf()->get_key();
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
        auto cur = r.const_leaf()->get_key();
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
        auto cur = r.const_leaf()->get_key();
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

void sharded_store::range(art::value_type lo, art::value_type hi, int64_t limit,
                          const key_cb& cb) const {
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
            for (art::iterator i(all[s], lo); i.ok(); i.next()) {
                auto k = i.key();
                if (!(k < hi)) return;
                if (k < lo) continue;
                cb(k);
                if (--limit == 0) return;
            }
        }
        return;
    }

    // walk every shard in lockstep, in 'striations': one pass takes the next key from
    // each shard, so after N passes we are certain we have seen the globally smallest
    // N keys and can stop early even though the collected list is unsorted
    auto collect = [&]() -> heap::std_vector<art::value_type> {
        int64_t striation_counter = 0;
        heap::std_vector<art::value_type> usorted;
        heap::vector<art::merge_iterator> iters;
        heap::unordered_set<size_t> active;
        for (const auto& t : shards()) {
            auto i = make_merged(t, lo);
            if (i.ok()) {
                active.insert(t->get_shard_number());
            }
            iters.push_back(i);
        }
        art::value_type list_max;
        while (!active.empty()) {
            bool has_first = false; // key in striation
            for (auto shard : active) {
                auto& i = iters[shard];
                if (i.current().cl()->is_tomb()) {
                    if (!i.next()) {
                        active.erase(shard);
                    }
                } else {
                    auto k = i.key();
                    if (k >= lo && k < hi) {
                        if (k > list_max) {
                            if (!has_first) {
                                // may or may not advance the counter - an optimisation
                                // to reach a correct result sooner
                                striation_counter = std::max<int64_t>(usorted.size(), striation_counter);
                                has_first = true;
                            }
                            list_max = k;
                        }
                        usorted.push_back(k);
                        if (!i.next()) {
                            active.erase(shard);
                        }
                    } else {
                        active.erase(shard);
                    }
                }
            }

            if (limit > 0 && striation_counter >= limit) {
                // certain the list contains the globally first limit entries, although
                // it is at most limit*shard_count large
                break;
            }
            ++striation_counter;
        }
        return usorted;
    };
    auto sorted = collect();
    std::sort(sorted.begin(), sorted.end()); // the sort must happen inside the lock
    for (auto& k : sorted) {
        cb(k);
        if (--limit == 0) break;
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
