//
// Created by teejip on 8/1/26.
//
// The sharding layer. Every algorithm here was previously open coded inside whichever
// command needed it - see the class comment in sharded_store.h for why they moved.
//

#include "rpc/server.h"
#include "sharded_store.h"
#include "dictionary_compressor.h"
#include "abstract_shard.h"

#include <algorithm>
#include <optional>

#include "art/iterator.h"
#include "tx_reads.h"
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

uint64_t visible_keys_holding_lock(const shard_ptr& t) {
    const uint64_t all = t->get_size();
    if (!t->opt_ordered_keys)
        return all;
    // meta keys sort above every other key, so they're the one range [13, 14).
    // Two bytes each: a key of one is refused as too short, and no stored key
    // has a zero byte inside it, so 1 comes before anything that follows the lead
    const char lo_b[2] = {(char) art::tmeta, 1}, hi_b[2] = {(char) (art::tmeta + 1), 1};
    art::iterator i(t, art::value_type{lo_b, 2});
    if (!i.ok())
        return all;
    int64_t meta = 0;
    art::iterator j(t, art::value_type{hi_b, 2});
    if (!j.ok()) {
        j.last();
        ++meta;
    }
    meta += i.fast_distance(j);
    return meta > 0 && (uint64_t) meta <= all ? all - (uint64_t) meta : all;
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

/*
 * The same question from inside the two lock types, which only have the shard. Which
 * space it's in is answered by the held space itself: shard numbers wrap in
 * key_space::get_ref, so a shard of another space never comes back as itself. See
 * TODO 497.
 */
bool region_holds(const abstract_shard* t) {
    const auto& held = shard_hold::current();
    if (!held.space || !t)
        return false;
    // shard_hold keeps the space untyped; it's always a key_space, set in s.locked
    auto* space = static_cast<key_space*>(const_cast<void*>(held.space));
    if (space->get_ref(t->get_shard_number()) != t)
        return false;               // another space, locked the ordinary way
    return shard_already_held(held.space, t);
}

/*
 * A shard and everything it depends on, the shard itself first - TODO 701. A chain
 * deeper than one (c on b on a) is what a transaction's version stack is.
 */
static heap::vector<shard_ptr> chain_of(const shard_ptr& shard) {
    heap::vector<shard_ptr> out;
    for (auto s = shard; s; s = s->sources())
        out.push_back(s);
    return out;
}

/*
 * A shard's levels for a walk: its chain, and inside a transaction the snapshot's layers
 * between the overlay and the space it reads through to, oldest first - TODO 701. The
 * overlay's leaf wins, then the oldest layer's, which is the key as the snapshot saw it,
 * then the space's. The walk holds the space's shards, and a layer shard is only written
 * by a writer of the space's shard with its number, so the layers hold still too.
 */
static heap::vector<shard_ptr> levels_of(const shard_ptr& shard) {
    auto chain = chain_of(shard);
    auto* log = barch::tx_reading();
    if (!log || chain.size() < 2)
        return chain;
    heap::vector<shard_ptr> layers;
    log->layers_under(shard.get(), layers);
    if (layers.empty())
        return chain;
    heap::vector<shard_ptr> out;
    out.reserve(chain.size() + layers.size());
    out.push_back(chain.front());
    for (auto& l : layers)
        out.push_back(std::move(l));
    for (size_t i = 1; i < chain.size(); ++i)
        out.push_back(chain[i]);
    return out;
}

/*
 * One iterator over a shard and its whole chain, in key order. On equal keys the
 * merge hands out the first iterator's, so the shard nearest the top wins, a tomb
 * there included, which the walks skip - TODO 701. It used to take one level, so a
 * walk of c on b on a never saw a.
 */
art::merge_iterator make_merged(const shard_ptr& shard, art::value_type lower) {
    heap::vector<art::iterator> each;
    for (const auto& s : levels_of(shard))
        each.emplace_back(s, lower);
    return art::merge_iterator(each);
}

/** keys in [lo, hi) a client sees through a shard's chain: each once, no tombs - TODO 701 */
static int64_t merged_count(const shard_ptr& t, art::value_type lo, art::value_type hi, bool skip_meta) {
    int64_t n = 0;
    // no lower bound: from the chain's smallest key. art::iterator(t) doesn't stand on
    // the first key (it finds the minimum but never fills the trace it walks from)
    std::string first;
    if (lo.empty()) {
        // the smallest of every level's, a layer's included - TODO 701
        bool any = false;
        for (const auto& s : levels_of(t)) {
            auto m = s->tree_minimum();
            if (!m.is_leaf)
                continue;
            const auto k = m.const_leaf()->get_key();
            if (!any || k < art::value_type{first.data(), first.size()}) {
                first.assign(k.chars(), k.size);
                any = true;
            }
        }
        if (!any)
            return 0;
        lo = art::value_type{first.data(), first.size()};
    }
    for (auto m = make_merged(t, lo); m.ok(); m.next()) {
        const auto k = m.key();
        if (!hi.empty() && !(k < hi))
            break;
        if (m.current().peek_leaf()->is_tomb())
            continue;
        if (skip_meta && k.size && art::is_meta_lead(k.bytes[0]))
            continue;
        ++n;
    }
    return n;
}

/*
 * The smallest and largest key a client sees through a shard's chain - TODO 701: the
 * top level holding a key decides, and a tomb there means it isn't there. min walks the
 * merge up from the smallest key of any level; max steps down, each time to the largest
 * key below the last one any level has, until the level that wins it holds no tomb.
 */
static bool chain_minimum(const shard_ptr& t, std::string& out) {
    const auto levels = levels_of(t);
    std::string first;
    bool any = false;
    for (const auto& s : levels) {
        auto m = s->tree_minimum();
        if (!m.is_leaf)
            continue;
        const auto k = m.const_leaf()->get_key();
        if (!any || k < art::value_type{first.data(), first.size()}) {
            first.assign(k.chars(), k.size);
            any = true;
        }
    }
    if (!any)
        return false;
    heap::vector<art::iterator> each;
    for (const auto& s : levels)
        each.emplace_back(s, art::value_type{first.data(), first.size()});
    for (art::merge_iterator m(each); m.ok(); m.next()) {
        if (m.current().peek_leaf()->is_tomb())
            continue;
        const auto k = m.key();
        out.assign(k.chars(), k.size);
        return true;
    }
    return false;
}

static bool chain_maximum(const shard_ptr& t, std::string& out) {
    const auto levels = levels_of(t);
    std::string bound;
    bool bounded = false;
    for (;;) {
        std::string best;
        bool found = false, tomb = false;
        for (const auto& s : levels) {
            if (!s->get_tree_size())
                continue;
            art::node_ptr n;
            if (!bounded) {
                n = s->tree_maximum();
            } else {
                // a lower bound, so the one before it is the largest below the bound
                art::iterator it(s, art::value_type{bound.data(), bound.size()});
                if (it.ok() ? (!it.previous() || !it.ok()) : (!it.last() || !it.ok()))
                    continue;
                n = it.current();
            }
            if (!n.is_leaf)
                continue;
            const auto k = n.const_leaf()->get_key();
            if (bounded && !(k < art::value_type{bound.data(), bound.size()}))
                continue;
            // only a larger key replaces it, so on equal keys the top level's stays
            if (!found || art::value_type{best.data(), best.size()} < k) {
                best.assign(k.chars(), k.size);
                tomb = n.const_leaf()->is_tomb();
                found = true;
            }
        }
        if (!found)
            return false;
        if (!tomb) {
            out = std::move(best);
            return true;
        }
        bound = std::move(best);
        bounded = true;
    }
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
    /*
     * A shard over a source (DEPENDS, a transaction's overlay) - TODO 707: a tomb here
     * is a key deleted, not a cached miss, and a key it doesn't hold is read from the
     * source, through shard::search, which tells a transaction's read log. This read
     * only ever looked at the shard itself, so barch.store.get on a dependent space
     * didn't see what was beneath it.
     */
    if (t->sources()) {
        if (!r.null()) {
            auto mine = r.const_leaf();
            if (mine->is_tomb() || mine->expired())
                return read_state::absent;
            cb(r);
            return read_state::present;
        }
        r = t->search(key);
        if (r.null())
            return read_state::absent;
        cb(r);
        return read_state::present;
    }
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

namespace {
    /*
     * A single-key write to a replicated space, under way - TODO 632. Its key is
     * held pending in its shard, so a second write to it waits, and the shard's
     * raft_commit lets the latch go while the entry commits (see abstract_shard).
     * Made before the latch is taken and gone after it's let go: declared ahead of
     * the storage_release, it's destroyed after it. Not for the commit thread's
     * own applies, nor inside a region, which holds the latch for others.
     */
    struct narrowed_write {
        barch::shard_ptr t{};
        std::string key{};
        narrowed_write(const barch::shard_ptr& shard, art::value_type k) {
            if (!shard || !shard->raft || repl::applying_now() || barch::region_holds(shard.get()))
                return;
            key.assign(k.chars(), k.size);
            shard->narrow_begin(key);
            t = shard;
            barch::cluster::set_narrowing(t.get());
        }
        ~narrowed_write() {
            if (!t) return;
            barch::cluster::set_narrowing(nullptr);
            t->narrow_end(key);
        }
        narrowed_write(const narrowed_write&) = delete;
        narrowed_write& operator=(const narrowed_write&) = delete;
    };
}

bool sharded_store::insert(const art::key_options& opts, art::value_type key,
                           art::value_type value, bool update, const art::NodeResult& fc) {
    narrowed_write narrowed(shard_for(key), key);
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->opt_insert(opts, key, value, update, fc);
}

bool sharded_store::add(const art::key_options& opts, art::value_type key,
                        art::value_type value, const art::NodeResult& fc) {
    narrowed_write narrowed(shard_for(key), key);
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->insert(opts, key, value, false, fc);
}

bool sharded_store::remove(art::value_type key, const art::NodeResult& fc) {
    narrowed_write narrowed(shard_for(key), key);
    std::optional<storage_release> release;
    auto t = route_locked<storage_release>(*this, key, release);
    if (!t) return false;
    return t->remove(key, fc);
}

bool sharded_store::update(art::value_type key, const updater_fn& updater) {
    narrowed_write narrowed(shard_for(key), key);
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
        // two shards changed as one: neither may save without the other now -
        // TODO 519. Counted under both latches, which is what a save checks under
        spc->note_cross_shard_write();
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

int64_t sharded_store::visible_size() const {
    int64_t size = 0;
    // get_size only reads counters, and read_lock still takes the source chain shared,
    // which get_size recurses into
    each_shard_read([&](const shard_ptr& t) {
        // the keys a client can see, meta keys left out - TODO 548. Through a chain the
        // counters add up every level, shadowed keys and tombs too, so the walk counts
        // instead - TODO 701; a space with no source still only reads counters
        if (t->sources() && t->opt_ordered_keys)
            size += merged_count(t, {}, {}, true);
        else
            size += (int64_t) visible_keys_holding_lock(t);
    });
    return size + (int64_t) space()->hash_buf_size();
}

void sharded_store::each_shard_parallel(const shard_fn& fn) const {
    // shard_thread_processor caps how many run at once. one thread per shard would be
    // 347 of them on a default space, which is what LOAD and RELOAD used to do
    const auto& all = shards();
    shard_thread_processor(all.size(), [&all, &fn](size_t i) {
        fn(all[i]);
    });
}

bool sharded_store::freeze_space(uint64_t* mark, uint64_t* crossed) const {
    for (;;) {
        shard_ptr busy;
        bool in_transaction = false;
        bool run_under_way = false;
        {
            write_guard all = lock_space_write();
            /*
             * Not partway through a run from a Raft log - TODO 673. One that starts
             * after this can't write until the shards are let go, so the freeze holds
             * none of it. One already under way is waited for below, with nothing held.
             */
            run_under_way = space()->log_run_under_way();
            if (!run_under_way) {
                each_shard([&](const shard_ptr& t) {
                    if (!busy && t->frozen_for_save()) busy = t;
                    if (t->in_transaction()) in_transaction = true;
                });
            }
            if (in_transaction)
                return false;
            if (!run_under_way && !busy) {
                if (mark)
                    *mark = space()->get_change_log() ? space()->get_change_log()->mark() : 0;
                // after this lock's own count, since taking it counted one
                if (crossed && space()->cross_shard_writes())
                    *crossed = space()->cross_shard_writes()->writes.load(std::memory_order_relaxed);
                each_shard([&](const shard_ptr& t) {
                    // nothing else is frozen and nothing is in a transaction,
                    // so every one of these freezes
                    if (t->freeze_for_save_holding_lock(true) != abstract_shard::save_freeze::frozen)
                        abort_with("a shard would not freeze under the space lock");
                });
                return true;
            }
        }
        if (run_under_way) {
            space()->wait_log_runs();
            continue;
        }
        // another save's freeze; it merges when its files are written
        busy->wait_for_frozen_save();
    }
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

    /*
     * A hash sharded space saves a shard at a time while nothing has written to
     * two shards as one since its last whole save - TODO 519. After one has, a
     * shard at a time could put one side of it in the files and not the other,
     * so it saves the way a stateful space does.
     */
    const auto& crossing = store.space()->cross_shard_writes();
    const auto crossed = [&]() { return crossing && crossing->unsaved(); };
    // the count the files will hold once every shard is written, when they're one moment
    uint64_t whole_at = 0;
    bool whole = store.space()->is_stateful_sharding() || crossed();
    if (!whole) {
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
        if (crossed()) {
            /*
             * One came in while the shards were saving. Every shard that saved
             * after it refused (shard::save checks under its latch), and the
             * ones before it saved what was there before it, so what's written
             * is all from before. The whole save below goes over all of them,
             * and the refusals weren't failures.
             */
            errors = 0;
            whole = true;
        }
    }
    if (whole) {
        /*
         * A stateful method moves keys between shards, so the files have to be
         * one moment or a key can land in two of them or in neither. Every shard
         * is frozen under one space write lock - a short one, for the CoW maps and
         * the state that isn't pages - and the files are written after it's let
         * go, while writes carry on into the CoW pages - TODO 465. Under that
         * lock no write is half done, so the log mark is exactly what the files
         * hold.
         */
        const bool in_transaction = !store.freeze_space(save_log ? &saved_through : nullptr, &whole_at);
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
            // no write of any shard can run under this, cross shard or not
            if (crossing)
                whole_at = crossing->writes.load(std::memory_order_relaxed);
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
        // the files are one moment up to `whole_at`, so a shard may save alone
        // again until the next write that changes two - TODO 519. Never
        // backwards: a save that started earlier can finish later
        if (whole && crossing) {
            uint64_t was = crossing->saved.load(std::memory_order_relaxed);
            while (was < whole_at
                   && !crossing->saved.compare_exchange_weak(was, whole_at, std::memory_order_relaxed)) {
            }
        }
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
    /*
     * A Raft space - TODO 610. A cleared space can't be put back, so the record
     * goes to the group first and the space is cleared once it has committed.
     * Applying the record on the other members comes back through here with
     * repl::applying set, and clears directly.
     */
    if (!repl::applying_now() && space()->raft_groups().size() > 1) {
        // one record can't clear a space whose shards commit in different groups,
        // all or nothing - TODO 615
        throw_exception<std::runtime_error>(
            "ERR a space split over several raft groups can't be cleared in one step");
    }
    if (auto raft = space()->get_raft(); raft && !repl::applying_now()) {
        aof::record r;
        r.type = aof::record_type::clear;
        r.space = space()->space_name();
        r.shard_count = (uint32_t) shards().size();
        std::vector<uint8_t> encoded;
        aof::encode(r, encoded);
        std::string why;
        if (raft->commit(std::string((const char*) encoded.data(), encoded.size()), 0, why, true)
                != cluster::binding::result::committed)
            throw_exception<std::runtime_error>(why.c_str());
    }
    if (const auto& change_log = space()->get_change_log())
        change_log->append_clear(space()->space_name(), (uint32_t) shards().size());
    each_shard([](const shard_ptr& shard) { shard->clear_holding_lock(); });
    // the dictionary key went with everything else - TODO 527
    dictionary::forget(space()->get_name());
    if (repl::capturing()) {
        // the whole space at this point in the writes, as the log has it - TODO 498
        aof::record r;
        r.type = aof::record_type::clear;
        r.space = space()->space_name();
        r.shard_count = (uint32_t) shards().size();
        std::vector<uint8_t> encoded;
        aof::encode(r, encoded);
        repl::record(r.space, std::string((const char*) encoded.data(), encoded.size()));
    } else {
        repl::note_unpublished();           // TODO 505
    }
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

/**
 * Shared locks on a run of an ordered space's shards, held until the read is done -
 * TODO 703.
 *
 * The ordered reads below used to hold every shard of the space to read a few of them,
 * so a small RANGE waited on a write anywhere in it. The run is enough because of the
 * rule in range_index.h: a boundary only moves while both shards beside it are write
 * locked. So with shards a..s held:
 *
 *  - nothing below a holds a key at or past a's boundary, and that boundary can't move.
 *    If a fresh route of lo still says a, nothing below a is needed;
 *  - nothing above s holds a key below the first boundary above s, and that can't move
 *    down either. A fresh route of hi says whether the walk has to take s + 1.
 *
 * Empty shards inside the run are taken too. They have no boundary in the table, and a
 * rebalance can fill one while the walk is below it.
 *
 * Shards are only ever taken upwards, the order every lock over more than one shard of a
 * space is taken in (keyspace_locks.h). A read that looks downwards, maximum and
 * maximum_below, widens its run by letting go and taking a longer one.
 */
class ordered_run {
public:
    explicit ordered_run(const sharded_store& store) : store(store), all(store.shards()) {}

    /** shards a..b and nothing else. false, holding nothing, if the space isn't ordered now */
    bool take(size_t a, size_t b) {
        release();
        for (size_t s = a; s <= b && s < all.size(); ++s)
            held.emplace_back(all[s]);
        first = a;
        last = std::min(b, all.size() - 1);
        // a DEPENDS waits for every shard, so once one is held the source can't change
        if (!store.ordered_shards()) {
            release();
            return false;
        }
        return true;
    }

    /** from the shard that owns lo, checked again under its lock. An empty lo is shard 0 */
    bool from(art::value_type lo) {
        for (int tries = 0;; ++tries) {
            // a route that keeps moving under a busy rebalance: shard 0 needs no check
            const size_t a = lo.empty() || tries > 8 ? 0 : range_index::route(*table(), lo);
            if (!take(a, a)) return false;
            // above a is fine too: a was emptied upwards, and the walk goes up anyway
            if (a == 0 || range_index::route(*table(), lo) >= a) return true;
        }
    }

    /** takes the next shard up if any key below hi can be in it. An empty hi is no bound */
    bool more(art::value_type hi = {}) {
        if (last + 1 >= all.size()) return false;
        if (!hi.empty() && range_index::route(*table(), hi) <= last) return false;
        held.emplace_back(all[last + 1]);
        ++last;
        return true;
    }

    [[nodiscard]] size_t bottom() const { return first; }
    [[nodiscard]] size_t top() const { return last; }
    [[nodiscard]] const shard_ptr& shard(size_t s) const { return all[s]; }
    [[nodiscard]] range_index::table_ptr table() const { return store.space()->routes().get(); }

    void release() {
        while (!held.empty()) held.pop_back();
    }
    ~ordered_run() { release(); }

private:
    const sharded_store& store;
    const heap::vector<shard_ptr>& all;
    heap::vector<read_lock> held;
    size_t first = 0, last = 0;
};

/**
 * The run for a read that looks down from the shard owning bound, or from the top shard
 * when bound is empty: that shard and a few below it, widened until `look` finds an
 * answer or the run reaches shard 0. look is asked shard by shard from the top.
 * @return false if the space stopped being ordered, and then nothing was asked
 */
static bool ordered_downwards(ordered_run& run, size_t n, art::value_type bound,
                              const std::function<bool(const shard_ptr&)>& look, bool& found) {
    found = false;
    for (size_t width = 1;; width *= 4) {
        const size_t b = bound.empty() ? n - 1 : range_index::route(*run.table(), bound);
        const size_t a = b + 1 > width ? b + 1 - width : 0;
        if (!run.take(a, b)) return false;
        // a boundary above b moved below bound before the locks: start again
        if (!bound.empty() && range_index::route(*run.table(), bound) > b) continue;
        for (size_t s = b + 1; s-- > a;) {
            if (look(run.shard(s))) {
                found = true;
                return true;
            }
        }
        if (a == 0) return true;
    }
}

bool sharded_store::minimum(const key_cb& cb) const {
    art::value_type the_min;
    // both shared, but still a pair, so it takes the same canonical order everything
    // else does rather than source-then-self by habit
    if (ordered_shards()) {
        // the smallest key in the space is the smallest key of the first shard that has
        // one - no other shard can hold anything below it. Held from shard 0 up to that
        // one, not the whole space - TODO 703
        ordered_run run(*this);
        if (run.from({})) {
            do {
                const auto& t = run.shard(run.top());
                if (!t->get_tree_size()) continue;
                art::node_ptr r = t->tree_minimum();
                if (!r.is_leaf) continue;
                cb(r.const_leaf()->get_key());
                return true;
            } while (run.more());
            return false;
        }
    }
    ks_two held(spc->source(), ks_mode::shared, spc, ks_mode::shared);
    std::string chained;
    bool any_chain = false;
    for (const auto& t : shards()) {
        if (t->sources()) {
            // through the chain, a transaction's snapshot included, held above - TODO 701
            std::string k;
            if (chain_minimum(t, k) && (!any_chain || k < chained)) {
                chained = std::move(k);
                any_chain = true;
            }
            continue;
        }
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->tree_minimum();
        if (!r.is_leaf) continue;
        auto cur = r.peek_leaf()->get_key();
        if (the_min.empty() || cur < the_min) {
            the_min = cur;
        }
    }
    if (any_chain && (the_min.empty() || art::value_type{chained.data(), chained.size()} < the_min))
        the_min = art::value_type{chained.data(), chained.size()};
    // what a transaction saw: nothing below the smallest, for its phantom check - TODO 701
    if (auto* log = barch::tx_reading()) {
        std::string past(the_min.chars(), the_min.size);
        past.push_back('\0');
        log->saw_range(spc.get(), {}, the_min.empty() ? art::value_type{} : art::value_type{past.data(), past.size()});
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
        // shards hold contiguous spans in order, so the first one down from the shard
        // owning the bound with anything below it holds the largest. Only the shards
        // looked at are held - TODO 703
        ordered_run run(*this);
        bool any = false;
        if (ordered_downwards(run, shards().size(), bound, consider, any)) {
            if (!any) return false;
            cb(art::value_type{best});
            return true;
        }
    }
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    for (const auto& t : shards()) {
        consider(t);
    }
    if (!found) return false;
    cb(art::value_type{best});
    return true;
}

bool sharded_store::maximum(const key_cb& cb) const {
    art::value_type the_max;
    if (ordered_shards()) {
        // the top shard with anything in it, holding only the shards down to it - TODO 703
        ordered_run run(*this);
        bool any = false;
        auto look = [&cb](const shard_ptr& t) -> bool {
            if (!t->get_tree_size()) return false;
            art::node_ptr r = t->tree_maximum();
            if (!r.is_leaf) return false;
            cb(r.const_leaf()->get_key());
            return true;
        };
        if (ordered_downwards(run, shards().size(), {}, look, any))
            return any;
    }
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
    std::string chained;
    bool any_chain = false;
    for (const auto& t : shards()) {
        if (t->sources()) {
            // through the chain, a transaction's snapshot included, held above - TODO 701
            std::string k;
            if (chain_maximum(t, k) && (!any_chain || chained < k)) {
                chained = std::move(k);
                any_chain = true;
            }
            continue;
        }
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->tree_maximum();
        if (!r.is_leaf) continue;
        auto cur = r.peek_leaf()->get_key();
        if (the_max.empty() || the_max < cur) {
            the_max = cur;
        }
    }
    if (any_chain && (the_max.empty() || the_max < art::value_type{chained.data(), chained.size()}))
        the_max = art::value_type{chained.data(), chained.size()};
    // what a transaction saw: nothing above the largest - TODO 701
    if (auto* log = barch::tx_reading())
        log->saw_range(spc.get(), the_max, {});
    if (the_max.empty()) return false;
    cb(the_max);
    return true;
}

bool sharded_store::lower_bound(art::value_type key, const key_cb& cb) const {
    art::value_type the_lb;
    if (ordered_shards()) {
        // a lower bound in the shard that owns key, and then up the shards from there.
        // Nothing below that shard can answer at all, so no other shard is asked or held
        // - TODO 703.
        //
        // The owning shard can still miss: key falling inside a shard's span is not a
        // promise that the shard holds anything at or above key within it. Shard 0
        // holding {a, b} and shard 1 holding {m, n} owns "c" and has no answer for it -
        // "m" does. Every key in a shard above is larger than key, so the first one up
        // with anything in it answers with its minimum.
        ordered_run run(*this);
        if (run.from(key)) {
            do {
                art::node_ptr r = run.shard(run.top())->lower_bound(key);
                if (!r.is_leaf) continue;
                cb(r.const_leaf()->get_key());
                return true;
            } while (run.more());
            return false;
        }
    }
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
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
    if (ordered_shards()) {
        // the same walk up as lower_bound above, and the same reason for it. Only a key
        // equal to key has to be skipped, and only the owning shard can hold one
        ordered_run run(*this);
        if (run.from(key)) {
            do {
                art::iterator ilb(run.shard(run.top()), key);
                if (ilb.ok() && ilb.key() == key) {
                    ilb.next();
                }
                if (!ilb.ok()) continue;
                cb(ilb.key());
                return true;
            } while (run.more());
            return false;
        }
    }
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
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
    auto count_in = [&lo, &hi](const shard_ptr& t) -> int64_t {
        int64_t count = 0;
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
    // locks per shard rather than over the whole space: this only measures a distance
    // between two iterators and never hands a key back, so it does not need the space
    // to hold still as a whole
    auto space_count = [&count_in](const key_space_ptr& spce, size_t shard) -> int64_t {
        if (!spce) return 0;
        auto t = spce->get(shard);
        read_lock release(t);
        return count_in(t);
    };
    if (ordered_shards()) {
        // only the shards whose spans overlap [lo, hi) can contribute, and they are
        // consecutive: the one that owns lo through the one that owns hi. They're held
        // together, as RANGE holds them, since a rebalance moving keys from a shard not
        // counted yet into one already counted would otherwise lose them - TODO 703
        ordered_run run(*this);
        if (run.from(lo)) {
            do {
                total += count_in(run.shard(run.top()));
            } while (run.more(hi));
            return total;
        }
    }
    if (auto* log = barch::tx_reading())
        log->saw_range(spc.get(), lo, hi);         // for COMMITTX's phantom check - TODO 701
    for (const auto& t : shards()) {
        if (t->sources()) {
            // each key once through the chain, as RANGE sees it - TODO 701
            read_lock release(t);
            total += merged_count(t, lo, hi, false);
            continue;
        }
        total += space_count(spc, t->get_shard_number());
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
    if (ordered_shards()) {
        // this is most of the reason for ordering the shards in the first place.
        //
        // Because the shards are a partition of the key order, the keys come out sorted
        // from walking the shards that overlap [lo, hi) in shard number order and each
        // of those in key order. There is no striation, no merge, nothing collected and
        // nothing sorted: a key is handed to cb as it is found. The walk begins at the
        // shard that owns lo, because nothing below it can hold a key at or above lo,
        // and stops at the first key not below hi, because nothing after that one is
        // either. Those shards are the only ones it holds, each taken as the walk gets to
        // it, so a write elsewhere in the space doesn't hold it up - TODO 703.
        ordered_run run(*this);
        if (run.from(lo)) {
            do {
                const auto& t = run.shard(run.top());
                art::iterator i(t, lo);
                if (offset > 0) {
                    // a whole shard's worth of the range is skipped by its count, and the
                    // shard the offset lands in is entered with a skip. This walk counts
                    // every leaf, tombstones too, and so do the node counts, so it's exact
                    const int64_t here = keys_before(t, i, hi);
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
            } while (run.more(hi));
            return;
        }
    }
    ks_shared kss(spc->source());
    ks_shared ksl(spc);

    /*
     * A k-way merge across the shards and their versions - TODO 323, 701.
     *
     * Every shard is ordered, so the smallest unseen key overall is the smallest
     * of the shards' current keys: take it, hand it out, advance that shard. A
     * limit costs exactly that many steps, nothing is collected and nothing is
     * sorted.
     *
     * What was here before TODO 323 walked all the shards in lockstep in "striations" -
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
     * One merge_iterator does all of it now, over one iterator per shard per level of
     * its chain: every shard's top first, then the level beneath, down to the bottom.
     * A key lives in one shard, so equal keys only come from levels of one shard, and
     * the merge hands out the lowest numbered, the top. A tomb there is a deleted key,
     * skipped here. It used to be a heap across shards of one merge per shard's chain,
     * each of which compared every level at every step.
     */
    if (hi.empty())
        return;                 // nothing is below an empty upper bound
    heap::vector<art::iterator> each;
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
            if (!select_start(shards(), lo, hi, drop, each, drop))
                return;
            placed = true;
        }
    }
    if (!placed) {
        heap::vector<heap::vector<shard_ptr>> chains;
        chains.reserve(shards().size());
        size_t depth = 0;
        for (const auto& t : shards()) {
            chains.push_back(levels_of(t));
            depth = std::max(depth, chains.back().size());
        }
        each.reserve(chains.size() * depth);
        for (size_t level = 0; level < depth; ++level)
            for (const auto& chain : chains)
                if (level < chain.size())
                    each.emplace_back(chain[level], lo);
    }
    // what a transaction walked, for its COMMITTX to check for phantoms - TODO 701
    if (auto* log = barch::tx_reading())
        log->saw_range(spc.get(), lo, hi);
    for (art::merge_iterator m(each, hi); m.ok(); m.next()) {
        if (m.current().peek_leaf()->is_tomb())
            continue;
        const auto k = m.key();
        if (k < lo)
            continue;
        if (drop > 0) {
            --drop;
            continue;
        }
        cb(k);
        // 0 or less means no limit, which is what the striation walk did too
        if (limit > 0 && --limit == 0)
            return;
    }
}

// ---- scan ----

bool sharded_store::open_scan(scan_cursor& cursor) const {
    cursor.space = spc;
    // each shard and every level of its chain, each paged on its own, and in a transaction
    // its snapshot's layers, the whole space walked for its phantom check - TODO 701
    for (size_t i = 0; i < spc->get_shard_count(); ++i)
        for (const auto& s : levels_of(spc->get(i)))
            cursor.shards.push_back(s);
    if (auto* log = barch::tx_reading())
        log->saw_range(spc.get(), {}, {});
    return true;
}

/**
 * walk what is left of the page the cursor is sitting on.
 * @return false if cb asked to stop
 */
static bool scan_page(barch::scan_cursor& cursor, const barch::shard_ptr& shard,
                      const art::scan_spec& spec, const sharded_store::scan_cb& cb) {
    if (!shard) throw_exception<std::runtime_error>("null shard");

    barch::shard_ptr dest = cursor.space->get(shard->get_shard_number());
    if (!dest) throw_exception<std::runtime_error>("scan_page: dest not found");
    const bool is_source = shard != dest;
    // the top's lock takes its whole chain, so every level above this one holds still
    read_lock release(is_source ? dest : nullptr);
    // the levels above this one, the top first - TODO 701
    heap::vector<barch::shard_ptr> above;
    if (is_source)
        for (const auto& s : levels_of(dest)) {
            if (s == shard)
                break;
            above.push_back(s);
        }

    bool keep_going = true;

    // a key from a source is only emitted when no level above has one of its own, a
    // tomb included, or the scan would report it twice, or report a deleted one
    auto emit = [&](const art::leaf* l) {
        for (const auto& s : above)
            if (s->is_present(l->get_key()))
                return;
        keep_going = cb(l->get_key());
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

bool sharded_store::leaf_page(size_t shard, size_t& page, const leaf_visit& cb) const {
    auto t = spc->get(shard);
    if (!t || t->get_size() == 0) {
        page = 0;
        return false;
    }
    heap::vector<uint8_t> buffer;
    {
        // the page is copied under the read latch and walked without it, as scan does
        read_lock release(t);
        t->page(page, buffer);
    }
    if (!buffer.empty()) {
        art::page_iterator_ptr(buffer.data(), (unsigned) buffer.size(),
            [&](const art::leaf* l, uint32_t) -> bool {
                if (!l->deleted() && !l->is_tomb() && !l->expired())
                    cb(*l);
                return true;
            });
    }
    page = t->next_page(page);
    return page > 0;
}

void sharded_store::glob(const art::keys_spec& spec, art::value_type pattern, bool by_value,
                         const leaf_cb& cb) const {
    // deliberately unlocked - each shard copies the page it is matching into a working
    // buffer first, so a leaf is only valid inside cb, and cb runs on worker threads
    auto* log = barch::tx_reading();
    if (!log) {
        for (const auto& shard : shards()) {
            shard->glob(spec, pattern, by_value, cb);
        }
        return;
    }
    /*
     * Inside a transaction - TODO 701: each shard held for reading, which takes the space
     * beneath it too, so no write moves a key from the space into a layer halfway through
     * its walk. The whole space counts as walked for COMMITTX's phantom check.
     */
    log->saw_range(spc.get(), {}, {});
    for (const auto& shard : shards()) {
        read_lock release(shard);
        shard->glob(spec, pattern, by_value, cb);
    }
}

}
