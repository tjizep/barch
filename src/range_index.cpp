//
// Created by teejip on 8/2/26.
//
// Ordered range sharding. The algorithm here was settled in
// test/rangeshard_prototype.cpp before any of it was written against real shards - see
// the class comment in range_index.h for what it established and DONE 31 for what this
// had to settle on top of it.
//

#include "range_index.h"

#include <algorithm>

#include "art/iterator.h"
#include "configuration.h"
#include "lzr_log.h"
#include "statistics.h"

namespace barch {

size_t get_range_shard_budget() {
    return 64;
}

double get_range_shard_tolerance() {
    return 1.25;
}

/** how many bytes of key and value one repartition pass may hold copies of at a time */
static constexpr size_t repartition_batch_bytes = 8ull * 1024 * 1024;

// ---- the table ---------------------------------------------------------------------

range_index::range_index() : current(std::make_shared<const table>()) {
}

static bool by_key(const range_index::entry& a, const range_index::entry& b) {
    return a.value() < b.value();
}

static range_index::entry make_entry(art::value_type key, size_t shard) {
    range_index::entry e;
    route_key(key).to_vector(e.key);
    e.shard = shard;
    return e;
}

size_t range_index::upper(const table& t, art::value_type key) {
    auto k = route_key(key);
    auto it = std::upper_bound(t.begin(), t.end(), k,
                               [](art::value_type a, const entry& e) { return a < e.value(); });
    return (size_t) (it - t.begin());
}

size_t range_index::route(const table& t, art::value_type key) {
    size_t at = upper(t, key);
    if (!at) return 0;               // below every boundary: the span open at the bottom
    return t[at - 1].shard;
}

void range_index::publish(const std::shared_ptr<table>& t) {
#if BARCH_HAS_ATOMIC_SHARED_PTR
    current.store(t, std::memory_order_release);
#else
    std::atomic_store_explicit(&current, table_ptr(t), std::memory_order_release);
#endif
}

// ---- reading the shards -------------------------------------------------------------

/** the shard's own smallest key, or an empty value if it has none */
static art::value_type shard_minimum(const shard_ptr& s) {
    auto m = s->tree_minimum();
    if (!m.is_leaf) return {};
    return m.const_leaf()->get_key();
}

static art::value_type shard_maximum(const shard_ptr& s) {
    auto m = s->tree_maximum();
    if (!m.is_leaf) return {};
    return m.const_leaf()->get_key();
}

bool range_index::partitioned(const heap::vector<shard_ptr>& shards) {
    art::value_type previous_max{};
    for (const auto& s : shards) {
        auto lo = shard_minimum(s);
        if (lo.empty()) continue;               // an empty shard breaks nothing
        if (!previous_max.empty() && !(route_key(previous_max) < route_key(lo))) {
            return false;
        }
        previous_max = shard_maximum(s);
    }
    return true;
}

bool range_index::rebuild(const heap::vector<shard_ptr>& shards) {
    auto t = std::make_shared<table>();
    t->reserve(shards.size());
    for (size_t s = 1; s < shards.size(); ++s) {
        auto lo = shard_minimum(shards[s]);
        if (lo.empty()) continue;               // shard 0 has no entry, and nor does an
        t->push_back(make_entry(lo, s));        // empty shard: it owns nothing yet
    }
    // if the shards are a partition these are already in key order. Sorting anyway
    // costs nothing at this size and keeps the table well formed even when they are
    // not, so that a route is at worst wrong rather than undefined
    bool ordered = std::is_sorted(t->begin(), t->end(), by_key);
    std::sort(t->begin(), t->end(), by_key);
    publish(t);
    return ordered && partitioned(shards);
}

void range_index::refresh(const heap::vector<shard_ptr>& shards, size_t a, size_t b) {
    auto old = get();
    auto t = std::make_shared<table>();
    t->reserve(old->size() + 2);
    for (const auto& e : *old) {
        if (e.shard != a && e.shard != b) t->push_back(e);
    }
    for (size_t s : {a, b}) {
        if (s == 0) continue;
        auto lo = shard_minimum(shards[s]);
        if (lo.empty()) continue;
        t->push_back(make_entry(lo, s));
    }
    std::sort(t->begin(), t->end(), by_key);
    publish(t);
}

// ---- moving keys --------------------------------------------------------------------

/**
 * take the leaf apart into copies that outlive it.
 *
 * The leaf lives in the source shard's storage and the remove that follows frees it, so
 * everything needed to put it back together has to be copied out first.
 */
struct moved_key {
    heap::vector<uint8_t> key{};
    heap::vector<uint8_t> value{};
    art::key_options options{};

    explicit moved_key(const art::leaf* l) {
        l->get_key().to_vector(key);
        l->get_value().to_vector(value);
        // the same three the defragmenter carries when it reinserts a leaf into its own
        // shard: a move is that operation with a different destination
        options.set_expiry(l->expiry_ms());
        options.set_volatile(l->is_volatile());
        options.set_compressed(l->is_compressed());
    }
    [[nodiscard]] art::value_type k() const { return art::value_type(key); }
    [[nodiscard]] art::value_type v() const { return art::value_type(value); }
    [[nodiscard]] size_t bytes() const { return key.size() + value.size(); }
};

static void apply_move(const shard_ptr& from, const shard_ptr& to, const moved_key& m) {
    auto fc = [](const art::node_ptr&) -> void {};
    from->tree_remove(m.k(), fc);
    to->tree_insert(m.options, m.k(), m.v(), true, fc);
    // a rebalance is not user traffic. Left counted it would show up as a delete and an
    // insert that no client asked for, and the key count would look like it grew
    --statistics::insert_ops;
    --statistics::new_keys_added;
    --statistics::delete_ops;
    ++statistics::range_shard_keys_moved;
}

/**
 * move up to `count` keys off one end of `from` into `to`.
 *
 * Both shards must already be write locked by the caller. Taking the extreme key each
 * time is what keeps the partition whole: the top of a shard is the only key that can
 * cross into the shard above without any key overtaking another.
 */
static size_t move_keys(const shard_ptr& from, const shard_ptr& to, size_t count,
                        bool from_top) {
    size_t moved = 0;
    for (size_t i = 0; i < count; ++i) {
        auto n = from_top ? from->tree_maximum() : from->tree_minimum();
        if (!n.is_leaf) break;
        const art::leaf* l = n.const_leaf();
        if (!l || l->is_tomb()) break;
        apply_move(from, to, moved_key(l));
        ++moved;
    }
    return moved;
}

// ---- the sweep ----------------------------------------------------------------------

/**
 * a shard with pull sources is left alone.
 *
 * Its contents are not all its own - a key it answers for may live upstream, and a
 * delete against it leaves a tombstone rather than removing anything - so neither its
 * size nor its minimum means what the algorithm here needs them to mean.
 */
static bool moveable(const heap::vector<shard_ptr>& shards) {
    for (const auto& s : shards) {
        if (s->sources()) return false;
    }
    return true;
}

range_index::sweep_result range_index::sweep(const heap::vector<shard_ptr>& shards,
                                             size_t budget, double tolerance,
                                             size_t max_sheds) {
    sweep_result r;
    const size_t n = shards.size();
    if (n < 2 || !moveable(shards)) return r;

    auto tree_size = [&](size_t s) -> size_t { return shards[s]->get_tree_size(); };

    while (r.sheds < max_sheds) {
        size_t total = 0;
        size_t cur = 0, least = 0;
        for (size_t i = 0; i < n; ++i) {
            total += tree_size(i);
            if (tree_size(i) > tree_size(cur)) cur = i;
            if (tree_size(i) < tree_size(least)) least = i;
        }
        if (!total) return r;
        double average = (double) total / (double) n;

        // the tolerance is a band around the average, not a ceiling over it, and the
        // second half of that is not decoration. A ceiling alone is satisfied by most of
        // the shards sitting at exactly the ceiling and the last one or two holding
        // almost nothing - the arithmetic works out and the space measures balanced with
        // a fraction of its shards unused, which is the whole point of having them
        bool over = (double) tree_size(cur) > tolerance * average;
        bool under = (double) tree_size(least) < average / tolerance;
        if (!over && !under) {
            return r;                        // balanced: nothing left to do
        }

        // cascade away from the largest shard. Each hop looks at where it just pushed
        // keys, because that is the shard that may now be over in turn.
        //
        // A hop is not conditional on the shard being over the threshold, only on its
        // neighbour being meaningfully smaller. A cascade that stopped at the first
        // shard under the threshold would never reach the ones past it, which is how a
        // space ends up balanced at the bottom and empty at the top. The half way rule
        // below is what stops it: once two neighbours are within a key of each other
        // there is nothing to take, so the cascade ends on its own
        bool progressed = false;
        for (size_t step = 0; step < n && r.sheds < max_sheds; ++step) {
            bool up_ok = cur + 1 < n;
            bool down_ok = cur > 0;
            if (!up_ok && !down_ok) break;
            bool up = up_ok && (!down_ok || tree_size(cur + 1) <= tree_size(cur - 1));
            size_t other = up ? cur + 1 : cur - 1;

            // meet the neighbour half way rather than shedding down to the threshold:
            // the prototype found the latter costs moves quadratic in the shard count
            if (tree_size(cur) <= tree_size(other) + 1) break;
            size_t take = std::min(budget, (tree_size(cur) - tree_size(other)) / 2);
            if (take == 0) break;

            size_t moved = 0;
            {
                // in shard order, which is the order every other lock over more than
                // one shard of a space is taken in
                size_t lo = std::min(cur, other), hi = std::max(cur, other);
                storage_release a(shards[lo]);
                storage_release b(shards[hi]);
                moved = move_keys(shards[cur], shards[other], take, up);
                if (moved) refresh(shards, lo, hi);
            }
            // the locks are dropped here on purpose: a sweep with a lot to do would
            // otherwise hold two shards still for all of it
            ++r.sheds;
            r.moved += moved;
            if (!moved) break;
            progressed = true;
            cur = other;
        }
        if (!progressed) {
            return r;               // out of balance but unable to shed any further
        }
    }
    r.balanced = false;             // ran out of sheds with work still to do
    return r;
}

// ---- repartition --------------------------------------------------------------------

/**
 * boundaries that would divide the space into `n` roughly equal parts, from a sample.
 *
 * An exact answer needs the whole key space in order, which is the thing we do not have
 * and cannot afford to build. A sample gets the boundaries close, every key still lands
 * on the right side of whatever boundaries are chosen - so the result is a correct
 * partition either way - and the sweep polishes the balance afterwards.
 */
static heap::vector<heap::vector<uint8_t>> sample_boundaries(
        const heap::vector<shard_ptr>& shards, size_t n) {
    static constexpr size_t max_samples_per_shard = 4096;
    heap::vector<heap::vector<uint8_t>> sample;
    for (const auto& s : shards) {
        size_t size = s->get_tree_size();
        if (!size) continue;
        // seeded from the shard's own minimum rather than art::iterator's one argument
        // form, which leaves its trace empty and walks nothing - see TODO 31
        auto first = shard_minimum(s);
        if (first.empty()) continue;
        size_t stride = std::max<size_t>(1, size / max_samples_per_shard);
        size_t at = 0;
        for (art::iterator i(s, first); i.ok(); i.next()) {
            if (at++ % stride) continue;
            heap::vector<uint8_t> k;
            route_key(i.key()).to_vector(k);
            sample.push_back(std::move(k));
        }
    }
    std::sort(sample.begin(), sample.end());
    heap::vector<heap::vector<uint8_t>> out;
    if (sample.size() < n) return out;
    for (size_t s = 1; s < n; ++s) {
        auto at = sample[(sample.size() * s) / n];
        if (out.empty() || out.back() < at) out.push_back(at);
    }
    return out;
}

size_t range_index::repartition(const heap::vector<shard_ptr>& shards) {
    const size_t n = shards.size();
    if (n < 2 || !moveable(shards)) return 0;

    auto bounds = sample_boundaries(shards, n);
    {
        // the boundaries become the table first, so that route() below says where every
        // key is going. Shard 0 keeps the span open at the bottom, as always
        auto t = std::make_shared<table>();
        for (size_t i = 0; i < bounds.size(); ++i) {
            entry e;
            e.key = bounds[i];
            e.shard = i + 1;
            t->push_back(std::move(e));
        }
        publish(t);
    }

    auto t = get();
    size_t moved = 0;
    for (size_t s = 0; s < n; ++s) {
        // drain in batches: a pass collects what is in the wrong place until it is
        // holding enough bytes, then applies those moves and starts again. Iterating a
        // tree while removing from it is not safe, and holding a copy of every misplaced
        // key at once would be the whole shard
        for (;;) {
            heap::vector<moved_key> batch;
            size_t bytes = 0;
            auto first = shard_minimum(shards[s]);
            if (first.empty()) break;
            for (art::iterator i(shards[s], first); i.ok(); i.next()) {
                const art::leaf* l = i.l();
                if (!l || l->is_tomb()) continue;
                if (route(*t, l->get_key()) == s) continue;
                batch.emplace_back(l);
                bytes += batch.back().bytes();
                if (bytes >= repartition_batch_bytes) break;
            }
            if (batch.empty()) break;
            for (const auto& m : batch) {
                apply_move(shards[s], shards[route(*t, m.k())], m);
                ++moved;
            }
        }
    }
    rebuild(shards);
    return moved;
}

}
