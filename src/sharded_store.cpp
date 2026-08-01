//
// Created by teejip on 8/1/26.
//
// The sharding layer. Every algorithm here was previously open coded inside whichever
// command needed it - see the class comment in sharded_store.h for why they moved.
//

#include "sharded_store.h"
#include "abstract_shard.h"

#include <algorithm>

#include "art/iterator.h"
#include "keys.h"
#include "statistics.h"

namespace barch {

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
    auto t = shard_for(key);
    if (!t) return false;

    // a shard with sources cannot answer from its own bloom filter alone, because the
    // key may only exist upstream
    if (!t->sources() && t->has_static_bloom_filter() && !t->is_bloom(key)) {
        return false;
    }
    read_lock release(t);
    auto r = t->search(key);
    if (r.null() || r.cl()->is_tomb()) {
        return false;
    }
    cb(r);
    return true;
}

bool sharded_store::exists(art::value_type key) const {
    auto t = shard_for(key);
    if (!t) return false;
    if (!t->sources() && t->has_static_bloom_filter() && !t->is_bloom(key)) {
        return false;
    }
    read_lock release(t);
    return !t->search(key).null();
}

bool sharded_store::insert(const art::key_options& opts, art::value_type key,
                           art::value_type value, bool update, const art::NodeResult& fc) {
    auto t = shard_for(key);
    if (!t) return false;
    storage_release release(t);
    return t->opt_insert(opts, key, value, update, fc);
}

bool sharded_store::add(const art::key_options& opts, art::value_type key,
                        art::value_type value, const art::NodeResult& fc) {
    auto t = shard_for(key);
    if (!t) return false;
    storage_release release(t);
    return t->insert(opts, key, value, false, fc);
}

bool sharded_store::remove(art::value_type key, const art::NodeResult& fc) {
    auto t = shard_for(key);
    if (!t) return false;
    storage_release release(t);
    return t->remove(key, fc);
}

bool sharded_store::update(art::value_type key, const updater_fn& updater) {
    auto t = shard_for(key);
    if (!t) return false;
    storage_release release(t);
    return t->update(key, updater);
}

void sharded_store::with_key_write(art::value_type key, const shard_fn& fn) {
    auto t = shard_for(key);
    if (!t) return;
    storage_release release(t);
    fn(t);
}

void sharded_store::with_key_read(art::value_type key, const shard_fn& fn) const {
    auto t = shard_for(key);
    if (!t) return;
    read_lock release(t);
    fn(t);
}

sharded_store::write_locked_shard sharded_store::write_locked(art::value_type key) const {
    auto t = shard_for(key);
    return {t, write_guard(t)};
}

sharded_store::read_locked_shard sharded_store::read_locked(art::value_type key) const {
    auto t = shard_for(key);
    return {t, read_guard(t)};
}

sharded_store::write_guard sharded_store::lock_key_write(art::value_type key) const {
    return write_guard(shard_for(key));
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
        storage_release release(t);
        fn(t);
    }
}

void sharded_store::each_shard_read(const shard_fn& fn) const {
    for (const auto& t : shards()) {
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

bool sharded_store::minimum(const key_cb& cb) const {
    art::value_type the_min;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
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

bool sharded_store::maximum(const key_cb& cb) const {
    art::value_type the_max;
    ks_shared kss(spc->source());
    ks_shared ksl(spc);
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
                        tmp = encoded_key_as_string(l->get_key());
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
                         const leaf_cb& cb) const {
    // deliberately unlocked - each shard copies the page it is matching into a working
    // buffer first, so a leaf is only valid inside cb, and cb runs on worker threads
    for (const auto& t : shards()) {
        t->glob(spec, pattern, by_value, cb);
    }
}

}
