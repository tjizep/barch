//
// A transaction's reads through its overlay, and what they're checked by - TODO 695, 701.
// See docs/TRANSACTIONS.md.
//
#include "tx_session.h"
#include "shard.h"
#include "sharded_store.h"
#include "dictionary_compressor.h"
#include "configuration.h"

#include <algorithm>
#include <set>

namespace barch {
    namespace {
        void put_u32(std::string& out, uint32_t v) {
            for (int i = 0; i < 4; ++i) out.push_back((char) ((v >> (8 * i)) & 0xff));
        }
        bool get_u32(const std::string& in, size_t& at, uint32_t& v) {
            if (in.size() - at < 4) return false;
            v = 0;
            for (int i = 3; i >= 0; --i) v = (v << 8) | (uint8_t) in[at + i];
            at += 4;
            return true;
        }
        thread_local tx_read_log* open_log = nullptr;
    }

    tx_read_log* tx_reading() { return open_log; }
    tx_reading_scope::tx_reading_scope(tx_read_log* log) : outer(open_log) { open_log = log; }
    tx_reading_scope::~tx_reading_scope() { open_log = outer; }

    bool tx_overlay_reads::as_of(const abstract_shard* source, art::value_type key, tx_seen& out) {
        if (!versions || !source)
            return false;
        // oldest first: the first layer holding the key has what it was at the snapshot
        for (const auto& t : versions->layers_from(snapshot, source->get_shard_number())) {
            auto* layer = dynamic_cast<barch::shard*>(t.get());
            if (!layer)
                continue;
            read_lock hold(t);
            const auto n = art::search(layer, key);
            if (n.null() || !n.is_leaf)
                continue;
            const art::leaf* l = n.const_leaf();
            out = tx_seen{};
            if (l->is_tomb() || l->expired())
                return true;            // missing then
            out.present = true;
            const auto v = l->get_value();
            out.value.assign(v.chars(), v.size);
            out.expiry = (int64_t) l->expiry_ms();
            out.is_volatile = l->is_volatile();
            out.hashed = l->is_hashed();
            return true;
        }
        return false;
    }

    void tx_overlay_reads::layers_under(const abstract_shard* top,
                                        heap::vector<std::shared_ptr<abstract_shard>>& out) {
        if (!versions || !overlay || !top)
            return;
        const auto n = top->get_shard_number();
        if (overlay->get(n).get() != top)
            return;                     // not this transaction's overlay
        for (auto& t : versions->layers_from(snapshot, n))
            out.push_back(std::move(t));
    }

    void tx_overlay_reads::saw_range(const void* space, art::value_type lo, art::value_type hi) {
        if (!versions || space != overlay.get())
            return;
        std::string packed(4, '\0');
        for (int i = 0; i < 4; ++i)
            packed[(size_t) i] = (char) ((lo.size >> (8 * i)) & 0xff);
        packed.append(lo.chars(), lo.size);
        packed.append(hi.chars(), hi.size);
        logged_bytes += packed.size();
        ranges.set(ranges.size(), packed);
    }

    bool tx_overlay_reads::saw(const abstract_shard* shard, art::value_type key, bool present, art::value_type value,
                               int64_t expiry) {
        // only the overlay's own reads: the space beneath may depend on another, and
        // its reading through is part of the same read
        if (!shard || !overlay || overlay->get(shard->get_shard_number()).get() != shard)
            return false;
        const std::string k(key.chars(), key.size);
        if (seen.contains(k))
            return false;               // what it held at the first read is what's checked
        std::string packed;
        put_u32(packed, (uint32_t) shard->get_shard_number());
        const uint64_t h = tx_hash(present, value, expiry);
        put_u32(packed, (uint32_t) (h & 0xffffffffu));
        put_u32(packed, (uint32_t) (h >> 32));
        logged_bytes += k.size() + packed.size();
        seen.set(k, packed);
        return true;
    }

    const std::string& tx_before(tx_session& t) {
        if (!t.open || !t.failed.empty())
            return t.failed;
        const uint64_t most = get_tx_max_age_ms();
        const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t.began).count();
        if ((most && (uint64_t) age > most) || (t.versions && t.versions->expired(t.snapshot)))
            t.failed = "TXABORTED the transaction was open longer than tx_max_age_ms (" + std::to_string(most) +
                       " ms)";
        return t.failed;
    }

    const std::string& tx_after(tx_session& t) {
        if (!t.open || !t.failed.empty() || !t.overlay)
            return t.failed;
        const uint64_t most_keys = get_tx_max_keys(), most_bytes = get_tx_max_bytes();
        if (!most_keys && !most_bytes)
            return t.failed;
        uint64_t keys = 0, bytes = 0;
        for (const auto& s : t.overlay->get_shards()) {
            keys += s->get_tree_size();         // a tomb is a key it holds too
            // what its leaves and nodes take, as INFO's virtual_allocated counts it
            if (auto* sh = dynamic_cast<barch::shard*>(s.get()))
                bytes += sh->get_ap().get_leaves().get_allocated() + sh->get_ap().get_nodes().get_allocated();
        }
        /*
         * A write reads its key first, so the keys written are among those read, and
         * each key counts once as the larger of the two. Bytes are both: the overlay and
         * the log each hold theirs.
         */
        if (t.reads) {
            keys = std::max<uint64_t>(keys, t.reads->size() + t.reads->range_count());
            bytes += t.reads->bytes();
        }
        if (most_keys && keys > most_keys)
            t.failed = "TXABORTED the transaction holds " + std::to_string(keys) + " keys, written and read, "
                       "more than tx_max_keys (" + std::to_string(most_keys) + ")";
        else if (most_bytes && bytes > most_bytes)
            t.failed = "TXABORTED the transaction holds " + std::to_string(bytes) + " bytes, written and read, "
                       "more than tx_max_bytes (" + std::to_string(most_bytes) + ")";
        if (!t.failed.empty())
            ++statistics::tx_over_limit;
        return t.failed;
    }

    bool tx_takes(const std::string& name) {
        static const std::set<std::string> s{
            "GET", "MGET", "EXISTS", "STRLEN", "INCR", "DECR", "INCRBY", "DECRBY", "APPEND",
            "SET", "SETNX", "SETEX", "PSETEX", "GETSET", "MSET", "DEL",
        };
        return s.count(name) > 0;
    }

    bool tx_walks(const std::string& name) {
        return name == "RANGE" || name == "COUNT" || name == "KEYS" || name == "SCAN";
    }

    uint64_t tx_hash(bool present, art::value_type value, int64_t expiry) {
        // FNV-1a over presence, deadline and value: the same bytes give the same hash anywhere
        uint64_t h = 1469598103934665603ull;
        auto mix = [&](const void* p, size_t n) {
            const auto* b = (const uint8_t*) p;
            for (size_t i = 0; i < n; ++i) {
                h ^= b[i];
                h *= 1099511628211ull;
            }
        };
        const uint8_t there = present ? 1 : 0;
        mix(&there, 1);
        if (present) {
            for (int i = 0; i < 8; ++i) {
                const uint8_t b = (uint8_t) (((uint64_t) expiry >> (8 * i)) & 0xff);
                mix(&b, 1);
            }
            mix(value.bytes, value.size);
        }
        return h;
    }

    uint64_t tx_hash_now(const key_space_ptr& ks, const std::string& key) {
        if (!ks)
            return tx_hash(false, {}, 0);
        uint64_t h = tx_hash(false, {}, 0);
        barch::sharded_store store(ks);
        store.search(art::value_type(key.data(), key.size()), [&](const art::node_ptr& n) {
            auto cl = n.const_leaf();
            if (cl->expired())
                return;
            auto vt = cl->get_value();
            if (cl->is_compressed())
                vt = dictionary::decompress(art::space_of(n), vt);
            h = tx_hash(true, vt, (int64_t) cl->expiry_ms());
        });
        return h;
    }

    std::string tx_range_key(art::value_type lo, art::value_type hi) {
        std::string out;
        put_u32(out, (uint32_t) lo.size);
        out.append(lo.chars(), lo.size);
        out.append(hi.chars(), hi.size);
        return out;
    }

    bool tx_range_from(const std::string& packed, std::string& lo, std::string& hi) {
        size_t at = 0;
        uint32_t n = 0;
        if (!get_u32(packed, at, n) || packed.size() - at < n)
            return false;
        lo.assign(packed, at, n);
        hi.assign(packed, at + n, std::string::npos);
        return true;
    }

    uint64_t tx_range_hash(const heap::vector<shard_ptr>& levels, art::value_type lo, art::value_type hi) {
        uint64_t h = 1469598103934665603ull;
        auto mix = [&](const void* p, size_t n) {
            const auto* b = (const uint8_t*) p;
            for (size_t i = 0; i < n; ++i) {
                h ^= b[i];
                h *= 1099511628211ull;
            }
        };
        // no lower bound: from the smallest key of any level, as merged_count starts
        std::string first;
        if (lo.empty()) {
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
                return h;
            lo = art::value_type{first.data(), first.size()};
        }
        heap::vector<art::iterator> each;
        for (const auto& s : levels)
            each.emplace_back(s, lo);
        for (art::merge_iterator m(each, hi); m.ok(); m.next()) {
            const auto k = m.key();
            if (k < lo)
                continue;
            const auto n = m.current();
            const art::leaf* l = n.const_leaf();
            if (l->is_tomb() || l->expired())
                continue;
            auto v = l->get_value();
            if (l->is_compressed())
                v = dictionary::decompress(art::space_of(n), v);
            // little end first, so any node makes the same bytes
            uint8_t b[8];
            for (int i = 0; i < 4; ++i) b[i] = (uint8_t) ((k.size >> (8 * i)) & 0xff);
            mix(b, 4);
            mix(k.bytes, k.size);
            const uint64_t one = tx_hash(true, v, (int64_t) l->expiry_ms());
            for (int i = 0; i < 8; ++i) b[i] = (uint8_t) ((one >> (8 * i)) & 0xff);
            mix(b, 8);
        }
        return h;
    }

    uint64_t tx_range_hash_now(const key_space_ptr& ks, uint32_t shard, art::value_type lo, art::value_type hi) {
        if (!ks || shard >= ks->get_shard_count())
            return tx_range_hash({}, lo, hi);
        auto t = ks->get(shard);
        read_lock hold(t);
        heap::vector<shard_ptr> levels;
        for (auto s = t; s; s = s->sources())
            levels.push_back(s);
        return tx_range_hash(levels, lo, hi);
    }

    std::string tx_read_word(uint32_t shard, const std::string& space, const std::string& key, uint64_t hash) {
        std::string out;
        put_u32(out, shard);
        put_u32(out, (uint32_t) space.size());
        out += space;
        put_u32(out, (uint32_t) key.size());
        out += key;
        put_u32(out, (uint32_t) (hash & 0xffffffffu));
        put_u32(out, (uint32_t) (hash >> 32));
        return out;
    }

    bool tx_read_from(const std::string& word, uint32_t& shard, std::string& space, std::string& key,
                      uint64_t& hash) {
        size_t at = 0;
        uint32_t n = 0, lo = 0, hi = 0;
        if (!get_u32(word, at, shard) || !get_u32(word, at, n) || word.size() - at < n)
            return false;
        space.assign(word, at, n);
        at += n;
        if (!get_u32(word, at, n) || word.size() - at < n)
            return false;
        key.assign(word, at, n);
        at += n;
        if (!get_u32(word, at, lo) || !get_u32(word, at, hi) || at != word.size())
            return false;
        hash = ((uint64_t) hi << 32) | lo;
        return true;
    }
}
