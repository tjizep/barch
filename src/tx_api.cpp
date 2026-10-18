//
// A transaction's reads through its overlay, and what they're checked by - TODO 695, 701.
// See docs/TRANSACTIONS.md.
//
#include "tx_session.h"
#include "sharded_store.h"
#include "dictionary_compressor.h"

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
        seen.set(k, packed);
        return true;
    }

    bool tx_takes(const std::string& name) {
        static const std::set<std::string> s{
            "GET", "MGET", "EXISTS", "STRLEN", "INCR", "DECR", "INCRBY", "DECRBY", "APPEND",
            "SET", "SETNX", "SETEX", "PSETEX", "GETSET", "MSET", "DEL",
        };
        return s.count(name) > 0;
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
