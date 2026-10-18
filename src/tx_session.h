#pragma once
//
// A connection's transaction, BEGINTX ... COMMITTX - TODO 693, 701. See docs/TRANSACTIONS.md.
//
#include "cluster_hooks.h"
#include "key_space.h"
#include "scratch_container.h"
#include "tx_reads.h"
#include "value_type.h"

#include <cstdint>
#include <memory>
#include <string>

class caller;

namespace barch {
    /**
     * The keys a transaction read from beneath its overlay, each with the hash of what
     * it held at the first read, which the prepare checks is still so (choice 3,
     * serializable). Kept in a scratch space, so a transaction that reads a lot is held
     * in the arenas the server manages, like any other data - TODO 701.
     */
    class tx_overlay_reads final : public tx_read_log {
    public:
        explicit tx_overlay_reads(key_space_ptr overlay) : overlay(std::move(overlay)) {}
        bool saw(const abstract_shard* shard, art::value_type key, bool present, art::value_type value,
                 int64_t expiry) override;
        /** the hash `key` had at its first read, false when it wasn't read */
        bool hash_of(const std::string& key, uint64_t& hash) const;
        /** each read: the stored key, its shard and its hash */
        template<typename F> void each(F&& f) const;
        [[nodiscard]] size_t size() const { return seen.size(); }
    private:
        key_space_ptr overlay;
        scratch::map<std::string, std::string> seen;   // stored key -> shard (4) and hash (8)
    };

    /**
     * The transaction. Its writes are made in `overlay`, a space that DEPENDS on `space`
     * (key_space::make_overlay): every command it takes runs there, reading through to
     * `space` for a key the overlay doesn't hold, and a delete is a tomb. Nobody else
     * sees the overlay; COMMITTX walks it and makes its writes in `space`.
     */
    struct tx_session {
        bool open{false};
        // made at BEGINTX: its start time first, so ids order by age - TODO 696
        std::string id;
        key_space_ptr space;
        key_space_ptr overlay;
        std::shared_ptr<tx_overlay_reads> reads;
    };

    /** the commands a transaction takes: they run on its overlay */
    bool tx_takes(const std::string& name);

    /** what a read is checked by: the same on every node for the same key state */
    uint64_t tx_hash(bool present, art::value_type value, int64_t expiry);
    /** the hash of `stored_key` in `ks` as it is now, an expired key absent */
    uint64_t tx_hash_now(const key_space_ptr& ks, const std::string& stored_key);

    /** a read as a word of a prepare entry, and back */
    std::string tx_read_word(uint32_t shard, const std::string& space, const std::string& key, uint64_t hash);
    bool tx_read_from(const std::string& word, uint32_t& shard, std::string& space, std::string& key,
                      uint64_t& hash);

    inline bool tx_overlay_reads::hash_of(const std::string& key, uint64_t& hash) const {
        auto packed = seen.get(key);
        if (!packed)
            return false;
        hash = 0;
        for (int i = 11; i >= 4; --i) hash = (hash << 8) | (uint8_t) (*packed)[(size_t) i];
        return true;
    }

    template<typename F> void tx_overlay_reads::each(F&& f) const {
        for (auto [key, packed] : seen) {
            uint32_t shard = 0;
            uint64_t hash = 0;
            for (int i = 3; i >= 0; --i) shard = (shard << 8) | (uint8_t) packed[(size_t) i];
            for (int i = 11; i >= 4; --i) hash = (hash << 8) | (uint8_t) packed[(size_t) i];
            f(key, shard, hash);
        }
    }
}
