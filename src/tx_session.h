#pragma once
//
// A connection's transaction, BEGINTX ... COMMITTX - TODO 693, 701. See docs/TRANSACTIONS.md.
//
#include "cluster_hooks.h"
#include "key_space.h"
#include "scratch_container.h"
#include "tx_reads.h"
#include "value_type.h"
#include "statistics.h"

#include <chrono>
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
        bool as_of(const abstract_shard* source, art::value_type key, tx_seen& out) override;
        void layers_under(const abstract_shard* top, heap::vector<std::shared_ptr<abstract_shard>>& out) override;
        void saw_range(const void* space, art::value_type lo, art::value_type hi) override;
        /** each range walked: lo and hi, in a scratch map so a lot of them stays bounded */
        template<typename F> void each_range(F&& f) const;
        /** the snapshot reads are answered as of - TODO 701; in a Raft space, this node's */
        std::shared_ptr<tx_versions> versions;
        uint64_t snapshot{0};
        uint64_t clears_at{0};      // the space's clears and loads when it was taken
        /** the hash `key` had at its first read, false when it wasn't read */
        bool hash_of(const std::string& key, uint64_t& hash) const;
        /** each read: the stored key, its shard and its hash */
        template<typename F> void each(F&& f) const;
        [[nodiscard]] size_t size() const { return seen.size(); }
        [[nodiscard]] size_t range_count() const { return ranges.size(); }
        /** about how many bytes the log holds: each read's key and its 12, each range's - TODO 701 */
        [[nodiscard]] uint64_t bytes() const { return logged_bytes; }
    private:
        uint64_t logged_bytes{0};
        key_space_ptr overlay;
        scratch::map<std::string, std::string> seen;   // stored key -> shard (4) and hash (8)
        scratch::map<uint64_t, std::string> ranges;    // n -> lo's length (4), lo, hi
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
        // its snapshot, in its space's versions - TODO 701; in a Raft space, this node's
        std::shared_ptr<tx_versions> versions;
        uint64_t snapshot{0};
        std::chrono::steady_clock::time_point began{};
        /**
         * Why it can only be ended now, or empty - TODO 701: it went past tx_max_keys or
         * tx_max_bytes, or was open past tx_max_age_ms. Every command in it but ABORTTX
         * and COMMITTX answers this, and COMMITTX aborts it.
         */
        std::string failed;
        /** the snapshot let go of, so the layers it held can go */
        void end_snapshot() {
            if (versions)
                versions->end(snapshot);
            versions.reset();
        }
        /**
         * ABORTTX, or a run with no client session ending with one open (TODO 709): its
         * writes go with the overlay. False when none was open.
         */
        bool abort_open() {
            if (!open)
                return false;
            open = false;
            failed.clear();
            reads.reset();              // it holds the overlay too
            key_space::give_back(std::move(overlay));
            end_snapshot();
            space.reset();
            --statistics::tx_live;
            ++statistics::tx_aborts;
            return true;
        }
        // a connection closed with one open: an abort, and no longer live - TODO 706
        ~tx_session() { abort_open(); }
    };

    /** a transaction running a command, for statistics::tx_active - TODO 706 */
    struct tx_active_scope {
        explicit tx_active_scope(bool counted = true) : counted(counted) {
            if (counted) ++statistics::tx_active;
        }
        ~tx_active_scope() {
            if (counted) --statistics::tx_active;
        }
        tx_active_scope(const tx_active_scope&) = delete;
        tx_active_scope& operator=(const tx_active_scope&) = delete;
        bool counted;
    };

    /**
     * Before a command in an open transaction - TODO 701: why it can't run, or empty. Sets
     * `failed` when the transaction has been open longer than tx_max_age_ms, or its
     * snapshot was let go of for that.
     */
    const std::string& tx_before(tx_session& t);
    /**
     * After one: sets `failed` when the transaction now holds more than tx_max_keys keys
     * or tx_max_bytes bytes, its overlay's and its read log's. Why, or empty.
     */
    const std::string& tx_after(tx_session& t);

    /** the commands a transaction takes: they run on its overlay */
    bool tx_takes(const std::string& name);
    /**
     * The walks a transaction takes: they read the overlay, the snapshot's layers and the
     * space - TODO 701.
     */
    bool tx_walks(const std::string& name);

    /** what a read is checked by: the same on every node for the same key state */
    uint64_t tx_hash(bool present, art::value_type value, int64_t expiry);
    /** the hash of `stored_key` in `ks` as it is now, an expired key absent */
    uint64_t tx_hash_now(const key_space_ptr& ks, const std::string& stored_key);

    /** a read as a word of a prepare entry, and back */
    std::string tx_read_word(uint32_t shard, const std::string& space, const std::string& key, uint64_t hash);
    bool tx_read_from(const std::string& word, uint32_t& shard, std::string& space, std::string& key,
                      uint64_t& hash);

    /*
     * A range walked, as a read word - TODO 701: its shard number with this bit set, its
     * "key" lo and hi packed (tx_range_key), and its hash the hash of what the shard held
     * in [lo, hi). A node that doesn't know the bit sees a shard outside its group and
     * skips the word.
     */
    constexpr uint32_t tx_range_bit = 0x80000000u;
    /** lo's length (4), lo, hi; an empty hi has no end */
    std::string tx_range_key(art::value_type lo, art::value_type hi);
    bool tx_range_from(const std::string& packed, std::string& lo, std::string& hi);
    /**
     * The hash of the keys in [lo, hi) through `levels`, the first holding a key winning,
     * tombs and expired keys left out: each key, then what tx_hash says of it. The caller
     * holds the levels still.
     */
    uint64_t tx_range_hash(const heap::vector<shard_ptr>& levels, art::value_type lo, art::value_type hi);
    /** that hash for shard `shard` of `ks` as it is now, the shard held for reading */
    uint64_t tx_range_hash_now(const key_space_ptr& ks, uint32_t shard, art::value_type lo, art::value_type hi);

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

    template<typename F> void tx_overlay_reads::each_range(F&& f) const {
        for (auto [n, packed] : ranges) {
            (void) n;
            uint32_t lo_size = 0;
            for (int i = 3; i >= 0; --i) lo_size = (lo_size << 8) | (uint8_t) packed[(size_t) i];
            const art::value_type lo{packed.data() + 4, lo_size};
            const art::value_type hi{packed.data() + 4 + lo_size, packed.size() - 4 - lo_size};
            f(lo, hi);
        }
    }
}
