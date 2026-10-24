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
#include "tx_locks.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

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
     * What an open transaction is doing, for INFO transactions - TODO 720: whose it is,
     * what it's running, what it holds and what it's waiting for. It's listed from
     * BEGINTX until the transaction ends, so one that isn't moving can be found by hand
     * when something times out. Its session writes it and INFO reads it from another
     * thread, so every field is under `m`.
     */
    struct tx_status {
        static constexpr size_t held_shown = 16;
        mutable std::mutex m;
        std::string id, space, who;
        std::chrono::steady_clock::time_point began{}, since{};    // since: in this state
        std::string function, command;      // what it's running now; both empty when idle
        bool committing{false};
        std::string waiting, waiting_on;    // what it waits for, and the id holding it
        bool waiting_gate{false};           // for its space's commit gate (no Raft)
        std::vector<std::string> held;      // the first held_shown keys it holds
        uint64_t holds{0};                  // how many keys it holds in all
        std::string holds_what;             // what else, like every shard of its space
    };

    /** listed in INFO transactions, and taken off again - TODO 720 */
    void tx_list(const std::shared_ptr<tx_status>& s);
    void tx_unlist(const std::shared_ptr<tx_status>& s);
    /** INFO transactions' line for each listed transaction, `tx0:...` on; only the
     * waiting ones for INFO transactions waiting - TODO 723 */
    std::string tx_listing(bool waiting_only = false);

    /**
     * A command running in a transaction - TODO 720. A function's name is kept while the
     * commands it calls run, and what was there before is put back after.
     */
    struct tx_running_scope {
        tx_running_scope(std::shared_ptr<tx_status> s, const std::string& function, const std::string& command);
        ~tx_running_scope();
        tx_running_scope(const tx_running_scope&) = delete;
        tx_running_scope& operator=(const tx_running_scope&) = delete;
    private:
        std::shared_ptr<tx_status> s;
        std::string function, command;
    };

    /** a transaction waiting for something someone else holds - TODO 720 */
    struct tx_waiting_scope {
        tx_waiting_scope(tx_status* s, std::string what, std::string on, bool gate = false);
        ~tx_waiting_scope();
        tx_waiting_scope(const tx_waiting_scope&) = delete;
        tx_waiting_scope& operator=(const tx_waiting_scope&) = delete;
    private:
        tx_status* s;
    };

    void tx_lock_wake(const key_space_ptr& ks, const std::set<std::string>& keys);

    /**
     * The transaction. Its writes are made in `overlay`, a space that DEPENDS on `space`
     * (key_space::make_overlay): every command it takes runs there, reading through to
     * `space` for a key the overlay doesn't hold, and a delete is a tomb. Nobody else
     * sees the overlay; COMMITTX walks it and makes its writes in `space`.
     */
    struct tx_session {
        bool open{false};
        /**
         * begun by a stored function's barch.tx.run, which promises to abort it on an
         * error. A timeout can stop run before its abort, so a CALLF that fails with
         * one of these open aborts it - TODO 743. One a script begins with BEGINTX
         * itself is left open for the client (TODO 705).
         */
        bool by_run{false};
        // made at BEGINTX: its start time first, so ids order by age - TODO 696
        std::string id;
        key_space_ptr space;
        key_space_ptr overlay;
        std::shared_ptr<tx_overlay_reads> reads;
        // its snapshot, in its space's versions - TODO 701; in a Raft space, this node's
        std::shared_ptr<tx_versions> versions;
        uint64_t snapshot{0};
        std::chrono::steady_clock::time_point began{};
        /** whose session this is, as INFO transactions names it - TODO 720 */
        std::string who;
        /** what it's doing, listed while it's open - TODO 720 */
        std::shared_ptr<tx_status> status;
        /** off the list, once it's ended */
        void unlist() {
            if (status)
                tx_unlist(status);
            status.reset();
        }
        /**
         * Why it can only be ended now, or empty - TODO 701: it went past tx_max_keys or
         * tx_max_bytes, or was open past tx_max_age_ms. Every command in it but ABORTTX
         * and COMMITTX answers this, and COMMITTX aborts it.
         */
        std::string failed;
        /**
         * BEGINTX PESSIMISTIC - TODO 719: it locks each key as it's used, a read shared and
         * a write exclusive, in its space's table, and holds them to the end. It has no
         * snapshot: it reads the latest, which its locks keep still.
         */
        bool pessimistic{false};
        std::shared_ptr<tx_locks> locks;
        std::set<std::string> locked;   // tx_locks::key_of form
        /** every lock it holds let go of, at its end, so whoever waits on one goes on */
        void release_locks() {
            if (locks) {
                // whoever parked on them - TODO 721, or behind one of its spans - TODO 749
                tx_lock_wake(space, locks->release(id, locked));
            }
            locks.reset();
            locked.clear();
            pessimistic = false;
        }
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
            by_run = false;
            failed.clear();
            reads.reset();              // it holds the overlay too
            key_space::give_back(std::move(overlay));
            end_snapshot();
            release_locks();
            space.reset();
            unlist();
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

    /** a key lock that was busy, and who held it - TODO 721 */
    struct tx_lock_busy {
        std::string key;        // tx_locks::key_of form
        std::string arg;        // the key as the client sent it
        std::string holder;
        explicit operator bool() const { return !key.empty(); }
    };
    /**
     * Before a command in a pessimistic transaction - TODO 719: the locks its keys need,
     * taken. `args` is the command as sent. With no `busy`, each is waited for here up to
     * tx_lock_wait_ms; empty, or why not, which sets `failed`. With `busy` nothing is
     * waited for (TODO 721): the first lock someone else holds is said there instead, and
     * the ones before it are kept.
     */
    std::string tx_lock(tx_session& t, const std::string& name, const std::vector<std::string>& args,
                        tx_lock_busy* busy = nullptr);
    /**
     * One key's lock for a stored function's barch.store - TODO 732: `key` as the script
     * named it, shared for a read and exclusive for a write or a read for update. Waited
     * for here, like tx_lock with no `busy`. Empty, or why not, which sets `failed`.
     * Nothing outside a pessimistic transaction. With `busy` it's only tried, and a lock
     * someone else holds is said there (TODO 731), as tx_lock does.
     */
    std::string tx_lock_key(tx_session& t, const std::string& key, bool exclusive, tx_lock_busy* busy = nullptr);
    /**
     * The span a stored function's barch.store walk reads - TODO 749: [lo, hi) as the
     * script named it, or every key with `whole`. Otherwise as tx_lock_key.
     */
    std::string tx_lock_span(tx_session& t, const std::string& lo, const std::string& hi, bool whole,
                             tx_lock_busy* busy = nullptr);
    /** a lock that wasn't got in time: why, and the transaction can only end now */
    std::string tx_lock_timed_out(tx_session& t, const tx_lock_busy& busy);
    /**
     * Before a plain string write, outside a transaction, in a space that has key locks -
     * TODO 719: waits, up to tx_lock_wait_ms, until no transaction holds its keys. Empty,
     * or why it can't go ahead. With `busy`, it doesn't wait but says which key is held.
     */
    std::string tx_plain_wait(const key_space_ptr& ks, const std::string& name, const std::vector<std::string>& args,
                              tx_lock_busy* busy = nullptr);
    std::string tx_plain_timed_out(const tx_lock_busy& busy);
    /**
     * A session parked on a key lock - TODO 721. It waits on the key's shard under this
     * name, which no client key has (a command can't start with \x01), and a lock let go
     * of wakes the first one waiting there; each that gets in passes the turn on.
     */
    std::string tx_lock_wake_key(const std::string& key);
    /** wakes whoever is parked on `keys`, their locks just let go of. Holds no latch. */
    void tx_lock_wake(const key_space_ptr& ks, const std::set<std::string>& keys);
    /**
     * After a parked session's wait is registered: if the lock went just before, nothing
     * will wake it, so it's woken now.
     */
    void tx_lock_recheck(const key_space_ptr& ks, const std::string& owner, const std::string& key);
    /** INFO transactions for a parked one: waiting on `busy`, or null when it's woken */
    void tx_lock_waiting(tx_status* s, const std::string& command, const tx_lock_busy* busy);

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
