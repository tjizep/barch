#pragma once
//
// Key locks for pessimistic transactions, BEGINTX PESSIMISTIC - TODO 719. See
// docs/TRANSACTIONS.md, "Pessimistic transactions".
//
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace barch {
    /**
     * The key locks of one space. A pessimistic transaction takes a shared lock on each
     * key it reads and an exclusive one on each key it writes, as the command runs, and
     * holds them until it ends (strict two phase locking). A lock someone else holds is
     * waited for, up to tx_lock_wait_ms.
     *
     * Keys are the stored form, as a shard holds them, so a command's key and a shard's
     * write to it name the same lock. The table is made at a space's first pessimistic
     * BEGINTX, and a shard of a space that never has one pays a null check per write.
     *
     * Its own mutex, never a shard latch: a transaction waits here before its command
     * touches a shard, so a wait never holds one up (the rule in TODO 699).
     *
     * A walk (RANGE, COUNT, KEYS, SCAN) takes a shared lock on the range it reads, a
     * span, so a key written into it later isn't missed - TODO 749. A span is in the
     * way of an exclusive lock on any key inside it, and waits for any exclusive lock
     * inside it, the same as a shared key lock does for its one key.
     */
    class tx_locks {
    public:
        /**
         * a range of keys in key_of form, both ends in: [lo, hi], or [lo, the end) with
         * `to_end`. key_of keeps the order of stored keys (dropping a last NUL only
         * moves a key down to the one below it), so every stored key in [lo, hi) is in
         * the span of key_of(lo) to key_of(hi). That takes in hi itself too, which only
         * locks a little more than was read.
         */
        struct span {
            std::string lo, hi;
            bool to_end{false};
            [[nodiscard]] bool covers(const std::string& key) const {
                return key >= lo && (to_end || key <= hi);
            }
            [[nodiscard]] bool covers(const span& o) const {
                return o.lo >= lo && (to_end || (!o.to_end && o.hi <= hi));
            }
        };
        struct taken {
            bool ok{false};
            std::string holder;     // when not ok: the transaction in the way
            std::string key;        // when not ok: the key it waits on (for a span, the one in it)
            /**
             * when not ok: waiting would close a ring of transactions each waiting on
             * the next - TODO 724. `ring` is the others in it, in order from `holder`.
             */
            bool deadlock{false};
            std::vector<std::string> ring;
            /*
             * when not ok: `holder` is a parked session waiting to write the key itself,
             * as a writer a reader queues behind, or a reader upgrading. It may go on
             * when this one can't, so a wake on the key that reached this one is passed
             * on - TODO 869.
             */
            bool behind_parked{false};
        };
        /**
         * a lock on `key` for `owner`, exclusive or shared, waiting up to `until`. A
         * shared lock the owner holds alone becomes exclusive; one shared with others
         * waits for them. Taking one already held is nothing new.
         *
         * A new shared lock waits behind another transaction waiting for an exclusive
         * one on the key - TODO 869. Otherwise a deadlock's victim, retrying, takes its
         * shared lock back before the survivor wakes to upgrade, and closes the same
         * ring again. An owner holding the key already doesn't wait: that would be a
         * ring of its own making.
         *
         * While it waits, `owner` is noted as waiting for `key`, and stays noted when it
         * returns not ok with `until` already past (a session parking, TODO 721) until it
         * takes a lock, `stop_waiting` or `release`. A wait that would close a ring of
         * waits isn't started: it returns at once with `deadlock` - TODO 724.
         */
        taken take(const std::string& owner, const std::string& key, bool exclusive,
                   std::chrono::steady_clock::time_point until);
        /**
         * a shared lock on every key in `s` for `owner`, as take does for one key - TODO
         * 749. It waits while another transaction holds an exclusive lock on a key in
         * it, noted as waiting for that key.
         */
        taken take_span(const std::string& owner, const span& s, std::chrono::steady_clock::time_point until);
        /**
         * every lock `owner` holds on `keys`, and all its spans, and wakes anyone
         * waiting; nor is it waiting. The keys a session parked on should be woken for:
         * `keys`, and when spans went, the keys someone was told a span held - TODO 749.
         */
        std::set<std::string> release(const std::string& owner, const std::set<std::string>& keys);
        /**
         * `owner` no longer waiting for a lock, when it gave up - TODO 724. The keys a
         * session parked on should be woken for, as release says.
         */
        std::set<std::string> stop_waiting(const std::string& owner);
        /** `owner` noted as waiting for a lock: for the tests */
        [[nodiscard]] bool waiting(const std::string& owner) const;
        /**
         * the transaction other than `owner` holding `key` (either way, since a write
         * would change what a shared holder read), or empty
         */
        std::string held_by_other(const std::string& owner, const std::string& key) const;
        /**
         * Wait until no transaction other than `owner` holds any of `keys`, up to
         * `until`, for a plain write. The one still in the way, or empty.
         */
        std::string wait_free(const std::string& owner, const std::set<std::string>& keys,
                              std::chrono::steady_clock::time_point until);
        /**
         * The form a lock names a key by: a key as encoded for routing, with no trailing
         * NUL, which a shard's stored keys carry (see route_key). Both a command's key
         * and a shard's come to the same lock through this.
         */
        static std::string key_of(const char* bytes, size_t size) {
            if (size > 1 && bytes[size - 1] == 0)
                --size;
            return {bytes, size};
        }
        /** keys and spans locked now: none means nothing to check */
        [[nodiscard]] bool any() const { return count.load(std::memory_order_acquire) > 0; }
        [[nodiscard]] size_t size() const { return count.load(std::memory_order_acquire); }

        /**
         * The shard check, made before a write a client command sends - TODO 719: throws
         * when another transaction holds `key`. It can't wait, since the shard is held
         * then. Only on a thread running a client's command (`guarding`), so eviction,
         * expiry, replication and a replay never meet it.
         */
        void check_write(const std::string& key) const;

        /** the transaction this thread writes for, whose own locks don't stop it */
        static const std::string& owner_now();
        /** a client's command running on this thread: its writes are checked */
        static bool guarding();

        struct owner_scope {
            explicit owner_scope(const std::string& id);
            ~owner_scope();
            owner_scope(const owner_scope&) = delete;
            owner_scope& operator=(const owner_scope&) = delete;
        private:
            const std::string* outer;
        };
        struct guard_scope {
            explicit guard_scope(bool on = true);
            ~guard_scope();
            guard_scope(const guard_scope&) = delete;
            guard_scope& operator=(const guard_scope&) = delete;
        private:
            bool outer;
        };

    private:
        struct entry {
            std::string exclusive;          // its owner, or empty
            std::set<std::string> shared;   // the owners of shared locks
            [[nodiscard]] bool free() const { return exclusive.empty() && shared.empty(); }
        };
        // the holder in the way of `owner` taking `e`, or empty
        static std::string in_way(const entry& e, const std::string& owner, bool exclusive);
        // in_way for `key`, a span another transaction holds over it included when exclusive
        std::string blocker(const std::string& key, const std::string& owner, bool exclusive) const;
        // the first key in `s` another transaction holds exclusively, and its holder
        std::pair<std::string, std::string> span_blocker(const span& s, const std::string& owner) const;
        // every holder in the way, for following waits
        static std::vector<std::string> all_in_way(const entry& e, const std::string& owner, bool exclusive);
        // the others waiting to write `key`, which a new shared lock waits behind - TODO 869
        std::vector<std::string> writers_waiting(const std::string& key, const std::string& owner) const;
        // `other` is a parked session waiting to write `key`
        bool parked_writer(const std::string& other, const std::string& key) const;
        /*
         * What each waiting transaction waits for - TODO 724. A transaction waits for one
         * lock at a time, since a command takes its keys one after another.
         */
        struct wait {
            std::string key;
            bool exclusive{false};
            bool span{false};       // a span's, which doesn't wait behind writers
            bool parked{false};     // a session parked on it, not a thread waiting in take
        };
        std::map<std::string, wait> waits;
        /*
         * `owner` no longer waiting, and a write it waited for noted in `writer_left`.
         * True when that was a write, which readers may wait behind.
         */
        bool unwait(const std::string& owner);
        /*
         * keys a waiting writer gave up on, which readers behind it may have parked on:
         * woken by the next release or stop_waiting - TODO 869
         */
        std::set<std::string> writer_left;
        // the ring `owner` waiting would close, from the first holder on; empty when none
        std::vector<std::string> ring_through(const std::string& owner) const;
        mutable std::mutex m;
        std::condition_variable freed;
        std::map<std::string, entry> held;
        std::map<std::string, std::vector<span>> spans;     // owner -> its spans - TODO 749
        size_t span_count{0};
        /*
         * keys someone was told a span holds, which may have parked on them: woken when a
         * span goes, since the span's owner never locked them by name - TODO 749
         */
        mutable std::set<std::string> span_blocked;
        void counted() { count.store(held.size() + span_count, std::memory_order_release); }
        std::atomic<size_t> count{0};
    };
}
