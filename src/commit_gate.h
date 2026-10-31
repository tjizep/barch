#pragma once
//
// A space's commit gate - TODO 775. Read commands and BEGINTX hold it shared, a commit
// that touches only some shards holds it in commit mode, and anything that needs the
// space to itself holds it alone.
//
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstddef>
#include <mutex>
#include <vector>

namespace barch {
    /**
     * Shared holders go together, and so do commit holders, but never one of each, and
     * an alone holder goes with nobody. Commits in commit mode keep out of each other
     * with their shards' write latches, so two on different shards run at once and two
     * on the same shard take turns there.
     *
     * Shared holders come first, as they did with the std::shared_mutex this replaced
     * (glibc prefers readers): one comes in whenever no commit or alone holder is in,
     * so a thread holding it shared can take it shared again. A commit doesn't come in
     * while a shared or alone holder is waiting, so commits overlapping without a gap
     * can't keep the reads or a whole space commit out for good.
     */
    class commit_gate {
    public:
        commit_gate() = default;
        commit_gate(const commit_gate&) = delete;
        commit_gate& operator=(const commit_gate&) = delete;

        void lock_shared() {
            std::unique_lock l(m);
            if (alone || committers > 0) {
                ++waiting_readers;
                reads_waiting_now().fetch_add(1, std::memory_order_relaxed);
                changed.wait(l, [this] { return !alone && committers == 0; });
                reads_waiting_now().fetch_sub(1, std::memory_order_relaxed);
                --waiting_readers;
            }
            ++readers;
        }
        /**
         * reads waiting at a gate now, in every space: INFO transactions'
         * tx_gate_reads_waiting, so a test sees a read stopped by a commit - TODO 800
         */
        static std::atomic<int64_t>& reads_waiting_now() {
            static std::atomic<int64_t> n{0};
            return n;
        }
        void unlock_shared() {
            std::lock_guard l(m);
            if (--readers == 0 && (waiting_committers > 0 || waiting_alone > 0))
                changed.notify_all();
        }
        void lock_commit() {
            std::unique_lock l(m);
            if (!commit_may_enter()) {
                ++waiting_committers;
                changed.wait(l, [this] { return commit_may_enter(); });
                --waiting_committers;
            }
            ++committers;
        }
        void unlock_commit() {
            std::lock_guard l(m);
            if (--committers == 0 && (waiting_readers > 0 || waiting_alone > 0 || waiting_committers > 0))
                changed.notify_all();
        }
        void lock() {
            std::unique_lock l(m);
            if (alone || readers > 0 || committers > 0) {
                ++waiting_alone;
                changed.wait(l, [this] { return !alone && readers == 0 && committers == 0; });
                --waiting_alone;
            }
            alone = true;
        }
        void unlock() {
            std::lock_guard l(m);
            alone = false;
            if (waiting_readers > 0 || waiting_alone > 0 || waiting_committers > 0)
                changed.notify_all();
        }

        /** a commit's hold, alone or in commit mode, let go when it goes */
        class hold {
        public:
            explicit hold(commit_gate& gate) : gate(gate) {}
            ~hold() {
                if (taken == how::commit) gate.unlock_commit();
                else if (taken == how::alone) gate.unlock();
            }
            hold(const hold&) = delete;
            hold& operator=(const hold&) = delete;
            void take(bool all) {
                if (all) gate.lock();
                else gate.lock_commit();
                taken = all ? how::alone : how::commit;
            }
            /** let go before it goes */
            void let_go() {
                if (taken == how::commit) gate.unlock_commit();
                else if (taken == how::alone) gate.unlock();
                taken = how::none;
            }
        private:
            enum class how { none, commit, alone };
            commit_gate& gate;
            how taken{how::none};
        };

    private:
        // under m
        [[nodiscard]] bool commit_may_enter() const {
            return !alone && readers == 0 && waiting_readers == 0 && waiting_alone == 0;
        }
        std::mutex m;
        std::condition_variable changed;
        size_t readers{0};
        size_t committers{0};
        bool alone{false};
        size_t waiting_readers{0};
        size_t waiting_committers{0};
        size_t waiting_alone{0};
    };

    /**
     * Which shards of a space a commit is using, from taking their latches until it has
     * synced and let them go - TODO 775. A commit claims its shards while holding the
     * gate, and if one is claimed already, lets the gate go and waits for it outside.
     * Waiting at the latch instead would keep the gate through the other commit's sync,
     * and BEGINTX and the reads with it. While a commit waits for every shard, no other
     * claims one, or claims overlapping without a gap could keep it out for good.
     */
    class shard_claims {
    public:
        shard_claims() = default;
        shard_claims(const shard_claims&) = delete;
        shard_claims& operator=(const shard_claims&) = delete;

        /**
         * numbers sorted; every shard if all. True if they're all now claimed. wanting
         * is the caller's, false to start: set while it waits for every shard, and
         * given up by a claim or by give_up
         */
        template<typename Numbers>
        bool try_claim(const Numbers& numbers, bool all, bool& wanting) {
            std::lock_guard l(m);
            if (wanting) {
                --wanting_whole;
                wanting = false;
            }
            if (!free(numbers, all)) {
                if (all) {
                    ++wanting_whole;
                    wanting = true;
                }
                return false;
            }
            if (all) {
                whole = true;
                return true;
            }
            for (auto n : numbers) {
                if (n >= claimed.size())
                    claimed.resize(n + 1, false);
                claimed[n] = true;
            }
            some += numbers.size();
            return true;
        }
        template<typename Numbers>
        void wait_free(const Numbers& numbers, bool all) {
            std::unique_lock l(m);
            changed.wait(l, [&] { return free(numbers, all); });
        }
        /** for a commit that leaves while wanting every shard */
        void give_up(bool& wanting) {
            if (!wanting)
                return;
            {
                std::lock_guard l(m);
                --wanting_whole;
                wanting = false;
            }
            changed.notify_all();
        }
        template<typename Numbers>
        void release(const Numbers& numbers, bool all) {
            {
                std::lock_guard l(m);
                if (all) {
                    whole = false;
                } else {
                    for (auto n : numbers)
                        claimed[n] = false;
                    some -= numbers.size();
                }
            }
            changed.notify_all();
        }

    private:
        // under m
        template<typename Numbers>
        [[nodiscard]] bool free(const Numbers& numbers, bool all) const {
            if (whole)
                return false;
            if (all)
                return some == 0;
            if (wanting_whole > 0)
                return false;
            for (auto n : numbers)
                if (n < claimed.size() && claimed[n])
                    return false;
            return true;
        }
        std::mutex m;
        std::condition_variable changed;
        std::vector<bool> claimed;
        size_t some{0};         // how many are claimed
        bool whole{false};      // every shard, by one commit
        size_t wanting_whole{0}; // commits waiting for every shard
    };
}
