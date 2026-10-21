#pragma once
//
// A space's versions for its transactions' snapshots: before-images, an undo log - TODO 701.
// See docs/TRANSACTIONS.md, "Chosen shape: before-images".
//
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include "abstract_shard.h"
#include "sastam.h"

namespace barch {
    class key_space;

    /**
     * The base stays current. While any transaction is open on the space, a write first
     * saves what its key held (or that it wasn't there) in the layer of the current
     * version, once per key per version. BEGINTX takes a new version as its snapshot, so
     * layer v holds what keys were before their first change after snapshot v. A snapshot
     * s reads the layers from s up, oldest first, then the base. A layer older than every
     * open snapshot is dropped.
     *
     * A layer is a space with the base's shard count and key split, so a key's shard
     * number is the same in both, and a layer shard is only written by the writers of the
     * base shard with that number, under that shard's write lock.
     */
    struct tx_versions {
        explicit tx_versions(std::weak_ptr<key_space> base) : base(std::move(base)) {}

        /** a snapshot, taken by BEGINTX */
        uint64_t begin();
        /** that snapshot let go of, by COMMITTX, ABORTTX or a session that goes */
        void end(uint64_t snapshot);
        /**
         * COMMITTX's hold on its snapshot, so it can't expire while the commit checks
         * against its layers - TODO 701. False when it already has.
         */
        bool claim(uint64_t snapshot);
        /** whether `snapshot` was let go of for being open too long */
        bool expired(uint64_t snapshot);
        /**
         * The snapshots open longer than `max_age_ms`, and not claimed, let go of, so the
         * layers they held can go - TODO 701: a forgotten session can't pin them for
         * ever. Its transaction finds out at its next command. How many it let go.
         */
        size_t expire(uint64_t max_age_ms);
        /** any snapshot open: the one thing a write asks before it saves anything */
        [[nodiscard]] bool versioning() const { return any_open.load(std::memory_order_acquire); }
        /** the current version's layer shard `shard`, made on first use; null with none open */
        shard_ptr layer_shard(size_t shard);
        /** the layer shards `shard` from snapshot `s` up, oldest first */
        heap::vector<shard_ptr> layers_from(uint64_t s, size_t shard);
        /** how many layers there are now */
        [[nodiscard]] size_t layer_count();
        /**
         * Whether `key` (as stored) in shard `shard` changed after snapshot `s`: a layer
         * from `s` up holds it - TODO 701. A key's every change while a snapshot is open
         * leaves it there.
         */
        bool changed_since(uint64_t s, size_t shard, art::value_type key);
        /** whether any key in [lo, hi) (an empty hi has no end) changed after `s`, in any shard */
        bool changed_in(uint64_t s, art::value_type lo, art::value_type hi);
        /**
         * A clear or a load of one of the space's shards, which aren't per-key writes and
         * leave nothing in the layers: a snapshot taken before one conflicts.
         */
        void note_clear() { clears.fetch_add(1, std::memory_order_acq_rel); }
        [[nodiscard]] uint64_t clears_now() const { return clears.load(std::memory_order_acquire); }

    private:
        std::weak_ptr<key_space> base;
        std::mutex mu;
        std::atomic<bool> any_open{false};
        std::atomic<uint64_t> clears{0};
        uint64_t current{0};
        // each open snapshot and when it was taken
        std::map<uint64_t, std::chrono::steady_clock::time_point> open;
        std::set<uint64_t> claimed;
        std::set<uint64_t> gone;        // expired, until their sessions end them
        /** under mu: the layers no open snapshot reads, into `dropped` */
        void trim(std::map<uint64_t, std::shared_ptr<key_space>>& dropped);
        std::map<uint64_t, std::shared_ptr<key_space>> layers;
    };
}
