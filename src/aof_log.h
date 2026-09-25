//
// Created by barch on 16-09-2026.
//
#ifndef BARCH_AOF_LOG_H
#define BARCH_AOF_LOG_H

#include "aof_record.h"
#include "queue_file.h"

#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace barch::aof {
    /**
     * A key space's change history - TODO 354.
     *
     * A checkpoint record says which writes are in the shard files: every
     * record up to the sequence it carries (`covers`). That one sentence
     * decides the rest:
     *
     *   - a save notes `mark()` before it starts, and writes a checkpoint
     *     covering that mark after it has completed, never before;
     *   - a replay loads the shard files and then applies every record with a
     *     sequence past the last checkpoint's `covers`;
     *   - every record up to `covers` can be dropped, and dropping it is what
     *     bounds the file - `queue_file` is a ring that doubles and only
     *     shrinks when something removes from it.
     *
     * Why the mark and not "everything before the checkpoint": a hash-sharded
     * space is saved one shard after another while writes carry on. A write to
     * a shard that was already saved lands in the log before the checkpoint but
     * not in any file, so a checkpoint meaning "everything before me" trimmed
     * it and a crash lost it - TODO 452. Writes past the mark are replayed even
     * when a shard file already has them, which is harmless: a `set` of a value
     * already there and an `erase` of a key already gone change nothing.
     *
     * A checkpoint written by an older build carries no mark, and is read as
     * covering everything before it, which is what it meant then.
     *
     * Sequences are found rather than stored: opening walks what is there and
     * carries on from the highest it saw, which is one less thing to keep
     * consistent across a restart.
     *
     * Thread safe, unlike the `queue_file` underneath it, because one log per
     * key space means every shard of that space appends to the same file - and
     * the append happens while the shard's own write latch is held. So there is
     * a mutex here, and it serialises writes across the whole space.
     *
     * That is worth saying plainly rather than discovering: with
     * `aof_durability = each` a write waits for a device round trip inside the
     * shard latch, and every other shard waits behind it. The lighter settings
     * turn the same append into a copy into the page cache. If the serialisation
     * is too expensive the answer is per shard staging flushed here in batches,
     * which keeps the latch short without going to a file per shard - see
     * TODO 355.
     */
    class log {
        mutable std::mutex mut;

    public:
        /** how a replay or a trim ended */
        struct outcome {
            uint32_t records{0};        // records handed to the callback, or dropped
            bool stopped_early{false};  // true when a record did not verify
            decoded why{decoded::ok};   // and how it failed
            uint64_t at_sequence{0};    // the last good sequence seen
        };

        log(const std::string& path, sync_policy sync);
        ~log();

        log(const log&) = delete;
        log& operator=(const log&) = delete;

        /** append a value for a key. returns the sequence it was given */
        /**
         * Append a value for a key. `options` is the `art::key_options` flags
         * byte the write was made with, and it has to travel: without
         * `flag_is_compressed` a replay would store compressed bytes as a plain
         * value - see the note in aof_record.h.
         */
        uint64_t append_set(const std::string& space, const std::string& key,
                            const std::string& value, int64_t expiry_ms = 0,
                            uint8_t options = 0, uint32_t shard = 0,
                            uint32_t shard_count = 0);
        /** append the removal of a key */
        uint64_t append_erase(const std::string& space, const std::string& key,
                              uint32_t shard = 0, uint32_t shard_count = 0);

        /**
         * The last sequence handed out so far, 0 when there is none. A save
         * takes this before it starts: every write up to it is in memory by
         * then, so the shard files the save writes will hold it.
         */
        [[nodiscard]] uint64_t mark() const;

        /**
         * Write a checkpoint: every record up to `covers` is in the shard files.
         *
         * Call it after the save has finished, with the mark taken before the
         * save started. A checkpoint that claims more than the save wrote is a
         * lie that survives a crash, and a replay believing it would skip
         * records that never reached a shard file.
         *
         * It syncs, whatever the durability setting says - a checkpoint that has
         * not reached the device is the one record whose loss is not a lost
         * write but a wrong answer about every write before it.
         */
        uint64_t checkpoint(const std::string& space, uint64_t covers);

        /**
         * A checkpoint covering everything written so far. Only right when
         * nothing can have written since the save began - tests, mostly.
         */
        uint64_t checkpoint(const std::string& space);

        /**
         * Apply every record past the last checkpoint's `covers`, eldest first.
         * Checkpoints themselves are never handed over.
         *
         * Stops at the first record that does not verify and says so in the
         * outcome: under a weak durability setting a torn record sits at the end
         * of the log, which is exactly where a partial write lands, and anything
         * after it is suspect.
         */
        outcome replay(const std::function<void(const record&)>& apply) const;

        /**
         * Drop every record the last checkpoint covers, and the checkpoint too
         * when nothing was written between its mark and it. Nothing is dropped
         * when there is no checkpoint, since without one nothing is known to be
         * saved.
         */
        outcome trim_to_last_checkpoint();

        /** push what has been written to the device, whatever the policy says */
        void sync() const;

        // these read state an appending thread is changing, so they take the
        // same lock rather than being cheap and wrong
        [[nodiscard]] uint64_t next_sequence() const {
            std::lock_guard lock(mut);
            return sequence;
        }
        [[nodiscard]] uint32_t records() const {
            std::lock_guard lock(mut);
            return queue->size();
        }
        [[nodiscard]] uint64_t file_bytes() const {
            std::lock_guard lock(mut);
            return queue->file_bytes();
        }
        [[nodiscard]] uint64_t unsynced_bytes() const {
            std::lock_guard lock(mut);
            return queue->unsynced_bytes();
        }

    private:
        std::unique_ptr<queue_file> queue;
        uint64_t sequence{1};

        /** all of these want `mut` already held */
        uint64_t append_locked(record& r);
        /**
         * The last checkpoint, and how the walk ended.
         *
         * A checkpoint found after a record that did not verify is not
         * reported: it may not be a checkpoint at all, and trusting it would
         * drop records that are still the only copy.
         */
        struct scan {
            bool found{false};
            uint32_t index{0};
            uint64_t covers{0};         // what the last checkpoint says is saved
            uint64_t sequence{0};       // the last checkpoint's own sequence
            uint32_t count{0};
            bool stopped_early{false};
            decoded why{decoded::ok};
            uint64_t highest_sequence{0};
        };
        [[nodiscard]] scan scan_locked() const;
    };
}

#endif //BARCH_AOF_LOG_H
