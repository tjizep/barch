//
// Created by barch on 16-09-2026.
//
#include "aof_log.h"

#include <algorithm>
#include <stdexcept>

namespace barch::aof {

    namespace {
        // a checkpoint's mark travels as its value, eight bytes little endian.
        // one from before TODO 452 has no value and meant "everything before
        // me", which is the sequence just under its own
        std::string encode_covers(uint64_t covers) {
            std::string v(8, '\0');
            for (int i = 0; i < 8; ++i) v[i] = (char) (uint8_t) (covers >> (8 * i));
            return v;
        }
        uint64_t covers_of(const record& r) {
            if (r.value.size() != 8)
                return r.sequence ? r.sequence - 1 : 0;
            uint64_t c = 0;
            for (int i = 0; i < 8; ++i) c |= (uint64_t) (uint8_t) r.value[i] << (8 * i);
            return c;
        }
    }

    log::log(const std::string& path, sync_policy sync)
        : queue(std::make_unique<queue_file>(path, sync)) {
        // carry on from the highest sequence already in the file. a log whose
        // tail is torn stops the scan, so the sequence continues from the last
        // record that verified rather than from a number read out of rubbish
        const auto s = scan_locked();       // nothing else can see it yet
        sequence = s.highest_sequence + 1;
        covered = s.found ? s.covers : 0;
        /*
         * A record that doesn't verify is cut off here, with everything after
         * it - TODO 466. Left in place, it's where every walk of the file stops,
         * so the writes appended after it would never be replayed and the
         * checkpoints after it never seen, which means never trimmed either.
         * What's after it can't be trusted anyway; the replay was already
         * stopping there.
         */
        if (s.stopped_early && s.why == decoded::unknown_type) {
            /*
             * Not torn: a whole record from a newer build - TODO 478. Cutting it
             * would destroy it and everything after it, and appending behind it
             * would hide what's appended, so this log isn't used at all. The
             * space runs without one and says so; the file is left for the build
             * that wrote it.
             */
            throw std::runtime_error("the change log " + queue->name() + " holds a record type"
                                     " this build doesn't know, written by a newer barch -"
                                     " leaving it as it is and not logging this space");
        }
        if (s.stopped_early) {
            opened.stopped_early = true;
            opened.why = s.why;
            opened.at_sequence = s.highest_sequence;
            opened.records = queue->size() - s.count;
            queue->truncate(s.count);
            queue->sync();                  // before anything lands behind it
        }
    }

    log::~log() = default;

    thread_local log::reservation* log::active = nullptr;

    log::reservation::reservation(log& owner, uint64_t bytes) : owner(owner) {
        std::lock_guard lock(owner.mut);
        owner.queue->reserve(bytes);    // throws holding nothing
        remaining = bytes;
        previous = active;
        active = this;
    }

    log::reservation::~reservation() {
        std::lock_guard lock(owner.mut);
        owner.queue->release(remaining);
        active = previous;
    }

    uint64_t log::append_locked(record& r) {
        r.sequence = sequence;
        std::vector<uint8_t> buffer;
        encode(r, buffer);
        // an append on the thread holding room here draws on it; any other has
        // to leave that room free - TODO 471
        reservation* mine = nullptr;
        for (reservation* at = active; at && !mine; at = at->previous) {
            if (&at->owner == this)
                mine = at;
        }
        const uint64_t from_mine = mine
            ? std::min<uint64_t>(mine->remaining, queue_file::element_header_length + buffer.size())
            : 0;
        queue->add(buffer, from_mine);
        if (mine)
            mine->remaining -= from_mine;
        return sequence++;
    }

    uint64_t log::append_set(const std::string& space, const std::string& key,
                             const std::string& value, int64_t expiry_ms,
                             uint8_t options, uint32_t shard, uint32_t shard_count) {
        record r;
        r.type = record_type::set;
        r.space = space;
        r.key = key;
        r.value = value;
        r.expiry_ms = expiry_ms;
        r.options = options;
        r.shard = shard;
        r.shard_count = shard_count;
        std::lock_guard lock(mut);
        return append_locked(r);
    }

    uint64_t log::append_erase(const std::string& space, const std::string& key,
                               uint32_t shard, uint32_t shard_count) {
        record r;
        r.type = record_type::erase;
        r.space = space;
        r.key = key;
        r.shard = shard;
        r.shard_count = shard_count;
        std::lock_guard lock(mut);
        return append_locked(r);
    }

    uint64_t log::append_clear(const std::string& space, uint32_t shard_count) {
        record r;
        r.type = record_type::clear;
        r.space = space;
        r.shard_count = shard_count;
        std::lock_guard lock(mut);
        return append_locked(r);
    }

    uint64_t log::mark() const {
        std::lock_guard lock(mut);
        return sequence - 1;
    }

    uint64_t log::checkpoint(const std::string& space, uint64_t covers) {
        record r;
        r.type = record_type::checkpoint;
        r.space = space;
        std::lock_guard lock(mut);
        /*
         * It can't cover a write that hasn't been handed a sequence yet. And it
         * never covers less than the last one: the shard files only move forward,
         * so a SAVE that took its mark before a LOAD and finishes after it would
         * otherwise hand the replay the writes the LOAD threw away - TODO 479.
         */
        covered = std::max(covered, std::min(covers, sequence - 1));
        r.value = encode_covers(covered);
        const uint64_t at = append_locked(r);
        /*
         * Synced regardless of the durability setting. Every other record is a
         * write that can be lost; this one is a claim about every write before
         * it, so a checkpoint that did not reach the device would have a replay
         * skipping records that are still the only copy of themselves.
         */
        queue->sync();
        return at;
    }

    uint64_t log::checkpoint(const std::string& space) {
        return checkpoint(space, mark());
    }

    log::scan log::scan_locked() const {
        scan out;
        uint32_t index = 0;
        queue->for_each([&](const uint8_t* data, uint32_t size) {
            record r;
            const decoded why = decode(data, size, r);
            if (why != decoded::ok) {
                out.stopped_early = true;
                out.why = why;
                return false;           // and nothing beyond it is trusted
            }
            out.highest_sequence = std::max(out.highest_sequence, r.sequence);
            if (r.type == record_type::checkpoint) {
                out.found = true;
                out.index = index;
                out.covers = covers_of(r);
                out.sequence = r.sequence;
            }
            ++index;
            ++out.count;
            return true;
        });
        return out;
    }

    log::outcome log::replay(const std::function<void(const record&)>& apply) const {
        std::lock_guard lock(mut);
        const scan s = scan_locked();

        outcome out;
        out.stopped_early = s.stopped_early;
        out.why = s.why;

        /*
         * Everything past what the last checkpoint covers, or everything when
         * there is none. That can include writes from before the checkpoint:
         * the ones made while the save was running, which a shard saved
         * earlier doesn't have - TODO 452.
         */
        const uint64_t covers = s.found ? s.covers : 0;
        queue->for_each([&](const uint8_t* data, uint32_t size) {
            record r;
            const decoded why = decode(data, size, r);
            if (why != decoded::ok) {
                out.stopped_early = true;
                out.why = why;
                return false;
            }
            if (r.type == record_type::checkpoint || r.sequence <= covers)
                return true;
            apply(r);
            out.at_sequence = r.sequence;
            ++out.records;
            return true;
        });
        return out;
    }

    log::outcome log::trim_to_last_checkpoint() {
        std::lock_guard lock(mut);
        const scan s = scan_locked();
        outcome out;
        out.stopped_early = s.stopped_early;
        out.why = s.why;
        if (!s.found)
            return out;               // nothing is known to be saved, so nothing goes

        /*
         * The front of the file, for as long as the records are covered.
         * Sequences are handed out and appended under one lock, so file order
         * is sequence order and the covered records are all at the front.
         *
         * The checkpoint goes too when it's next, since it's a statement about
         * what came before it and that is gone. When writes landed while the
         * save ran it stays where it is, behind them, and the next trim takes
         * it: its own sequence is under the next save's mark.
         */
        uint32_t drop = 0;
        queue->for_each([&](const uint8_t* data, uint32_t size) {
            record r;
            if (decode(data, size, r) != decoded::ok)
                return false;
            if (r.sequence <= s.covers
                || (r.type == record_type::checkpoint && r.sequence == s.sequence)) {
                ++drop;
                return true;
            }
            return false;
        });
        if (drop)
            queue->remove(drop);
        out.records = drop;
        return out;
    }

    void log::sync() const {
        std::lock_guard lock(mut);
        queue->sync();
    }
}
