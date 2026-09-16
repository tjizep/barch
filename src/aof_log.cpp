//
// Created by barch on 16-09-2026.
//
#include "aof_log.h"

#include <algorithm>

namespace barch::aof {

    log::log(const std::string& path, sync_policy sync)
        : queue(std::make_unique<queue_file>(path, sync)) {
        // carry on from the highest sequence already in the file. a log whose
        // tail is torn stops the scan, so the sequence continues from the last
        // record that verified rather than from a number read out of rubbish
        const auto s = scan_locked();       // nothing else can see it yet
        sequence = s.highest_sequence + 1;
    }

    log::~log() = default;

    uint64_t log::append_locked(record& r) {
        r.sequence = sequence;
        std::vector<uint8_t> buffer;
        encode(r, buffer);
        queue->add(buffer);
        return sequence++;
    }

    uint64_t log::append_set(const std::string& space, const std::string& key,
                             const std::string& value, int64_t expiry_ms) {
        record r;
        r.type = record_type::set;
        r.space = space;
        r.key = key;
        r.value = value;
        r.expiry_ms = expiry_ms;
        std::lock_guard lock(mut);
        return append_locked(r);
    }

    uint64_t log::append_erase(const std::string& space, const std::string& key) {
        record r;
        r.type = record_type::erase;
        r.space = space;
        r.key = key;
        std::lock_guard lock(mut);
        return append_locked(r);
    }

    uint64_t log::checkpoint(const std::string& space) {
        record r;
        r.type = record_type::checkpoint;
        r.space = space;
        std::lock_guard lock(mut);
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

        // everything after the last checkpoint, or everything when there is none
        const uint32_t from = s.found ? s.index + 1 : 0;
        uint32_t index = 0;
        queue->for_each([&](const uint8_t* data, uint32_t size) {
            if (index++ < from)
                return true;
            record r;
            const decoded why = decode(data, size, r);
            if (why != decoded::ok) {
                out.stopped_early = true;
                out.why = why;
                return false;
            }
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

        // the checkpoint itself goes too: it is a statement about what came
        // before it, and what came before it is no longer here
        const uint32_t drop = s.index + 1;
        queue->remove(drop);
        out.records = drop;
        return out;
    }

    void log::sync() const {
        std::lock_guard lock(mut);
        queue->sync();
    }
}
