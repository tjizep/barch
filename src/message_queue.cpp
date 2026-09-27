//
// See message_queue.h - TODO 366.
//
#include "message_queue.h"

#include "aof_record.h"     // crc32c
#include "lzr_log.h"

#include <algorithm>
#include <cstring>
#include <unistd.h>

namespace message_queue_anon {
    constexpr uint8_t record_version = 1;
    constexpr uint8_t marker_version = 2;       // TODO 511
    constexpr size_t header_size = 16;
    constexpr size_t marker_size = header_size + 4 + 8;

    void mq_put_u32(uint8_t* b, uint32_t v) {
        b[0] = (uint8_t) (v & 0xFF);
        b[1] = (uint8_t) ((v >> 8) & 0xFF);
        b[2] = (uint8_t) ((v >> 16) & 0xFF);
        b[3] = (uint8_t) ((v >> 24) & 0xFF);
    }
    void mq_put_u64(uint8_t* b, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            b[i] = (uint8_t) ((v >> (i * 8)) & 0xFF);
    }
    uint32_t mq_get_u32(const uint8_t* b) {
        return (uint32_t) b[0] | ((uint32_t) b[1] << 8)
               | ((uint32_t) b[2] << 16) | ((uint32_t) b[3] << 24);
    }
    uint64_t mq_get_u64(const uint8_t* b) {
        uint64_t v = 0;
        for (int i = 7; i >= 0; --i)
            v = (v << 8) | b[i];
        return v;
    }

    /** a marker's fields - TODO 511 */
    struct marker {
        uint64_t about{0};          // the message it's about
        uint32_t started{0};        // deliveries of it begun, this one included
        uint64_t highest{0};        // the highest sequence handed out
    };

    void encode_marker(const marker& k, std::vector<uint8_t>& into) {
        into.assign(marker_size, 0);
        into[4] = marker_version;
        mq_put_u64(into.data() + 8, k.about);
        mq_put_u32(into.data() + header_size, k.started);
        mq_put_u64(into.data() + header_size + 4, k.highest);
        mq_put_u32(into.data(), barch::aof::crc32c(into.data() + 4, into.size() - 4));
    }

    /** a record that verified: a message, or a marker */
    enum class kind { message, marker, bad };

    kind classify(const uint8_t* data, size_t size, barch::mq::message& m, marker& k) {
        if (size == marker_size && data[4] == marker_version
            && mq_get_u32(data) == barch::aof::crc32c(data + 4, size - 4)) {
            k.about = mq_get_u64(data + 8);
            k.started = mq_get_u32(data + header_size);
            k.highest = mq_get_u64(data + header_size + 4);
            return kind::marker;
        }
        return barch::mq::decode(data, size, m) == barch::mq::decoded::ok ? kind::message : kind::bad;
    }
} // namespace message_queue_anon

using namespace message_queue_anon;

namespace barch::mq {

    sync_policy policy_of(const aof_sync_setting& setting) {
        switch (setting.mode) {
            case aof_sync_setting::none:  return {sync_when::never, 0};
            case aof_sync_setting::timer: return {sync_when::on_demand, 0};
            case aof_sync_setting::each:  return {sync_when::each_add, 0};
            case aof_sync_setting::bytes: return {sync_when::after_bytes, setting.threshold};
        }
        return {sync_when::on_demand, 0};
    }

    const char* describe(decoded d) {
        switch (d) {
            case decoded::ok: return "ok";
            case decoded::too_short: return "shorter than a record header";
            case decoded::bad_version: return "a record version this build does not know";
            case decoded::bad_checksum: return "the record checksum does not match";
        }
        return "unknown";
    }

    void encode(uint64_t sequence, const std::string& data, std::vector<uint8_t>& into) {
        into.assign(header_size + data.size(), 0);
        into[4] = record_version;
        mq_put_u64(into.data() + 8, sequence);
        std::memcpy(into.data() + header_size, data.data(), data.size());
        // the checksum covers everything after itself, so it is written last
        mq_put_u32(into.data(), aof::crc32c(into.data() + 4, into.size() - 4));
    }

    decoded decode(const uint8_t* data, size_t size, message& into) {
        if (size < header_size)
            return decoded::too_short;
        if (data[4] != record_version)
            return decoded::bad_version;
        // no length field to cross check - the payload is whatever follows the
        // header - so the checksum is the whole of the verification here, unlike
        // aof_record where the key and value lengths are checked against the size
        if (mq_get_u32(data) != aof::crc32c(data + 4, size - 4))
            return decoded::bad_checksum;
        into.sequence = mq_get_u64(data + 8);
        into.attempts = 0;
        into.data.assign((const char*) data + header_size, size - header_size);
        return decoded::ok;
    }

    queue::queue(const std::string& path, sync_policy sync, uint32_t max_attempts)
        : path(path), policy(sync),
          max_attempts(max_attempts ? max_attempts : 1),
          file(std::make_unique<queue_file>(path, sync)) {
        /*
         * Carry on from the highest sequence in the file rather than storing the
         * next one anywhere else. Markers count too, which is what keeps a queue
         * that drained from starting again at 1 - TODO 511.
         *
         * A record that doesn't verify gives nothing, and what follows it
         * decides whether anything after it can be trusted - TODO 515. Below
         * `each` it's just as likely that the length in front of a record is
         * what's wrong as its payload, and then every record after it is read
         * from the wrong place. So:
         *   - if the next record verifies, the length was right, and only the
         *     bad one's payload is lost. A record read from the wrong place
         *     passes its checksum about once in 2^32. `peek` drops the bad one
         *     when it gets to the head.
         *   - if nothing after it verifies - another bad record, a length that
         *     can't fit (read_element throws), or the end of the file - the
         *     file is cut back to just before it, the way aof::log does.
         * DONE 479 walked on past every bad record, which followed a wrong
         * length into misframed records, and threw on one that couldn't fit.
         */
        constexpr uint32_t none = UINT32_MAX;
        uint64_t highest = 0;
        uint64_t head = 0;
        bool have_head = false;
        uint32_t index = 0;
        uint32_t cut = none;            // the first bad record not yet followed by a good one
        std::string why;
        std::unordered_map<uint64_t, uint32_t> started_of;
        try {
            file->for_each([&](const uint8_t* d, uint32_t n) {
                message m;
                marker k;
                switch (classify(d, n, m, k)) {
                    case kind::message:
                        cut = none;
                        highest = std::max(highest, m.sequence);
                        if (!have_head) {
                            head = m.sequence;
                            have_head = true;
                        }
                        break;
                    case kind::marker:
                        cut = none;
                        ++markers;
                        highest = std::max({highest, k.about, k.highest});
                        started_of[k.about] = std::max(started_of[k.about], k.started);
                        break;
                    case kind::bad:
                        if (cut == none) {
                            cut = index;
                            why = "a record that doesn't verify";
                        }
                        break;
                }
                ++index;
                return true;
            });
        } catch (const std::exception& e) {
            if (cut == none) {
                cut = index;            // this one's length can't frame
                why = e.what();
            }
        }
        if (cut != none) {
            const uint32_t lost = file->size() - cut;
            file->truncate(cut);
            file->sync();               // before anything lands behind it
            barch::err({"queue", path, "was cut back to the", (uint64_t) cut,
                        "records before", why, "-", (uint64_t) lost,
                        "records from there on were dropped"});
        }
        sequence = highest + 1;
        // a dead letter queue from before this start: made on first use, but one
        // already on disk has to be seen, or STATUS says nothing's dead - TODO 511
        if (::access((path + ".dead").c_str(), F_OK) == 0)
            dead_file();
        /*
         * Deliveries of the head that started and never reported. Each of them
         * failed as far as anyone can tell: a success would have removed it.
         * Only the head: nothing behind it has been handed out yet.
         */
        if (have_head) {
            if (auto at = started_of.find(head); at != started_of.end() && at->second > 0)
                attempts[head] = at->second;
        }
    }

    queue::~queue() = default;

    queue_file& queue::dead_file() const {
        // not created until something is actually dead lettered, so a queue that
        // never fails leaves no second file lying around
        if (!dead)
            dead = std::make_unique<queue_file>(path + ".dead", policy);
        return *dead;
    }

    void queue::append_marker_locked(uint64_t about, uint32_t started) {
        std::vector<uint8_t> buffer;
        encode_marker(marker{about, started, sequence - 1}, buffer);
        file->add(buffer);
        ++markers;
    }

    void queue::skip_markers_locked() {
        std::vector<uint8_t> raw;
        while (file->size() > 1 && file->peek(raw)) {
            message m;
            marker k;
            if (classify(raw.data(), raw.size(), m, k) != kind::marker)
                return;
            file->remove();
            --markers;
        }
    }

    void queue::drop_head_locked() {
        // the last record is what the next open takes the sequence from, so it
        // can't be a message that's about to go - TODO 511
        if (file->size() == 1) {
            std::vector<uint8_t> raw;
            message m;
            marker k;
            if (file->peek(raw) && classify(raw.data(), raw.size(), m, k) == kind::message)
                append_marker_locked(m.sequence, 0);
        }
        file->remove();
    }

    void queue::dead_letter_locked(const message& m, uint32_t tried, const char* why) {
        std::vector<uint8_t> buffer;
        encode(m.sequence, m.data, buffer);
        dead_file().add(buffer);
        dead_file().sync();             // it is the only copy now
        attempts.erase(m.sequence);
        drop_head_locked();
        barch::err({"queue", path, "gave up on message", m.sequence, "after", tried,
                    why, "- it is in the dead letter queue"});
    }

    void queue::set_max_attempts(uint32_t n) {
        std::lock_guard lock(mut);
        max_attempts = n ? n : 1;
    }

    void queue::set_policy(sync_policy sync) {
        std::lock_guard lock(mut);
        if (sync.when == policy.when && sync.bytes == policy.bytes)
            return;
        // what's written so far goes down under the old policy, then the new
        // handle replaces the old one only once it has opened. Two handles for a
        // moment is fine - it's one queue, and opening an existing file only
        // reads its header; it's two queue objects on one file that isn't
        file->sync();
        auto reopened = std::make_unique<queue_file>(path, sync);
        std::unique_ptr<queue_file> reopened_dead;
        if (dead) {
            dead->sync();
            reopened_dead = std::make_unique<queue_file>(path + ".dead", sync);
        }
        file = std::move(reopened);
        if (reopened_dead)
            dead = std::move(reopened_dead);
        policy = sync;
    }

    void queue::started(const message& m) {
        std::lock_guard lock(mut);
        const auto at = attempts.find(m.sequence);
        append_marker_locked(m.sequence, (at == attempts.end() ? 0 : at->second) + 1);
    }

    uint64_t queue::publish(const std::string& data) {
        std::vector<uint8_t> buffer;
        std::lock_guard lock(mut);
        const uint64_t seq = sequence++;
        encode(seq, data, buffer);
        file->add(buffer);
        return seq;
    }

    bool queue::peek_locked(message& into) {
        std::vector<uint8_t> raw;
        skip_markers_locked();
        while (file->peek(raw)) {
            marker k;
            const kind what = classify(raw.data(), raw.size(), into, k);
            if (what == kind::marker) {
                if (file->size() == 1)
                    return false;       // only the sequence's keeper is left
                file->remove();
                --markers;
                continue;
            }
            if (what == kind::message) {
                const auto at = attempts.find(into.sequence);
                into.attempts = at == attempts.end() ? 0 : at->second;
                /*
                 * Out of attempts before it's even handed over: deliveries that
                 * started before a restart and never finished - TODO 511. The
                 * usual cause is a handler that took the process down with it.
                 */
                if (into.attempts >= max_attempts) {
                    dead_letter_locked(into, into.attempts, "deliveries, the last of them never finished");
                    continue;
                }
                return true;
            }
            const auto why = decode(raw.data(), raw.size(), into);
            /*
             * A record that does not verify is not a message. It cannot be
             * retried into existence and it cannot be dead lettered either -
             * there is nothing to put in the dead letter queue - so it is
             * dropped, and said out loud, because the usual cause is a crash
             * under a durability setting weaker than `each`.
             */
            barch::err({"queue", path, "dropped a record that did not verify:",
                        describe(why), "-", (uint64_t) raw.size(), "bytes"});
            file->remove();
        }
        return false;
    }

    bool queue::peek(message& into) {
        std::lock_guard lock(mut);
        return peek_locked(into);
    }

    void queue::remove_locked() {
        skip_markers_locked();
        std::vector<uint8_t> raw;
        message m;
        if (file->peek(raw) && decode(raw.data(), raw.size(), m) == decoded::ok)
            attempts.erase(m.sequence);
        drop_head_locked();
    }

    void queue::remove() {
        std::lock_guard lock(mut);
        remove_locked();
    }

    bool queue::failed(const message& m) {
        std::lock_guard lock(mut);
        const uint32_t tried = ++attempts[m.sequence];
        if (tried < max_attempts)
            return false;               // leave it where it is for the next go
        /*
         * Out of attempts. The message goes to the dead letter queue and out of
         * this one, so the consumer can get on with what is behind it - a queue
         * stuck on one bad message is the failure mode this exists to avoid.
         */
        skip_markers_locked();
        dead_letter_locked(m, tried, "attempts");
        return true;
    }

    void queue::sync() const {
        std::lock_guard lock(mut);
        file->sync();
    }

    uint32_t queue::size() const {
        std::lock_guard lock(mut);
        return file->size() - markers;          // messages, not markers - TODO 511
    }

    bool queue::empty() const {
        std::lock_guard lock(mut);
        return file->size() == markers;
    }

    uint64_t queue::file_bytes() const {
        std::lock_guard lock(mut);
        return file->file_bytes();
    }

    uint64_t queue::unsynced_bytes() const {
        std::lock_guard lock(mut);
        return file->unsynced_bytes();
    }

    uint32_t queue::dead_size() const {
        std::lock_guard lock(mut);
        return dead ? dead->size() : 0;
    }

    bool queue::peek_dead(message& into) const {
        std::lock_guard lock(mut);
        if (!dead)
            return false;
        std::vector<uint8_t> raw;
        if (!dead->peek(raw))
            return false;
        return decode(raw.data(), raw.size(), into) == decoded::ok;
    }

    void queue::remove_dead() {
        std::lock_guard lock(mut);
        if (dead)
            dead->remove();
    }
}
