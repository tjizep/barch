//
// See message_queue.h - TODO 366.
//
#include "message_queue.h"

#include "aof_record.h"     // crc32c
#include "lzr_log.h"

#include <cstring>

namespace {
    constexpr uint8_t record_version = 1;
    constexpr size_t header_size = 16;

    void put_u32(uint8_t* b, uint32_t v) {
        b[0] = (uint8_t) (v & 0xFF);
        b[1] = (uint8_t) ((v >> 8) & 0xFF);
        b[2] = (uint8_t) ((v >> 16) & 0xFF);
        b[3] = (uint8_t) ((v >> 24) & 0xFF);
    }
    void put_u64(uint8_t* b, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            b[i] = (uint8_t) ((v >> (i * 8)) & 0xFF);
    }
    uint32_t get_u32(const uint8_t* b) {
        return (uint32_t) b[0] | ((uint32_t) b[1] << 8)
               | ((uint32_t) b[2] << 16) | ((uint32_t) b[3] << 24);
    }
    uint64_t get_u64(const uint8_t* b) {
        uint64_t v = 0;
        for (int i = 7; i >= 0; --i)
            v = (v << 8) | b[i];
        return v;
    }
}

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
        put_u64(into.data() + 8, sequence);
        std::memcpy(into.data() + header_size, data.data(), data.size());
        // the checksum covers everything after itself, so it is written last
        put_u32(into.data(), aof::crc32c(into.data() + 4, into.size() - 4));
    }

    decoded decode(const uint8_t* data, size_t size, message& into) {
        if (size < header_size)
            return decoded::too_short;
        if (data[4] != record_version)
            return decoded::bad_version;
        // no length field to cross check - the payload is whatever follows the
        // header - so the checksum is the whole of the verification here, unlike
        // aof_record where the key and value lengths are checked against the size
        if (get_u32(data) != aof::crc32c(data + 4, size - 4))
            return decoded::bad_checksum;
        into.sequence = get_u64(data + 8);
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
         * next one anywhere. One less thing to keep consistent across a restart,
         * and the same trick aof::log uses.
         *
         * A record that does not verify stops the walk: the sequence continues
         * from the last one that was readable rather than from a number taken
         * out of rubbish.
         */
        uint64_t highest = 0;
        file->for_each([&](const uint8_t* d, uint32_t n) {
            message m;
            if (decode(d, n, m) != decoded::ok)
                return false;
            if (m.sequence > highest)
                highest = m.sequence;
            return true;
        });
        sequence = highest + 1;
    }

    queue::~queue() = default;

    queue_file& queue::dead_file() const {
        // not created until something is actually dead lettered, so a queue that
        // never fails leaves no second file lying around
        if (!dead)
            dead = std::make_unique<queue_file>(path + ".dead", policy);
        return *dead;
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
        while (file->peek(raw)) {
            const auto why = decode(raw.data(), raw.size(), into);
            if (why == decoded::ok) {
                const auto at = attempts.find(into.sequence);
                into.attempts = at == attempts.end() ? 0 : at->second;
                return true;
            }
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
        std::vector<uint8_t> raw;
        message m;
        if (file->peek(raw) && decode(raw.data(), raw.size(), m) == decoded::ok)
            attempts.erase(m.sequence);
        file->remove();
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
        std::vector<uint8_t> buffer;
        encode(m.sequence, m.data, buffer);
        dead_file().add(buffer);
        dead_file().sync();             // it is the only copy now
        attempts.erase(m.sequence);
        file->remove();
        barch::err({"queue", path, "gave up on message", m.sequence, "after", tried,
                    "attempts - it is in the dead letter queue"});
        return true;
    }

    void queue::sync() const {
        std::lock_guard lock(mut);
        file->sync();
    }

    uint32_t queue::size() const {
        std::lock_guard lock(mut);
        return file->size();
    }

    bool queue::empty() const {
        std::lock_guard lock(mut);
        return file->empty();
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
