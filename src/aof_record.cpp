//
// Created by barch on 16-09-2026.
//
#include "aof_record.h"

#include <array>
#include <cstring>

namespace {
    /** the reflected Castagnoli polynomial */
    constexpr uint32_t poly = 0x82F63B78u;

    std::array<uint32_t, 256> build_table() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k)
                c = (c & 1) ? (poly ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }

    const std::array<uint32_t, 256>& table() {
        static const std::array<uint32_t, 256> t = build_table();
        return t;
    }

    // little endian, a byte at a time: no padding to think about and the same
    // bytes on every machine barch builds for
    void put_u16(uint8_t* b, uint16_t v) {
        b[0] = (uint8_t) v; b[1] = (uint8_t) (v >> 8);
    }
    void put_u32(uint8_t* b, uint32_t v) {
        for (int i = 0; i < 4; ++i) b[i] = (uint8_t) (v >> (8 * i));
    }
    void put_u64(uint8_t* b, uint64_t v) {
        for (int i = 0; i < 8; ++i) b[i] = (uint8_t) (v >> (8 * i));
    }
    uint16_t get_u16(const uint8_t* b) {
        return (uint16_t) ((uint16_t) b[0] | ((uint16_t) b[1] << 8));
    }
    uint32_t get_u32(const uint8_t* b) {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= (uint32_t) b[i] << (8 * i);
        return v;
    }
    uint64_t get_u64(const uint8_t* b) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= (uint64_t) b[i] << (8 * i);
        return v;
    }
}

namespace barch::aof {

    uint32_t crc32c(const uint8_t* data, size_t size, uint32_t seed) {
        const auto& t = table();
        uint32_t c = ~seed;
        for (size_t i = 0; i < size; ++i)
            c = t[(c ^ data[i]) & 0xFF] ^ (c >> 8);
        return ~c;
    }

    void encode(const record& r, std::vector<uint8_t>& into) {
        const auto space_len = (uint16_t) r.space.size();
        const auto key_len = (uint32_t) r.key.size();
        const auto value_len = (uint32_t) r.value.size();

        into.assign(header_length + space_len + key_len + value_len, 0);
        uint8_t* b = into.data();

        b[4] = current_version;
        b[5] = (uint8_t) r.type;
        put_u16(b + 6, space_len);
        put_u32(b + 8, key_len);
        put_u32(b + 12, value_len);
        put_u64(b + 16, r.sequence);
        put_u64(b + 24, (uint64_t) r.expiry_ms);
        b[32] = r.options;
        put_u32(b + 36, r.shard);
        put_u32(b + 40, r.shard_count);
        // 33..35 and 44..47 stay zero: reserved, and the checksum covers them so
        // a future reader can tell a zero it wrote from one it did not

        uint8_t* at = b + header_length;
        std::memcpy(at, r.space.data(), space_len);            at += space_len;
        std::memcpy(at, r.key.data(), key_len);                at += key_len;
        std::memcpy(at, r.value.data(), value_len);

        // the checksum last, over everything that follows it
        put_u32(b, crc32c(b + 4, into.size() - 4));
    }

    decoded decode(const uint8_t* data, uint32_t size, record& into) {
        if (data == nullptr || size < header_length)
            return decoded::too_short;
        if (data[4] != current_version)
            return decoded::bad_version;

        const uint16_t space_len = get_u16(data + 6);
        const uint32_t key_len = get_u32(data + 8);
        const uint32_t value_len = get_u32(data + 12);

        /*
         * The framing is checked before the checksum and in 64 bit arithmetic,
         * so three lengths that would overflow a 32 bit sum cannot be made to
         * look as though they fit. A record whose lengths disagree with the
         * element it came out of is refused whatever its checksum says.
         */
        const uint64_t expect = (uint64_t) header_length + space_len + key_len + value_len;
        if (expect != (uint64_t) size)
            return decoded::bad_framing;

        if (get_u32(data) != crc32c(data + 4, size - 4))
            return decoded::bad_checksum;

        /*
         * Past the checksum, so this record is exactly what was written: a type
         * this build doesn't know came from a newer one, not from a torn write.
         * It gets its own answer because the two need opposite handling - a torn
         * tail is cut off, and cutting this would destroy good records - TODO 478.
         */
        const auto type = (record_type) data[5];
        if (type != record_type::set && type != record_type::erase
            && type != record_type::checkpoint && type != record_type::clear)
            return decoded::unknown_type;

        into.type = type;
        into.sequence = get_u64(data + 16);
        into.expiry_ms = (int64_t) get_u64(data + 24);
        into.options = data[32];
        into.shard = get_u32(data + 36);
        into.shard_count = get_u32(data + 40);
        const uint8_t* at = data + header_length;
        into.space.assign((const char*) at, space_len);        at += space_len;
        into.key.assign((const char*) at, key_len);            at += key_len;
        into.value.assign((const char*) at, value_len);
        return decoded::ok;
    }

    std::string_view describe(decoded why) {
        switch (why) {
            case decoded::ok:           return "ok";
            case decoded::too_short:    return "shorter than a record header";
            case decoded::bad_version:  return "written by a newer version";
            case decoded::bad_framing:  return "the lengths do not match the record";
            case decoded::bad_checksum: return "checksum mismatch";
            case decoded::unknown_type: return "a record type this build doesn't know, from a newer build";
        }
        return "unknown";
    }
}
