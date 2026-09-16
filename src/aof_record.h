//
// Created by barch on 16-09-2026.
//
#ifndef BARCH_AOF_RECORD_H
#define BARCH_AOF_RECORD_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace barch::aof {
    /**
     * One record in a key space's change log - TODO 353.
     *
     * The framing exists for a specific reason rather than for neatness. Below
     * `aof_durability = each` the queue file's header can reach the device
     * before the element it points at, so a crash can leave an element whose
     * bytes never arrived - and the first four bytes of an element are its
     * length, so a reader that trusts them can take a wild length and walk off
     * the end of the ring. A record that cannot be distinguished from rubbish
     * would make every durability level below `each` a promise barch cannot
     * keep. Hence a checksum, and a decode that refuses whatever it cannot
     * verify.
     *
     *     0   4  crc32c over every byte after this field
     *     4   1  version, 1
     *     5   1  type
     *     6   2  key space name length
     *     8   4  key length
     *     12  4  value length
     *     16  8  sequence
     *     24  8  expiry in milliseconds, 0 for none
     *     32  1  the art::key_options flags the write was made with
     *     33  3  reserved, written as zero
     *     36  4  the shard the write went to
     *     40  4  how many shards the space had at the time
     *     44  4  reserved, written as zero
     *     48  .. the name, the key and the value, in that order
     *
     * The options byte is not decoration. `flag_is_compressed` says the value
     * bytes are a compressed form, so a replay that dropped it would store the
     * compressed bytes as a value and hand them back to clients as though they
     * were the real thing. `flag_is_hashed` decides which index the key lives
     * in, `flag_is_volatile` whether eviction may take it, and
     * `flag_is_keep_ttl` and `flag_has_expiry` what happens to its expiry. A
     * record without them describes a write that cannot be repeated.
     *
     * The shard and the count are there because a key does not say where it
     * belongs. A container's entries live on the shard its *name* routes to -
     * `with_container_write` routes by the container and then writes
     * `container|field` keys into that shard - so routing a composite key by
     * itself at replay lands it somewhere else, and the read, which routes by
     * the container, never finds it. Recording the placement is the only way to
     * reproduce it, since nothing can recover a container name from a composite
     * key.
     *
     * The count travels with it so a replay knows whether the number still
     * means anything. Same count, same placement; a different count and the
     * shard numbers are meaningless, which a replay has to say rather than
     * quietly scatter a hash across the space.
     *
     * Bytes are reserved and written as zero because this is the third field
     * this format has needed in an afternoon, and there will be a fourth.
     *
     * Little endian, written byte by byte rather than memcpy'd from a struct, so
     * a log moves between machines and does not depend on padding. This is
     * barch's framing *inside* an element; `queue_file` itself stays big endian
     * and byte compatible with Tape, which is a separate question.
     */
    enum class record_type : uint8_t {
        set = 1,        // the key holds this value now
        erase = 2,      // the key is gone; value is empty
        checkpoint = 3  // everything before this is in the shard file
    };

    /** the fixed part of a record */
    static constexpr uint32_t header_length = 48;
    static constexpr uint8_t current_version = 1;

    struct record {
        record_type type{record_type::set};
        uint64_t sequence{0};
        int64_t expiry_ms{0};
        /** the art::key_options flags bits, as the write was made */
        uint8_t options{0};
        /** where it went, and what the count was, so a replay can put it back */
        uint32_t shard{0};
        uint32_t shard_count{0};
        std::string space;
        std::string key;
        std::string value;
    };

    /** why a decode refused, because "corrupt" alone is not actionable */
    enum class decoded {
        ok,
        too_short,      // fewer bytes than a header
        bad_version,    // written by something newer than this
        bad_framing,    // the three lengths do not add up to the element
        bad_checksum    // the bytes are not what was written
    };

    /** append the encoded record to `into`, which is cleared first */
    void encode(const record& r, std::vector<uint8_t>& into);

    /** read one back. `into` is only meaningful when this returns ok */
    decoded decode(const uint8_t* data, uint32_t size, record& into);

    /** the reason, for a log line */
    std::string_view describe(decoded why);

    /**
     * crc32c (Castagnoli), table driven.
     *
     * Not borrowed: zstd ships an xxhash but only as a private header under
     * `lib/common`, and barch builds for ARM as well as x86, so neither a
     * dependency's internals nor the SSE4.2 instruction is a safe thing to
     * depend on. Castagnoli is the usual choice for framing and catches the
     * short bursts a torn write produces.
     */
    uint32_t crc32c(const uint8_t* data, size_t size, uint32_t seed = 0);
}

#endif //BARCH_AOF_RECORD_H
