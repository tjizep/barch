#include "stream_backup.h"

#include "block_stream.h"

#include <ostream>

namespace barch {

bool stream_open(const key_space_ptr& space, stream_session& s, std::string& err) {
    s = stream_session{};
    s.space = space;
    if (!space) {
        err = "no such key space";
        return false;
    }
    const auto& shards = space->get_shards();
    s.generations.assign(shards.size(), 0);
    for (size_t i = 0; i < shards.size(); ++i) {
        if (!shards[i])
            continue;
        bool in_tx = false;
        shards[i]->tx_state(in_tx, s.generations[i]);
        if (!in_tx) {
            // BEGIN and COMMIT are the caller's, so metadata can be written from the
            // same moment after the save - TODO 424
            err = "a streaming save runs inside a transaction: BEGIN first";
            return false;
        }
    }
    return true;
}

bool stream_save_shard(stream_session& s, size_t shard, std::ostream& out, std::string& err) {
    const auto& shards = s.space->get_shards();
    if (shard >= shards.size() || !shards[shard]) {
        err = "no shard " + std::to_string(shard);
        return false;
    }
    return shards[shard]->stream_save(shard, shards.size(), s.generations[shard], out, err);
}

bool stream_save_space(const key_space_ptr& space,
                       const std::function<bool(const char*, size_t, uint64_t, size_t)>& emit,
                       std::string& err) {
    stream_session s;
    if (!stream_open(space, s, err))
        return false;
    bool ok = true;
    const size_t n = space->get_shards().size();
    for (size_t i = 0; ok && i < n; ++i) {
        if (!space->get_shards()[i])
            continue;
        block_out blocks([&](const char* data, size_t len, uint64_t block) {
            return emit(data, len, block, i);
        });
        std::ostream out(&blocks);
        ok = stream_save_shard(s, i, out, err);
    }
    return ok;
}

bool stream_load_shard(const key_space_ptr& space, size_t shard, const std::string& bytes,
                       std::string& err) {
    const auto& shards = space->get_shards();
    if (shard >= shards.size() || !shards[shard]) {
        err = "no shard " + std::to_string(shard);
        return false;
    }
    return shards[shard]->stream_load(bytes.data(), bytes.size(), shard, shards.size(), err);
}

bool stream_load_space(const key_space_ptr& space,
                       const std::function<bool(uint64_t, size_t, std::string&)>& next,
                       std::string& err) {
    if (!space) {
        err = "no such key space";
        return false;
    }
    if (space->is_stateful_sharding()) {
        // LOAD holds the whole space and rebuilds the routes for these, since the
        // range sweep moves keys between shards; a shard at a time would lose some
        err = "a streaming load of a range sharded space isn't supported";
        return false;
    }
    const size_t n = space->get_shards().size();
    for (size_t i = 0; i < n; ++i) {
        bool in_tx = false;
        uint64_t gen = 0;
        if (space->get_shards()[i])
            space->get_shards()[i]->tx_state(in_tx, gen);
        if (in_tx) {
            err = "a streaming load can't run inside a transaction";
            return false;
        }
    }
    for (size_t i = 0; i < n; ++i) {
        if (!space->get_shards()[i])
            continue;
        // the whole shard first, so no script runs while its latch is held
        std::string bytes, block;
        uint64_t b = 0;
        for (;; ++b) {
            block.clear();
            if (!next(b, i, block))
                break;
            bytes += block;
        }
        if (b == 0) {
            err = "no blocks for shard " + std::to_string(i);
            return false;
        }
        if (!stream_load_shard(space, i, bytes, err))
            return false;
    }
    return true;
}

}
