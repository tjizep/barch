#include "stream_backup.h"

#include "block_stream.h"
#include "keyspace_locks.h"

#include <ostream>

namespace barch {

bool stream_begin(const key_space_ptr& space, stream_session& s, std::string& err) {
    s = stream_session{};
    s.space = space;
    if (!space) {
        err = "no such key space";
        return false;
    }
    // every shard's write latch at once, the way BEGIN takes them, so a
    // transaction opened here is one moment for the whole space
    ks_unique held(space);
    const auto& shards = space->get_shards();
    size_t open = 0, present = 0;
    for (const auto& t : shards) {
        if (!t)
            continue;
        ++present;
        bool in_tx = false;
        uint64_t gen = 0;
        t->tx_state_holding_lock(in_tx, gen);
        if (in_tx)
            ++open;
    }
    if (open != 0 && open != present) {
        err = "some shards of this space are in a transaction and some aren't";
        return false;
    }
    if (open == 0) {
        for (const auto& t : shards)
            if (t)
                t->begin_holding_lock();
        s.began = true;
    }
    s.generations.assign(shards.size(), 0);
    for (size_t i = 0; i < shards.size(); ++i) {
        bool in_tx = false;
        if (shards[i])
            shards[i]->tx_state_holding_lock(in_tx, s.generations[i]);
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

void stream_end(stream_session& s) {
    if (!s.began || !s.space)
        return;
    const auto& shards = s.space->get_shards();
    for (size_t i = 0; i < shards.size() && i < s.generations.size(); ++i) {
        if (!shards[i])
            continue;
        bool in_tx = false;
        uint64_t gen = 0;
        shards[i]->tx_state(in_tx, gen);
        // only the transaction this save opened; one somebody else began since
        // is theirs to end
        if (in_tx && gen == s.generations[i])
            shards[i]->commit();
    }
    s.began = false;
}

bool stream_save_space(const key_space_ptr& space,
                       const std::function<bool(const char*, size_t, uint64_t, size_t)>& emit,
                       std::string& err) {
    stream_session s;
    if (!stream_begin(space, s, err))
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
    stream_end(s);
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
