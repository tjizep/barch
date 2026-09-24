#pragma once
//
// Streaming save and load of a whole key space - TODO 418. What barch.store.save /
// barch.store.load run in Luau and StreamSave / StreamLoad run through SWIG.
//
#include <cstdint>
#include <functional>
#include <string>

#include "key_space.h"

namespace barch {

/**
 * Every shard of `space`, in shard order, as blocks: `emit(data, len, block, shard)`,
 * block numbers starting at 0 in every shard. The state saved is the space as it
 * stood at BEGIN, so the caller opens the transaction and ends it: BEGIN, save,
 * whatever else has to come from that same moment, COMMIT - TODO 424. Refused with no
 * transaction open, and the transaction is left open. `emit` answering false stops
 * the save, which is then an error.
 */
bool stream_save_space(const key_space_ptr& space,
                       const std::function<bool(const char* data, size_t len, uint64_t block,
                                                size_t shard)>& emit,
                       std::string& err);

/**
 * Put every shard of `space` back from blocks: `next(block, shard, out)` fills `out`
 * with that block and answers true, or answers false when the shard has no more.
 * A shard's blocks are collected before it is replaced, and it is refused untouched
 * when they aren't a whole stream for that shard of a space with this many shards.
 * Refused inside a transaction. Shards before a refused one stay loaded, and `err`
 * says which shard it was.
 */
bool stream_load_space(const key_space_ptr& space,
                       const std::function<bool(uint64_t block, size_t shard, std::string& out)>& next,
                       std::string& err);

/**
 * The pieces, for a caller that drives a save a shard at a time (the SWIG cursor).
 * `stream_open` refuses a space that isn't in a transaction and records every shard's
 * generation, and `stream_save_shard` saves one shard against that record, so a
 * COMMIT part way through ends the save rather than mixing two states. Neither
 * begins or commits anything.
 */
struct stream_session {
    key_space_ptr space{};
    heap::vector<uint64_t> generations{};
};
bool stream_open(const key_space_ptr& space, stream_session& s, std::string& err);
bool stream_save_shard(stream_session& s, size_t shard, std::ostream& out, std::string& err);
/** load one shard's collected stream; refuses inside a transaction */
bool stream_load_shard(const key_space_ptr& space, size_t shard, const std::string& bytes,
                       std::string& err);

}
