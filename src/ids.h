#pragma once
//
// Sequences - a counter in a key space, handed out in blocks. See TODO 253.
//
// A file's chunks are keyed by an id rather than by its path, so creating one needs
// a number nobody else has. The counter is a key like any other, which means it
// lives on whichever shard its name hashes to - almost never the shard the file
// itself lands on. Taking that second shard's latch once per created file would
// make every concurrent create in the space queue behind one counter.
//
// So ids are reserved in blocks and handed out from memory. Two things follow, and
// callers have to know both:
//
//   * IDS ARE UNIQUE, NOT DENSE. The stored counter is moved past the whole block
//     before any id in it is handed out, so a crash or a restart abandons whatever
//     was left. Gaps are normal and nothing may assume otherwise - not "the last id
//     is the count of files", not "ids are contiguous within a directory".
//
//   * RESERVE BEFORE TAKING THE LATCH YOU ARE WRITING UNDER, never inside it.
//     Reserving touches another shard, and doing that while holding your own is how
//     a lock order inversion gets built. A batch knows how many ids it needs, so it
//     asks once, up front, before it starts writing. That is the whole reason the
//     count is a parameter.
//
#include <cstdint>
#include <string>

#include "key_space.h"

namespace barch {

/**
 * `count` consecutive ids from the named sequence in `space`, the first in `first`.
 * Ids start at 1, so 0 is never valid and can mean "none".
 *
 * The block handed back is yours whether you use it or not - there is no way to
 * give one back, and no reason to want one.
 */
bool reserve_ids(const key_space_ptr& space, const std::string& name,
                 uint64_t count, uint64_t& first, std::string& err);

/**
 * Forget what is cached for a space, because its data no longer says what the
 * cache assumes. Anything that clears a space has to call this: the counter is a
 * key, so clearing the space resets it to nothing, and a cached block from before
 * would collide with the ids handed out after. See CLEAR and CLEARALL.
 */
void forget_sequences(const std::string& space_name);
void forget_all_sequences();

}
