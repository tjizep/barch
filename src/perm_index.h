#pragma once
//
// Secondary indexes over the fields of composite keys - TODO 422, phase 1.
//
// A record is a composite key: some fields, then a pinned tail (the doc id of an
// n-gram) that always stays last. An index holds the record again under orderings of
// its fields, so any set of fields can be looked up as a prefix. For equality on up to
// 8 fields, C(n, n/2) orderings are enough: the chains of a symmetric chain
// decomposition of the subsets (Greene-Kleitman). Every subset of the fields is the
// first |subset| fields of exactly one chain.
//
// Paths live in `<space>_ix` as ordinary composite keys:
//
//     [name] [chain] [the fields in the chain's order] [the pinned tail]
//
// made of the source key's own encoded components, so nothing is parsed back to text
// and the source key can be rebuilt from the path without reading the source.
//
// Phase 1 builds a chain on request, over the data as it is. Phase 2 follows the
// writes after it: shards report every write and erase to a queue per space (see
// index_sink.h), which the space's maintenance thread applies, and FIND applies
// before it reads. FIND also checks each answer against the source, so a path the
// queue never heard about (an expiry, an eviction, a build racing a delete) is
// dropped rather than answered.
//
#include <cstdint>
#include <string>
#include <vector>

#include "key_space.h"

namespace barch::pindex {

constexpr size_t max_fields = 8;

/** the chains for n fields: each an ordering of 0..n-1, plus which chain has a subset */
struct chain_set {
    size_t n{0};
    std::vector<std::vector<uint8_t>> chains{};
    /** subset bitmask -> the chain whose first |subset| fields are that subset */
    std::vector<uint8_t> chain_of{};
};
const chain_set& chains_for(size_t n);

/** a stored composite key's components, each slice with its terminator; false if not one */
bool split(art::value_type key, std::vector<std::string>& parts);
/** components back into a composite key, terminators put right */
std::string join(const std::vector<std::string>& parts);
/** one field's text as a component, the way the space's key encoding makes it */
std::string component_of(const std::string& text);

struct definition {
    std::string name{};
    size_t fields{0};
    size_t pinned{0};
    std::vector<size_t> ready{};    // chains built
    std::vector<size_t> building{}; // chains a BUILD is walking: writes are followed already
    bool is_ready(size_t chain) const;
};

/** the space a source's paths are kept in: `<canonical>_ix` */
std::string index_space_name(const key_space_ptr& source);

bool create(const key_space_ptr& source, const definition& def, std::string& err);
bool load(const key_space_ptr& source, const std::string& name, definition& out, std::string& err);
std::vector<definition> list(const key_space_ptr& source);
bool drop(const key_space_ptr& source, const std::string& name, std::string& err);

struct build_result {
    uint64_t records{0};
    uint64_t skipped{0};    // keys that aren't a composite of fields + pinned components
};
/** write every record's path for `chain`, then mark the chain ready */
bool build(const key_space_ptr& source, definition& def, size_t chain, build_result& out,
           std::string& err);

/**
 * The source keys whose fields equal `equal` (field number, value), from the chain that
 * has exactly those fields in front, as encoded source keys. Refused when that chain
 * isn't built, and the error names it.
 */
bool find(const key_space_ptr& source, const definition& def,
          const std::vector<std::pair<size_t, std::string>>& equal, int64_t limit,
          std::vector<std::string>& keys, std::string& err);

// ---- phase 2: following writes -------------------------------------------------------

/**
 * A space has opened: when it has indexes, point its shards at its queue. Only when
 * the configuration space is already open - a space must not build that from its own
 * constructor - and otherwise the first maintenance tick does it.
 */
void on_open(const std::string& canonical, const heap::vector<shard_ptr>& shards);
/** the maintenance tick: attach once if on_open couldn't, then apply the queue */
void tick(const std::string& canonical, const heap::vector<shard_ptr>& shards, bool& checked);
/** apply every write queued for this space to its indexes, in order */
void drain(const std::string& canonical);
/** writes queued and not yet applied */
size_t pending(const std::string& canonical);

}
