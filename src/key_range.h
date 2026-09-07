#pragma once
//
// A range over keys as text, whichever way they were encoded. See TODO 260.
//
// A caller's key is encoded one of two ways. Without the space's split character in
// it - a space, unless `key_split` says otherwise - it becomes a plain string. With
// one, it becomes a multi part composite, which is the feature `key_split` is for:
// `RANGE "alpha a" "alpha z"` then walks the parts under `alpha`.
//
// The two live in different regions of the tree and each is correctly ordered
// inside itself, so a scan of one never sees the other. That is fine when the
// caller meant one of them and a trap when it did not: `SET "k with space" v`
// reads back with GET and never appears in `RANGE k l`, because the key is a
// composite and the bounds are plain strings. 992 files stored under directories
// like `Home & Kitchen` listed as three.
//
// So a text range asks both regions and merges them in text order. Nothing about
// how a key is stored changes - this is only about a scan being able to reach it.
//
#include <functional>
#include <string>

#include "key_space.h"
#include "value_type.h"

namespace barch {

/**
 * Every key whose text is in [lo, hi), in text order, up to `limit` of them
 * (0 or negative for no limit). The callback gets the encoded key, valid only for
 * the duration of the call, the same as `sharded_store::range`.
 */
/**
 * `keep` decides what the caller is allowed to see - stored functions are hidden
 * from a caller without the `function` category, and hiding them cannot be done by
 * clamping the upper bound any more: there are two regions to bound now, and a
 * function key is in a third. Empty means everything.
 */
using key_filter = std::function<bool(art::value_type)>;

void text_range(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                int64_t limit, const std::function<void(art::value_type)>& cb,
                const key_filter& keep = {});

/**
 * How many keys are in [lo, hi), counting both regions. Without a filter this is
 * two counts and no walk; with one it has to look at each key.
 */
int64_t text_count(const key_space_ptr& space, art::value_type lo, art::value_type hi,
                   const key_filter& keep = {});

}
