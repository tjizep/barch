#pragma once
//
// Meta keys - state that belongs to a space's data rather than to a caller. TODO 527.
//
// Some of what a space needs isn't data anyone asked to store, but it has to agree
// with the data: the fs and graph id counters, and later the layout markers and the
// dictionary. Kept as ordinary keys it went wherever the data went - saved, logged,
// replicated and copied - but a client could also DEL or SET it, and eviction could
// take it (TODO 528). Kept in a file beside the shard files it had to be kept in step
// by hand, which is most of TODO 518 and 521.
//
// A meta key is an ordinary key under its own lead byte, art::tmeta. So it still goes
// everywhere the data goes, and:
//   - nothing a client sends can name it: every command's key goes through
//     encode_key, which never makes that lead;
//   - no walk a client sees shows it - KEYS, SCAN, RANGE, MIN, MAX, RANDOMKEY, the
//     Luau walks and EXPORT's key walk skip it (EXPORT writes what it needs of it on
//     purpose);
//   - eviction and cold key compression leave it alone.
// DBSIZE still counts it, the way it counted the same state as plain keys before.
//
// The key is one component: a scope byte, then the name. The only scope so far is `data`:
// state that travels with the space. A RETRIEVE ships shard files whole, so anything
// belonging to one node rather than to the data would need a way to be left out
// first; the byte leaves room for that without another format change.
//
#include <string>

#include "composite.h"
#include "key_space.h"

namespace barch::meta {
    /** state that travels with the space's data - the only scope so far */
    constexpr char scope_data = 'd';

    /** the key `name` is kept under */
    art::value_type key(composite& q, const std::string& name);
    /** is this stored key a meta key */
    inline bool is_meta(art::value_type key) {
        return key.size > 0 && art::is_meta_lead(key.bytes[0]);
    }
    /** the name a stored meta key of the `data` scope is kept under; false for any other key */
    bool name_of(art::value_type key, std::string& name);

    /** the value of `name` in `space`; false when there's none */
    bool get(const key_space_ptr& space, const std::string& name, std::string& out);
    /**
     * Set `name` in `space`, through the same path as any write - logged and
     * replicated. False, with `err`, when the write was refused.
     */
    bool set(const key_space_ptr& space, const std::string& name, const std::string& value,
             std::string& err);
    /** remove `name` from `space`; true when there was one */
    bool remove(const key_space_ptr& space, const std::string& name);
}
