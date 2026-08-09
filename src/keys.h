//
// Created by teejip on 4/9/25.
//

#ifndef KEYS_H
#define KEYS_H
#include <cstdlib>
#include "../external/include/valkeymodule.h"
#include "art/art.h"
#include "conversion.h"
#include "value_type.h"


int key_ok(const char *k, size_t klen);

int key_ok(art::value_type v);

int key_check(ValkeyModuleCtx *ctx, const char *k, size_t klen);

int reply_encoded_key(ValkeyModuleCtx *ctx, art::value_type key);
int reply_variable(ValkeyModuleCtx *ctx, const Variable var) ;

Variable param_as_variant(art::value_type param);
Variable encoded_key_as_variant(art::value_type key);

std::string encoded_key_as_string(art::value_type key);

unsigned log_encoded_key(art::value_type key, bool start = true);

/**
 * Why a numeric update did not produce a new leaf.
 *
 * It used to answer with a null node for all three reasons at once, and shard::update
 * reports a null the same way it reports a key that was not there. INCR then took the
 * miss branch and inserted, so `SET s abc` followed by `INCR s` replaced "abc" with 1 -
 * a type error that silently destroyed the value instead of being refused. The caller
 * needs to tell the three apart to answer the way redis does.
 */
enum class numeric_status {
    updated,        // a new leaf was produced
    not_numeric,    // the value that was there is not a number
    overflowed,     // the arithmetic would wrap
    compressed      // the leaf is compressed and was not decompressed to try
};

template<typename UT>
static art::node_ptr leaf_numeric_update(UT &l, const art::node_ptr &old, UT by, numeric_status& why) {
    why = numeric_status::updated;
    const art::leaf *leaf = old.const_leaf();
    if (leaf->is_compressed()) {
        why = numeric_status::compressed;
        return nullptr;
    }
    if (conversion::convert_value(l, leaf->get_value())) {

        auto& alloc = const_cast<alloc_pair&>(old.logical.get_ap<alloc_pair>());

        auto old = l;
        l += by;
        if ((long long)by > 0ll && l < old) {
            why = numeric_status::overflowed;
            return nullptr;
        }
        if ((long long)by < 0ll && l > old) {
            why = numeric_status::overflowed;
            return nullptr;
        }
        auto s = std::to_string(l);
        return make_leaf
        (  alloc
        ,  leaf->get_key()
        ,  conversion::to_value(s)
        ,  leaf->expiry_ms()
        ,  leaf->is_volatile()
        , leaf->is_compressed()
        );
    }
    why = numeric_status::not_numeric;
    return nullptr;
}

#endif //KEYS_H
