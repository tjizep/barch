//
// Created by teejip on 4/9/25.
//

#ifndef KEYS_H
#define KEYS_H
#include <cstdlib>
#include "valkeymodule.h"
#include "art.h"
#include "conversion.h"
#include "value_type.h"

namespace art {
    unsigned key_type_size(value_type key);
}

int key_ok(const char *k, size_t klen);

int key_ok(art::value_type v);

int key_check(ValkeyModuleCtx *ctx, const char *k, size_t klen);

int reply_encoded_key(ValkeyModuleCtx *ctx, art::value_type key);
int reply_variable(ValkeyModuleCtx *ctx, const Variable var) ;

Variable param_as_variant(art::value_type param);
Variable encoded_key_as_variant(art::value_type key);

std::string encoded_key_as_string(art::value_type key);

unsigned log_encoded_key(art::value_type key, bool start = true);

template<typename UT>
static art::node_ptr leaf_numeric_update(UT &l, const art::node_ptr &old, UT by) {
    const art::leaf *leaf = old.const_leaf();
    if (conversion::convert_value(l, leaf->get_value())) {
        l += by;
        auto& alloc = const_cast<alloc_pair&>(old.logical.get_ap<alloc_pair>());
        auto s = std::to_string(l);
        return make_leaf
        (  alloc
        ,  leaf->get_key()
        ,  conversion::to_value(s)
        ,  leaf->expiry_ms()
        ,  leaf->is_volatile()
        );
    }
    return nullptr;
}

#endif //KEYS_H
