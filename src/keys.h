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

int key_ok(const char* k, size_t klen);
int key_check(ValkeyModuleCtx* ctx, const char* k, size_t klen);
int reply_encoded_key(ValkeyModuleCtx* ctx, art::value_type key);
template<typename UT>
static art::node_ptr leaf_numeric_update(UT& l,const art::node_ptr & old,UT by)
{
	const art::leaf * leaf = old.const_leaf();
	if (conversion::convert_value(l,leaf->get_value())){

		l += by;
		auto s = std::to_string(l);
		return make_leaf
		(   leaf->get_key()
		,   conversion::to_value(s)
		,   leaf->ttl()
		,   leaf->is_volatile()
		);
	}
	return nullptr;
}

#endif //KEYS_H
