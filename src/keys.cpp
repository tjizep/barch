//
// Created by teejip on 4/9/25.
//

#include "keys.h"
#include <cstdlib>
#include "conversion.h"

int key_ok(const char* k, size_t klen)
{
	if (k == nullptr)
		return -1;

	if (klen == 0)
		return -1;

	return 0;
}

int key_check(ValkeyModuleCtx* ctx, const char* k, size_t klen)
{
	if (k == nullptr)
		return ValkeyModule_ReplyWithError(ctx, "No null keys");

	if (klen == 0)
		return ValkeyModule_ReplyWithError(ctx, "No empty keys");


	return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
}

int reply_encoded_key(ValkeyModuleCtx* ctx, art::value_type key)
{
	double dk;
	int64_t ik;
	const char* k;
	size_t kl;
	const unsigned char* enck = key.bytes;
	unsigned key_len = key.size;
	if (key_len == numeric_key_size && (*enck == art::tinteger || *enck == art::tdouble))
	{
		ik = conversion::enc_bytes_to_int(enck, key_len);
		if (*enck == art::tdouble)
		{
			memcpy(&dk, &ik, sizeof(ik));
			if (ValkeyModule_ReplyWithDouble(ctx, dk) == VALKEYMODULE_ERR)
			{
				return -1;
			}
		}
		else
		{
			if (ValkeyModule_ReplyWithLongLong(ctx, ik) == VALKEYMODULE_ERR)
			{
				return -1;
			}
		}
	}
	else if (key_len >= 1 && *enck == art::tstring)
	{
		k = (const char*)&enck[1];
		kl = key_len - 2;
		if (ValkeyModule_ReplyWithStringBuffer(ctx, k, kl) == VALKEYMODULE_ERR)
		{
			return -1;
		}
	}
	else
	{
		abort();
	}
	return 0;
}
