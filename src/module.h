//
// Created by teejip on 4/9/25.
//

#ifndef MODULE_H
#define MODULE_H

#include "art.h"
#define NAME(x) "B." #x , cmd_##x
extern art::tree* ad;
struct constants
{
	ValkeyModuleString * OK = nullptr;
	ValkeyModuleString * FIELDS = nullptr;
	void init(ValkeyModuleCtx* ctx)
	{

		OK = ValkeyModule_CreateString(ctx, "OK", 2);
		FIELDS = ValkeyModule_CreateString(ctx, "FIELDS", 6);
	}
};
extern constants Constants;
std::shared_mutex& get_lock();
art::tree* get_art();

#endif //MODULE_H
