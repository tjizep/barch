#ifndef BARCH_FOREIGN_H
#define BARCH_FOREIGN_H

#include "caller.h"
#include "key_space.h"
#include "value_type.h"

namespace barch {
namespace foreign {

int point_get(caller& call, art::value_type key);
int point_exists(caller& call, art::value_type key);
void kick(const key_space_ptr& space, const std::string& key);
int mget(caller& call, const arg_t& argv);
int exists_many(caller& call, const arg_t& argv);
int FAKE(caller& call, const arg_t& argv);
int MISS(caller& call, const arg_t& argv);

}
}

#endif
