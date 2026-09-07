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
/**
 * Fill one key from the foreign source and wait for it, with no caller involved.
 *
 * `point_get` parks the connection that asked, which is why a script could not use
 * it: inside a function there is no connection to park. This waits on the flight
 * the same way the SWIG path does - on the calling thread, bounded by the space's
 * waiter timeout - so a script can ask for a fill and mean it. See TODO 259.
 *
 * True means the key is now whatever the source says it is, including absent. False
 * fills `err`.
 */
bool fetch_now(const key_space_ptr& space, art::value_type key, std::string& err);
int mget(caller& call, const arg_t& argv);
int exists_many(caller& call, const arg_t& argv);
int FAKE(caller& call, const arg_t& argv);
int MISS(caller& call, const arg_t& argv);

}
}

#endif
