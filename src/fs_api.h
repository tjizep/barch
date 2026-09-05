//
// LOADFS - a directory imported into the key space as the chunked file store.
// See fs_api.cpp and TODO 238.
//
#ifndef BARCH_FS_API_H
#define BARCH_FS_API_H

#include <string>
#include <vector>

#include "barch_apis.h"
#include "key_space.h"

namespace barch {
    /**
     * Import `dir` under `root` as the chunked file store. Empty return is success
     * and `reply` holds the summary; anything else is why it did not happen. The
     * whole import lands or none of it does.
     */
    std::string load_fs_directory(const std::string& dir, const std::string& root,
                                  size_t chunk, const barch::key_space_ptr& space,
                                  std::vector<std::string>& reply);
    /** into the default key space, at the default chunk size */
    std::string load_fs_directory(const std::string& dir, const std::string& root,
                                  std::vector<std::string>& reply);
    /**
     * Import `dir` as discrete keys and stored functions - `.luau` files become
     * functions, everything else keys named `prefix:sub:file`. Empty return is
     * success. All or nothing, the same as load_fs_directory.
     */
    std::string load_keys_directory(const std::string& dir, const std::string& prefix,
                                    const barch::key_space_ptr& space,
                                    std::vector<std::string>& reply);
    std::string load_keys_directory(const std::string& dir, const std::string& prefix,
                                    std::vector<std::string>& reply);
}

int LOADFS(caller& call, const arg_t& argv);
int LOADKEYS(caller& call, const arg_t& argv);
int cmd_LOADKEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int cmd_LOADFS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
void register_fs_api(function_map& r);

#endif //BARCH_FS_API_H
