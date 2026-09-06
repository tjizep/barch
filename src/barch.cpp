//
// Created by me on 11/9/24.
//

#include <ranges>

#include "auth_api.h"
#include "barch_apis.h"
#include "art/iterator.h"
#include "rpc/redis_parser.h"
#include "vk_caller.h"
#include "keys.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "rpc/restarter.h"
/* cdict --
 *
 * This module implements a volatile key-value store on top of the
 * dictionary exported by the modules API.
 *
 * -----------------------------------------------------------------------------
 * */
extern "C" {
#include "../external/include/valkeymodule.h"
}

#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>
#include "conversion.h"
#include "version.h"
#include "glob.h"
#include "art/art.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "module.h"
#include "hash_api.h"
#include "connection_api.h"
#include "keyspace_api.h"
#include "repl_api.h"
#include "config_api.h"
#include "info_api.h"
#include "keys_api.h"
#include "sharded_store.h"
#include "ordered_api.h"
#include "caller.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"
#include "function_sync.h"


extern "C" {
























































































/* This function must be present on each module. It is used in order to
 * register the commands into the server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) {
    if (ValkeyModule_Init(ctx, "B", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    // every command is registered by its own category, which is also where it is
    // declared and where its RESP registration lives
    for (auto add : { add_keys_api, add_hash_api, add_ordered_api, add_connection_api,
                      add_keyspace_api, add_repl_api, add_config_api, add_info_api }) {
        if (add(ctx) != VALKEYMODULE_OK) {
            return VALKEYMODULE_ERR;
        }
    }
    Constants.init(ctx);
    // valkey should free this I hope
    if (barch::register_valkey_configuration(ctx) != 0) {
        return VALKEYMODULE_ERR;
    };
    if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    // after LoadConfigs, never before: it applies a registered default to every setting
    // the config file does not mention, so anything taken from the environment earlier
    // would be undone here
    barch::apply_environment_configuration();
    auto ks = get_default_ks();
    if (ks == nullptr) {
        return VALKEYMODULE_ERR;
    }
    /*
     * Repositories live in the configuration space now, so this has to come after
     * the default space is up. A repository that said `asynch off` is applied here
     * and a failure refuses the load, which is what asking for it synchronously
     * means; everything else is the sync thread's problem. See TODO 252.
     */
    if (barch::any_repo_configured()) {
        auto sync_err = barch::sync_startup_repos();
        if (!sync_err.empty()) {
            barch::err({"git repository", sync_err});
            return VALKEYMODULE_ERR;
        }
        barch::start_function_sync();
    }
    if (!barch::get_server_binding().empty())
        barch::server::start(barch::get_server_binding(),barch::get_server_port(), false);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void *unused_arg) {
    // TODO: destroy tree
    return VALKEYMODULE_OK;
}
}
