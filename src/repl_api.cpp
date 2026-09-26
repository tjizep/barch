//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "repl_api.h"
#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>

#include "barch_apis.h"
#include "caller.h"
#include "vk_caller.h"
#include "module.h"
#include "conversion.h"
#include "version.h"
#include "glob.h"
#include "function_sync.h"
#include "http_api.h"
#include "keys.h"
#include "art/art.h"
#include "art/iterator.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "sharded_store.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"
#include "statistics.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "auth_api.h"
#include "rpc/server.h"
#include "rpc/restarter.h"
#include "rpc/redis_parser.h"

extern "C" {
#include "../external/include/valkeymodule.h"
}

extern "C" {

static restarter restart;

/* B.LOAD
 * loads and overwrites the data from files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int LOAD(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    std::atomic<size_t> errors = 0;
    auto ks = call.kspace();
    barch::sharded_store store(ks);
    const auto& change_log = ks->get_change_log();
    uint64_t loaded_at = 0;
    {
        /*
         * Every shard locked, whatever the sharding, and each cleared before it
         * loads - TODO 479. After a LOAD the files are the state, so the change
         * log gets a checkpoint at this moment and a replay starts from the files
         * again. That's only true with nothing half written when the mark is
         * taken, and with nothing kept that the files don't have. A range sweep
         * is held off too, and the range table is rebuilt before the lock drops,
         * because it is nothing but each shard's first key.
         */
        barch::sharded_store::write_guard held = store.lock_space_write();
        loaded_at = change_log ? change_log->mark() : 0;
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->load_holding_lock()) ++errors;
        });
        if (ks->is_range_sharded()) {
            ks->routes().rebuild(store.shards());
        }
    }
    /*
     * Not after a shard failed: what it holds now isn't its file, so the
     * checkpoint would be a claim the files can't back up.
     */
    if (errors == 0 && change_log) {
        try {
            change_log->checkpoint(ks->space_name(), loaded_at);
            change_log->trim_to_last_checkpoint();
        } catch (const std::exception& e) {
            barch::err({"loaded, but could not checkpoint the change log:", e.what()});
        }
    }
    return errors>0 ? call.push_error("some shards did not load") : call.push_simple("OK");
}
int cmd_LOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LOAD);
}
int RELOAD(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    std::atomic<size_t> errors = 0;
    barch::sharded_store store(call.kspace());
    // freeze only when the partition is state. a range sweep needs two write
    // locks to move a key, so it waits, and a key cannot leave a shard that
    // has already been replaced from disk for one that has not. hash sharding
    // reloads under each shard's own latch. the range table is rebuilt before
    // the space lock drops
    barch::sharded_store::write_guard held;
    if (call.kspace()->is_stateful_sharding()) {
        held = store.lock_space_write();
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->reload_holding_lock()) ++errors;
        });
        if (call.kspace()->is_range_sharded()) {
            // the routing table describes the shards, and the shards were just
            // replaced by what was on disk. rebuilding is what a load does
            // anyway - the table is never written down, only derived
            call.kspace()->routes().rebuild(store.shards());
        }
    } else {
        store.each_shard_parallel([&errors](const barch::shard_ptr& shard) {
            if (!shard->reload()) ++errors;
        });
    }
    return errors>0 ? call.push_error("some shards did not reload") : call.push_simple("OK");
}
int START(caller& call, const arg_t& argv) {
    if (argv.size() > 5 || argv.size() < 3)
        return call.wrong_arity();
    if (argv.size() >= 4 && argv[3] != "SSL") {
        return call.push_error("invalid argument");
    }
    if (argv.size() == 5 && argv[4] != "ASYNCH") {
        return call.push_error("invalid argument");
    };
    auto interface = argv[1];
    auto port = conversion::as_variable(argv[2]).ui();
    bool ssl = argv.size() == 4 && argv[3] == "SSL";
    bool async = argv.size() == 5 && argv[4] == "ASYNCH";
    if (call.is_remote()) async = true;
    if (async) {
        restart.asynch_restart(interface.chars(), port, ssl);
    } else {
        auto failed = restart.inline_restart(interface.chars(), port, ssl);
        if (!failed.empty())
            return call.push_error(("could not listen: " + failed).c_str());
    }
    return call.push_simple("OK");
}
int cmd_START(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, START);
}
int PUBLISH(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    barch::repl::publish(interface.s(), port.i());
    return call.push_simple("OK");
}
int cmd_PUBLISH(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PUBLISH);
}
int PULL(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    auto ks = call.kspace();
    barch::sharded_store store(call.kspace());
    store.each_shard([&](const barch::shard_ptr& t) {
        if (!t->pull(interface.s(), port.i())) {}
    });
    return call.push_simple("OK");
}
int cmd_PULL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PULL);
}
int STOP(caller& call, const arg_t& ) {
    if (call.get_context() == ctx_resp) {
        return call.push_error("Cannot stop server");
    }
    barch::stop_function_sync();
    barch::stop_http_servers();
    if (call.is_remote()) {
        restart.asynch_stop();
    }else {
        barch::server::stop();
    }

    return call.push_simple("OK");
}
int cmd_STOP(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, STOP);
}
/* RPING <host> <port>
 *
 * reach out to another barch and check it answers. This was called PING until the name
 * was given back to redis's health check, which is what every client sends and what a
 * connection pool sends before handing a connection out. The two are unrelated: this
 * one opens a connection to somewhere else, over the binary replication protocol, and
 * says nothing about the server being asked.
 */
int RPING(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    //barch::server::start(interface.chars(), atoi(port.chars()));
    barch::repl::temp_client cli(interface.chars(), atoi(port.chars()), 0);
    if (!cli.ping()) {
        return call.push_error("could not ping");
    }
    return call.push_simple("OK");
}
int cmd_RPING(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, RPING);
}
/*
 * RETRIEVE host port [user secret] - TODO 480.
 *
 * Replaces this key space with the one of the same name on another barch. Every
 * shard comes over first, as shard files beside this space's own, and nothing
 * here changes until all of them have: a connection that drops, a login the other
 * side refuses or a different shard count leaves this space as it was. Then they
 * go in the way LOAD does it - every shard locked, the files installed and
 * loaded, the range table rebuilt, and a change log checkpoint at that moment,
 * since the files are the state now (TODO 479).
 *
 * `user` and `secret` log in on the other side, which needs read rights on the
 * space. Without them it's `default`, the way a RESP client starts out.
 */
int RETRIEVE(caller& call, const arg_t& argv) {

    if (argv.size() != 3 && argv.size() != 5)
        return call.wrong_arity();
    Variable host = argv[1];
    Variable port = argv[2];
    const std::string user = argv.size() == 5 ? argv[3].to_string() : std::string("default");
    const std::string secret = argv.size() == 5 ? argv[4].to_string() : std::string("empty");
    auto ks = call.kspace();
    barch::sharded_store store(ks);

    std::string err;
    barch::repl::temp_client cli(host.s(), (int) port.i(), 0);
    if (!cli.receive_space(ks, user, secret, err))
        return call.push_error(("RETRIEVE: " + err + " - nothing here changed").c_str());

    std::atomic<size_t> errors = 0;
    std::mutex first_mut;
    std::string first;
    const auto& change_log = ks->get_change_log();
    uint64_t installed_at = 0;
    {
        barch::sharded_store::write_guard held = store.lock_space_write();
        installed_at = change_log ? change_log->mark() : 0;
        store.each_shard_parallel([&](const barch::shard_ptr& shard) {
            std::string e;
            if (!shard->install_received_holding_lock(e)) {
                ++errors;
                std::lock_guard l(first_mut);
                if (first.empty()) first = e;
            }
        });
        if (ks->is_range_sharded()) {
            ks->routes().rebuild(store.shards());
        }
    }
    if (errors == 0 && change_log) {
        try {
            change_log->checkpoint(ks->space_name(), installed_at);
            change_log->trim_to_last_checkpoint();
        } catch (const std::exception& e) {
            barch::err({"retrieved, but could not checkpoint the change log:", e.what()});
        }
    }
    if (errors)
        return call.push_error(("RETRIEVE: " + first).c_str());
    return call.push_simple("OK");
}
int cmd_RETRIEVE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, RETRIEVE);
}
}

int add_repl_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(START), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(STOP), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(RPING), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PUBLISH), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PULL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    // it replaces the space's data - TODO 480
    if (ValkeyModule_CreateCommand(ctx, NAME(RETRIEVE), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(LOAD), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

void register_repl_api(function_map& r) {
    r["ADDROUTE"] = {::ADDROUTE,{"write","connection"}};
    r["ROUTE"] = {::ROUTE,{"read","connection"}};
    r["REMROUTE"] = {::REMROUTE,{"write","connection"}};
    r["PUBLISH"] = {::PUBLISH,{"write","connection"}};
    r["PULL"] = {::PULL,{"write","dangerous"}};
    r["LOAD"] = {::LOAD,{"write","dangerous"}};
    r["RELOAD"] = {::RELOAD,{"write","dangerous"}};
    r["START"] = {::START,{"write","connection","data"}};
    r["STOP"] = {::STOP,{"write","connection","data"}};
    r["RETRIEVE"] = {::RETRIEVE,{"write","dangerous","data"}};
    r["RPING"] = {::RPING,{"read","connection","data"}};
}
