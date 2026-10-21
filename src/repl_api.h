//
// Created by teejip on 8/1/26
//

#ifndef REPL_API_H
#define REPL_API_H

#include <memory>
#include <string>
#include <vector>
#include "../external/include/valkeymodule.h"
#include "barch_apis.h"

/*
 * the replication and cluster commands: routing, pushing and pulling between
 * barch instances, and starting and stopping the RESP server.
 *
 * ADDROUTE, ROUTE and REMROUTE are implemented in rpc/server.cpp, where the routing
 * table lives, and only declared and registered here with the rest of their family.
 */
extern "C" {
    int ADDROUTE(caller& call, const arg_t& argv);
    int ROUTE(caller& call, const arg_t& argv);
    int REMROUTE(caller& call, const arg_t& argv);
    int PUBLISH(caller& call, const arg_t& argv);
    int PULL(caller& call, const arg_t& argv);
    int LOAD(caller& call, const arg_t& argv);
    int RELOAD(caller& call, const arg_t& argv);
    int START(caller& call, const arg_t& argv);
    int STOP(caller& call, const arg_t& argv);
    int RETRIEVE(caller& call, const arg_t& argv);
    // reaches another barch over the replication protocol. PING is redis health
    // check and lives in connection_api.h
    int RPING(caller& call, const arg_t& argv);
    /** apply another barch's replicated shard records - TODO 498 */
    int REPLAPPLY(caller& call, const arg_t& argv);
    int REPLRAFT(caller& call, const arg_t& argv);
    int REPLMISS(caller& call, const arg_t& argv);
    /** this node's replication id, for a RETRIEVE on another - TODO 504 */
    int REPLNODE(caller& call, const arg_t& argv);
}

/** register these commands with the valkey module */
int add_repl_api(ValkeyModuleCtx *ctx);

namespace barch {
    /**
     * START and STOP's restarter: no restart after this, and one on its way is
     * waited for. Part of stop_background_threads - TODO 533
     */
    void stop_repl_restarts();
}

namespace barch::aof { struct record; }
namespace barch { class key_space; }
namespace barch::repl {
    /**
     * One record applied here the way REPLAPPLY applies it: routed to its shard,
     * then set or erased, or the space cleared. The caller holds a repl::applying,
     * so it isn't recorded again. What a Raft group's members apply - TODO 610.
     */
    bool apply_one(const aof::record& r, std::string& why);
    /**
     * The same, into `ks`, which the caller already has, so the space isn't looked up
     * by the name in the record - TODO 714. For a caller holding a space's tx_gate:
     * the lookup takes the keyspace registry lock, and a USE making a new space holds
     * that lock while it takes a tx_gate, so looking up under one can deadlock.
     */
    bool apply_one(const std::shared_ptr<key_space>& ks, const aof::record& r, std::string& why);
    /**
     * Run `commands` in turn on another barch over the barch port, in one session,
     * and give the last one's reply flattened to strings - TODO 687. False, with
     * `why`, when it can't be reached or a command answers an error.
     */
    bool ask(const std::string& host, int port, const std::vector<std::vector<std::string>>& commands,
             std::vector<std::string>& out, std::string& why);
}

/** register them for RESP, into the table functions_by_name() builds */
void register_repl_api(function_map& r);

#endif //REPL_API_H
