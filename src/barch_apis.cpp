#include "index_api.h"
#include "sastam.h"
#include "barch_apis.h"
#include "dir_api.h"
#include "keys_api.h"
#include "list_api.h"
#include "hash_api.h"
#include "info_api.h"
#include "ordered_api.h"
#include "connection_api.h"
#include "keyspace_api.h"
#include "repl_api.h"
#include "config_api.h"
#include "auth_api.h"
#include "export_api.h"
#include "function_api.h"
#include "http_api.h"
#include "fs_api.h"
#include "graph_api.h"

// the RESP client's RESP POOL - resp_client.cpp, TODO 379
void register_resp_api(function_map& r);
//
// Created by teejip on 7/13/25.
//
/*
 * Built once, by the first caller, while any other waits - TODO 561. It used to
 * test `r.empty()` before taking the latch, which reads the map while another
 * thread may be filling it. categories() is a fixed list, so there's nothing to
 * build again later.
 */
catmap& get_category_map() {
    static catmap r = [] {
        catmap m;
        size_t at = 0;
        for (auto& c : categories()) {
            m[c] = at++;
        }
        return m;
    }();
    return r;
}


heap::vector<std::string> categories() {
    // appended, never inserted: get_category_map() numbers these by position and
    // is_authorized compares by index, so a name added in the middle silently
    // reassigns everyone's rights. Stored ACLs are keyed by name and re-vectorised
    // at AUTH, which is what makes appending free
    heap::vector<std::string> r = {"read","write","data", "stats",
        "dangerous","acl", "keyspace",
        "keys", "orderedset","hash","list","auth",
        "connection","config","function",
        // scheduling a job is a right of its own: it says who may install a cron
        // entry, not what the job may do - that comes from the user the entry names,
        // in the space it targets. See TODO 249 and 250
        "cron",
        // reaching off the box: http.request today, and whatever TODO 301's socket
        // client becomes. Not a store right and not a key right - it says whether a
        // script may talk to anything that is not this server at all. Without it a
        // user granted `function` so it can run stored code got outbound network
        // reach thrown in, which is not what `function` says. See TODO 304
        "outbound"};

    return r;
}
heap::vector<bool> cats2vec(const catmap& icats) {
    heap::vector<bool> cats;
    auto &catm = get_category_map();
    cats.resize(get_category_map().size());
    for (auto &c : icats) {
        if (c.first == "all") {
            for (size_t i = 0;i < cats.size();++i) {
                cats[i] = c.second != 0;
            }
            continue;
        }
        auto i = catm.find(c.first);
        if (i != catm.end()) {
            cats[i->second] = c.second != 0;
        } //ignore unknown cats
    }
    return cats;
}
/*
 * Built whole before anyone sees it - TODO 561. It used to be filled in place
 * after a `r->empty()` test outside the latch, so another thread could find it
 * half registered (and answer "unknown command"), or read it while it was being
 * written.
 */
std::shared_ptr<function_map>  functions_by_name() {
    static std::shared_ptr<function_map> r = [] {
        auto t = std::make_shared<function_map>();
        auto& m = *t;
        register_keys_api(m);
        register_list_api(m);
        register_hash_api(m);
        register_ordered_api(m);
        register_info_api(m);
        register_connection_api(m);
        register_keyspace_api(m);
        register_repl_api(m);
        register_config_api(m);
        register_auth_api(m);
        register_export_api(m);
        register_function_api(m);
        register_http_api(m);
        register_fs_api(m);
        register_graph_api(m);
        register_index_api(m);
        register_dir_api(m);
        register_resp_api(m);
        return t;
    }();
    return r;
}
