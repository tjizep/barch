#include "sastam.h"
#include "barch_apis.h"
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
//
// Created by teejip on 7/13/25.
//
static std::recursive_mutex& latch() {
    static std::recursive_mutex l{};
    return l;
}
catmap& get_category_map() {
    static catmap r;
    if (r.empty()) {
        std::unique_lock lock(latch());
        if (!r.empty()) return r;
        size_t at = 0;
        for (auto& c : categories()) {
            r[c] = at++;
        }
    }
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
        "connection","config","function"};

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
std::shared_ptr<function_map>  functions_by_name() {
    static std::shared_ptr<function_map> r = std::make_shared<function_map>();
    if (r->empty()) {
        std::unique_lock lock(latch());
        if (!r->empty()) return r;
        register_keys_api(*r);
        register_list_api(*r);
        register_hash_api(*r);
        register_ordered_api(*r);
        register_info_api(*r);
        register_connection_api(*r);
        register_keyspace_api(*r);
        register_repl_api(*r);
        register_config_api(*r);
        register_auth_api(*r);
        register_export_api(*r);
        register_function_api(*r);
    }

    return r;
}
