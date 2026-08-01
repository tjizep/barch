#include "sastam.h"
#include "barch_apis.h"
#include "keys_api.h"
#include "list_api.h"
#include "hash_api.h"
#include "info_api.h"
#include "ordered_api.h"
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
    heap::vector<std::string> r = {"read","write","data", "stats",
        "dangerous","acl", "keyspace",
        "keys", "orderedset","hash","list","auth",
        "connection","config"};

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
        (*r)["SIZE"] = {::SIZE,{"read"}};
        (*r)["DBSIZE"] = {::SIZE,{"read"}};
        (*r)["SIZEALL"] = {::SIZEALL,{"read"}};
        (*r)["TRAIN"] = {::TRAIN,{"write"}};
        (*r)["USE"] = {::USE,{"write"}};
        (*r)["SELECT"] = {::USE,{"write"}};
        (*r)["KSOPTIONS"] = {::KSOPTIONS,{"write"}};
        (*r)["UNLOAD"] = {::UNLOAD,{"write"}};
        (*r)["SPACES"] = {::SPACES,{"read"}};
        (*r)["KSPACE"] = {::KSPACE,{"read","write"}};

        (*r)["SAVE"] = {::SAVE,{"read"}};
        (*r)["SAVEALL"] = {::SAVEALL,{"read"}};
        (*r)["AUTH"] = {::AUTH,{"auth"}};
        (*r)["ACL"] = {::ACL,{"write","acl"}};

        (*r)["FLUSHDB"] = {::CLEAR,{"write","dangerous"}};
        (*r)["CLEARALL"] = {::CLEARALL,{"write","dangerous"}};
        (*r)["FLUSHALL"] = {::CLEAR,{"write","dangerous"}};
        (*r)["STATS"] = {::STATS,{"read","stats"}};
        (*r)["OPS"] = {OPS,{"read","stats"}};
        register_info_api(*r);
        // COMMAND is deliberately not registered. It is implemented for the valkey
        // module, where the server asks the module to describe itself; over RESP a
        // client that sends COMMAND gets "unknown command" and falls back, which is
        // what we want until there is a command table worth publishing.
        (*r)["CLIENT"] = {CLIENT,{"read","stats"}};
        (*r)["HELLO"] = {HELLO,{"connection"}};

        (*r)["MULTI"] = {MULTI,{"write"}};
        (*r)["EXEC"] = {EXEC,{"write"}};

        (*r)["ADDROUTE"] = {::ADDROUTE,{"write","connection"}};
        (*r)["ROUTE"] = {::ROUTE,{"read","connection"}};
        (*r)["REMROUTE"] = {::REMROUTE,{"write","connection"}};
        (*r)["PUBLISH"] = {::PUBLISH,{"write","connection"}};
        (*r)["PULL"] = {::PULL,{"write","dangerous"}};
        (*r)["LOAD"] = {::LOAD,{"write","dangerous"}};
        (*r)["RELOAD"] = {::RELOAD,{"write","dangerous"}};
        (*r)["CONFIG"] = {::CONFIG,{"write","read","config"}};

        register_list_api(*r);
        (*r)["START"] = {::START,{"write","connection","data"}};
        (*r)["STOP"] = {::STOP,{"write","connection","data"}};
        (*r)["RETRIEVE"] = {::RETRIEVE,{"write","dangerous","data"}};
        (*r)["RPING"] = {::RPING,{"read","connection","data"}};
        (*r)["PING"] = {::PING,{"read","connection"}};
        register_hash_api(*r);
        register_ordered_api(*r);
    }

    return r;
}
