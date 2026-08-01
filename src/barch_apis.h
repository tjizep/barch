//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include "caller.h"
typedef std::function<int (caller& call, const arg_t& argv)> barch_function;
typedef heap::string_map<size_t> catmap;
heap::vector<std::string> categories();
catmap& get_category_map();
heap::vector<bool> cats2vec(const catmap& icats);

struct barch_info {
    barch_info() = default;
    void set_cats(const std::initializer_list<const char *>& icats) {
        catmap mycats;
        for (auto c : icats) {
            mycats[c] = true;
        }
        this->cats = cats2vec(mycats);
        this->dp = get_category_map().at("data");
        this->wr = get_category_map().at("write");
    }
    barch_info(const barch_function& call, const std::initializer_list<const char *>& cats, bool asynch = false) : call(call), is_asynch(asynch) {
        set_cats(cats);
    }
    barch_info(const barch_info& binfo) = default;
    barch_info& operator=(const barch_info& binfo) = default;
    barch_info& operator=(barch_info&& binfo) = default;
    barch_info(barch_info&& binfo) = delete;
    bool is_data() const {
        return cats[dp];
    }
    bool is_write() const {
        return cats[wr];
    }
    barch_function call{};
    heap::vector<bool> cats{};
    uint64_t calls {0};
    bool is_asynch{false};
    int dp = 0;
    int wr = 0;
    uint64_t total_nanos{};
};
typedef heap::string_map<barch_info> function_map;

/* The command declarations below are the ones that have not yet been given a
 * {category}_api.h of their own - see TODO 22. Keys, lists, hashes, ordered sets and
 * info now declare their own commands and register them from their own translation
 * unit, through register_*_api(). */
extern "C"{
    // Misc/sys
    // not registered in functions_by_name: it serves the valkey module, where the
    // server asks the module to describe itself, rather than RESP clients
    int COMMAND(caller& call,const arg_t& argv);
    int AUTH(caller& call,const arg_t& argv);
    int ACL(caller& call,const arg_t& argv);
    int CLIENT(caller& call, const arg_t& arg_v);
    int HELLO(caller& call, const arg_t& argv);
    int MULTI(caller& call, const arg_t& arg_v);
    int EXEC(caller& call, const arg_t& arg_v);
    // database
    int USE(caller& call, const arg_t& argv);
    int UNLOAD(caller& call, const arg_t& argv);
    int SPACES(caller& call, const arg_t& argv);
    int KSPACE(caller& call, const arg_t& argv);
    // size in current keyspace
    int SIZE(caller& call, const arg_t& argv);
    // total count in the entire db
    int SIZEALL(caller& call, const arg_t& argv);
    int SAVE(caller& call, const arg_t& argv);
    int CLEAR(caller& call, const arg_t& argv);
    // save and clear all key spaces
    int CLEARALL(caller& call, const arg_t& argv);
    int SAVEALL(caller& call, const arg_t& argv);
    int KSOPTIONS(caller& call, const arg_t& argv);
    // replication+cluster
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
    // reaches another barch over the replication protocol; PING is redis's health check
    int RPING(caller& call, const arg_t& argv);
    int PING(caller& call, const arg_t& argv);
    // compression
    int TRAIN(caller& call, const arg_t& argv);
    // stats
    int OPS(caller& call, const arg_t& argv);
    int STATS(caller& call, const arg_t& argv);

    // config
    int CONFIG(caller& call, const arg_t& argv);
}

extern std::shared_ptr<function_map> functions_by_name();
#endif //BARCH_APIS_H
