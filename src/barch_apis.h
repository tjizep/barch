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

/*
 * Every command now declares itself in its own {category}_api.h and registers itself
 * from the matching .cpp, through register_*_api(). What is left here is the vocabulary
 * they are all built from: the function signature, the category map that drives ACLs,
 * and the table itself.
 */

extern std::shared_ptr<function_map> functions_by_name();
#endif //BARCH_APIS_H
