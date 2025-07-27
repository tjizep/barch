//
// Created by teejip on 7/26/25.
//

#include "auth_api.h"
#include "keyspec.h"
#include "caller.h"
#include "art.h"
#include "barch_apis.h"
std::mutex& latch() {
    static std::mutex latch;
    return latch;
}

heap::vector<std::string> category_groups() {
    heap::vector<std::string> r = {"all","readonly","admin", "user"};
    return r;
}
static void init_auth(art::tree* auth) {
    if (auth->size == 0) {

    }
}

const heap::vector<bool>& get_all_acl() {
    static heap::vector<bool> all_acl;
    if (all_acl.empty()) {
        std::lock_guard lock(latch());
        if (!all_acl.empty()) return all_acl;
        heap::vector<bool> temp;
        auto cats = categories();
        temp.reserve(cats.size());
        for (size_t c = 0;c < cats.size();++c) {
            temp.emplace_back(true);
        }
        all_acl.swap(temp);
    }
    return all_acl;
}

art::tree * get_auth() {
    static art::tree * auth = nullptr;
    if (!auth) {
        std::lock_guard lock(latch());
        if (auth) return auth;
        auth = new(heap::allocate<art::tree>(1)) art::tree("auth", nullptr, 0, 0);
        auth->load();
        init_auth(auth);
    }
    return auth;
}
void save_auth() {
    auto a = get_auth();
    a->save();
}
extern "C"
{
    int AUTH(caller& call, const arg_t& argv) {
        if (argv.size() < 3) {
            return call.error("AUTH requires at least 3 arguments");
        }
        auto user = argv[1];
        auto secret = argv[2];
        if (user.empty()) user = "default";
        auto a = get_auth();
        if (a->size == 0) {
            call.set_acl("default", get_all_acl());
            return call.simple("OK");
        }
        heap::string_map<bool> cats;
        std::string key = "user:cat:";
        key += user.to_string();
        key += ":";
        art::iterator cat_data(a,key);
        while (cat_data.ok()) {

            auto k = cat_data.key();
            auto v = cat_data.value();
            if (!k.starts_with(key)) {
                break;
            }
            std::string cat = k.sub(key.size()).to_string();
            cats[cat] = v == "true";
            cat_data.next();

        }
        key = "user:secret:";
        key += user.to_string();
        art::value_type key_secret = key;
        auto s = a->search(key_secret.ex());
        if (!s.null() && s.const_leaf()->get_value() == secret) {
            call.set_acl(user.to_string(), get_all_acl());
            return call.simple("OK");
        }
        return call.error("authentication failed");
    }
    int ACL(caller& call, const arg_t& argv) {
        if (argv.size() < 3) {
            return call.error("ACL requires at least 3 arguments");
        }
        art::acl_spec spec(argv);
        if (spec.parse_options() != 0) {
            return call.error("ACL syntax error");
        }
        auto a = get_auth();

        auto &valid_categories = get_category_map();
        for (auto& cat : spec.cat) {
            if (!valid_categories.contains(cat.first)) {
                return call.error("ACL category not found");
            }
        }
        spec.cat.emplace("data",true);
        write_lock write(a->latch);
        std::string key;
        for (auto& cat : spec.cat) {
            key = "user:cat:";
            key += spec.user;
            key += ":";
            key += cat.first;
            std::string value = cat.second ? "true":"false";
            art::value_type vkey = key;
            a->insert(vkey.ex(), value);
        }
        if (spec.is_secret) {
            key = "user:secret:";
            key += spec.user;
            art::value_type vkey = key;
            a->insert(vkey.ex(), spec.secret);
        }
        return call.simple("OK");
    }
}