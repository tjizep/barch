//
// Created by teejip on 7/26/25.
//

#include "auth_api.h"
#include "keyspec.h"
#include "caller.h"
#include "art.h"
#include "barch_apis.h"
static const std::string CAT_PREFIX = "user:cat:";
static const std::string SECRET_PREFIX = "user:secret:";
static std::string user_cats(std::string user) {
    return CAT_PREFIX + user + ":";
}
static std::string user_secret(std::string user) {
    return SECRET_PREFIX + user;
}
std::mutex& latch() {
    static std::mutex latch;
    return latch;
}
static void add_cats(art::tree * a, const std::string& user,const std::string& secret, const heap::string_map<bool> & cats);
heap::vector<std::string> category_groups() {
    heap::vector<std::string> r = {"all","readonly","admin", "user"};
    return r;
}
static void init_auth(art::tree* auth) {
    if (auth->size == 0) {
        heap::string_map<bool> cats;
        cats.emplace("data",true);
        cats.emplace("all",true);
        {
            write_lock write(auth->latch);
            add_cats(auth,"default","empty",cats);
        }
        auth->save(false);
        art::std_log("Saved initial acl");
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
    auth->set_thread_ap();
    //art::hashed_key::thread_ap = auth;
    return auth;
}
void save_auth() {
    auto a = get_auth();
    a->save(false); //no stats
 }
static void add_cats(art::tree * a, const std::string& user,const std::string& secret, const heap::string_map<bool> & cats) {
    std::string key;
    if (user.empty()) return;
    for (auto& cat : cats) {
        key = user_cats(user);
        key += cat.first;
        std::string value = cat.second ? "true":"false";
        a->insert(key, value);
    }
    if (!secret.empty()) {
        key = user_secret(user);
        a->insert(key, secret);
    }
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
        read_lock read(a->latch);
        catmap cats;
        std::string key = user_cats(user.to_string());
        art::iterator cat_data(a,key);
        while (cat_data.ok()) {

            auto k = cat_data.key();
            auto v = cat_data.value();
            if (!k.starts_with(key)) {
                break;
            }
            std::string cat = k.sub(key.size()).pref(1).to_string();
            cats[cat] = v == "true";
            cat_data.next();

        }
        key = SECRET_PREFIX;
        key += user.to_string();
        auto s = a->search(key);
        if (!s.null() && s.const_leaf()->get_value() == secret) {
            auto acl = cats2vec(cats);
            call.set_acl(user.to_string(), acl);
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
        if (spec.get) {
            read_lock read(a->latch);
            std::string key = user_cats(spec.user);
            call.start_array();
            art::iterator cat_data(a,key);
            heap::vector<art::value_type> to_del;
            while (cat_data.ok()) {

                auto k = cat_data.key();
                auto v = cat_data.value();
                if (!k.starts_with(key)) {
                    break;
                }
                call.reply_values({"$" + k.sub(key.size()).pref(1).to_string(),v.to_string()} );
                cat_data.next();
            }
            call.end_array(0);
            return 0;
        }
        if (spec.del) {
            write_lock write(a->latch);
            std::string key = user_cats(spec.user);
            art::iterator cat_data(a,key);
            heap::vector<art::value_type> to_del;
            while (cat_data.ok()) {

                auto k = cat_data.key();
                if (!k.starts_with(key)) {
                    break;
                }
                to_del.push_back(k);
                cat_data.next();
            }
            for (auto& k : to_del) {
                a->remove(k);
            }
            key = user_secret(spec.user);
            a->remove(key);
            return call.simple("OK");
        }
        auto &valid_categories = get_category_map();
        for (auto& cat : spec.cat) {
            if (cat.first == "all") {
                continue;
            }
            if (!valid_categories.count(cat.first)) {
                return call.error("ACL category not found");
            }
        }

        spec.cat.emplace("data",true); // always add the data right
        spec.cat.emplace("auth",true); // always add the data right
        write_lock write(a->latch);
        add_cats(a,spec.user,spec.secret,spec.cat);
        return call.simple("OK");
    }
}