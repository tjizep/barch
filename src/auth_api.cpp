//
// Created by teejip on 7/26/25.
//

#include "auth_api.h"
#include "repo_library.h"
#include "keyspec.h"
#include "caller.h"
#include "shard.h"
#include "barch_apis.h"
#include "art/iterator.h"
#include "cluster_hooks.h"
#include "function_api.h"
#include "key_space.h"
#include "meta_keys.h"

#include <map>
#include <mutex>

static const std::string CAT_PREFIX = "user:cat:";
static const std::string SECRET_PREFIX = "user:secret:";
static std::string user_cats(std::string user) {
    return CAT_PREFIX + user + ":";
}
/*
 * `user:space:<user>:<space>:<cat>` - one level below the categories, so the same
 * prefix walk reads it. See TODO 135.
 */
static const std::string SPACE_PREFIX = "user:space:";
static std::string user_space_cats(const std::string& user, const std::string& space) {
    return SPACE_PREFIX + user + ":" + space + ":";
}
static std::string user_space_prefix(const std::string& user) {
    return SPACE_PREFIX + user + ":";
}
static std::string user_secret(std::string user) {
    return SECRET_PREFIX + user;
}
static void add_cats(barch::shard_ptr a, const std::string& user,const std::string& secret, const heap::string_map<bool> & cats);
heap::vector<std::string> category_groups() {
    heap::vector<std::string> r = {"all","readonly","admin", "user"};
    return r;
}
static void init_auth(barch::shard_ptr auth) {
    if (auth->get_tree_size() == 0) {
        heap::string_map<bool> cats;
        cats.emplace("data",true);
        cats.emplace("all",true);
        heap::string_map<bool> web;
        web.emplace("read", true);
        web.emplace("write", true);
        web.emplace("data", true);
        web.emplace("stats", true);
        web.emplace("keys", true);
        web.emplace("connection", true);
        {
            unique_latch write(auth->get_latch());
            add_cats(auth,"default","empty",cats);
            add_cats(auth,"web","",web);
        }
        auth->save(false);
        barch::log({"Saved initial acl"});
    }
}

/*
 * These two are built once, by the first caller, while any other waits - TODO
 * 561. Each used to test itself (`empty()`, `!auth`) before taking the latch,
 * which reads it while another thread may be writing it.
 */
const heap::vector<bool>& get_all_acl() {
    static const heap::vector<bool> all_acl(categories().size(), true);
    return all_acl;
}

/** this node's own users: its auth file */
static barch::shard_ptr local_auth() {
    static const barch::shard_ptr auth = [] {
        heap::allocator<barch::shard> alloc;
        auto a = std::allocate_shared<barch::shard>(alloc, "auth", nullptr, 0, 0);
        a->opt_ordered_keys = true;
        a->load(false);
        init_auth(a);
        return a;
    }();
    return auth;
}

/*
 * The cluster's users - TODO 676. See auth_api.h. The store is one meta key in the
 * `cluster` space; `cluster_copy` is the same in a shard, so the code that reads users
 * reads it as it reads the file. Never loaded or saved: it's built from the key.
 */
namespace {
    const char* users_meta = "auth.users";
    using store_map = std::map<std::string, std::string>;

    barch::shard_ptr cluster_copy() {
        static const barch::shard_ptr copy = [] {
            heap::allocator<barch::shard> alloc;
            auto a = std::allocate_shared<barch::shard>(alloc, "auth.cluster", nullptr, 0, 0);
            a->opt_ordered_keys = true;
            return a;
        }();
        return copy;
    }

    /** the `cluster` space when this node is in a cluster */
    barch::key_space_ptr users_space() {
        if (!barch::cluster::any_bound())
            return nullptr;
        // only once it's open: opening it reads the users - see loaded_keyspace
        auto ks = barch::loaded_keyspace("cluster");
        return ks && ks->has_raft() ? ks : nullptr;
    }

    // `<length>:<key><length>:<value>` for each, in key order
    std::string encode(const store_map& m) {
        std::string out;
        for (const auto& [k, v] : m) {
            out += std::to_string(k.size()) + ":" + k;
            out += std::to_string(v.size()) + ":" + v;
        }
        return out;
    }

    bool decode(const std::string& s, store_map& m) {
        m.clear();
        size_t at = 0;
        auto next = [&](std::string& into) {
            const auto colon = s.find(':', at);
            if (colon == std::string::npos)
                return false;
            size_t n = 0;
            try {
                n = std::stoull(s.substr(at, colon - at));
            } catch (const std::exception&) {
                return false;
            }
            if (colon + 1 + n > s.size())
                return false;
            into = s.substr(colon + 1, n);
            at = colon + 1 + n;
            return true;
        };
        while (at < s.size()) {
            std::string k, v;
            if (!next(k) || !next(v))
                return false;
            m[k] = v;
        }
        return true;
    }

    store_map read_all(const barch::shard_ptr& a) {
        store_map m;
        read_lock read(a);
        const std::string prefix = "user:";
        barch::iterator it(a, prefix);
        while (it.ok()) {
            auto k = it.key();
            if (!k.starts_with(prefix))
                break;
            m[k.to_string()] = it.value().to_string();
            it.next();
        }
        return m;
    }

    std::mutex copy_mu;
    std::string copy_blob;          // what cluster_copy was built from, under copy_mu
    bool copy_valid{false};

    void fill(const barch::shard_ptr& a, const store_map& m) {
        unique_latch write(a->get_latch());
        heap::vector<std::string> gone;
        {
            const std::string prefix = "user:";
            barch::iterator it(a, prefix);
            while (it.ok()) {
                auto k = it.key();
                if (!k.starts_with(prefix))
                    break;
                gone.push_back(k.to_string());
                it.next();
            }
        }
        for (const auto& k : gone)
            a->remove(art::value_type{k});
        for (const auto& [k, v] : m)
            a->insert(k, v, true);
    }

    /** inside a run from the log that changes users: the store's there, empty at worst */
    void begin_users_run() {
        auto ks = users_space();
        if (!ks || !barch::functions::running_from_log())
            return;
        std::string blob, err;
        if (!barch::meta::get(ks, users_meta, blob) && !barch::meta::set(ks, users_meta, encode({}), err))
            throw std::runtime_error("could not start the cluster's users: " + err);
    }

    /** and after it: what the copy holds now is the cluster's store, on every member */
    void end_users_run() {
        auto ks = users_space();
        if (!ks || !barch::functions::running_from_log())
            return;
        const auto blob = encode(read_all(cluster_copy()));
        std::string err;
        if (!barch::meta::set(ks, users_meta, blob, err))
            throw std::runtime_error("could not keep the cluster's users: " + err);
        std::lock_guard l(copy_mu);
        copy_blob = blob;
        copy_valid = true;
    }

    std::vector<std::string> words_of(const arg_t& argv) {
        std::vector<std::string> w;
        for (const auto& a : argv)
            w.push_back(a.to_string());
        return w;
    }
}

barch::shard_ptr get_auth() {
    if (auto ks = users_space()) {
        std::string blob;
        if (barch::meta::get(ks, users_meta, blob)) {
            std::lock_guard l(copy_mu);
            if (!copy_valid || blob != copy_blob) {
                store_map m;
                if (!decode(blob, m))
                    barch::err({"the cluster's users don't read; using them as far as they do"});
                fill(cluster_copy(), m);
                copy_blob = blob;
                copy_valid = true;
            }
            return cluster_copy();
        }
    }
    return local_auth();
}
void save_auth() {
    // the node's own file; the cluster's users are in the cluster space
    local_auth()->save(false); //no stats
}

namespace barch {
    bool route_users_change(const std::vector<std::string>& words, std::string& err) {
        auto ks = users_space();
        if (!ks || barch::functions::running_from_log())
            return false;
        err.clear();
        barch::cluster::reply_ptr reply;
        std::string blob;
        if (!barch::meta::get(ks, users_meta, blob)) {
            // the cluster's first change: this node's users go in first, so nobody who
            // could log in here is lost
            if (!barch::cluster::log_server_command(ks, {auth_seed_word, encode(read_all(local_auth()))}, reply, err))
                return true;
        }
        barch::cluster::log_server_command(ks, words, reply, err);
        return true;
    }

    bool seed_users_from_log(const std::vector<std::string>& words, std::string& err) {
        auto ks = users_space();
        if (!ks || words.size() != 2) {
            err = "a users seed entry outside a cluster, or with " + std::to_string(words.size()) + " words";
            return false;
        }
        std::string have;
        if (barch::meta::get(ks, users_meta, have))
            return true;                    // the first seed stays
        return barch::meta::set(ks, users_meta, words[1], err);
    }

    void seed_cluster_users() {
        auto ks = users_space();
        if (!ks)
            return;
        std::string have, err;
        if (barch::meta::get(ks, users_meta, have))
            return;
        barch::cluster::reply_ptr reply;
        if (!barch::cluster::log_server_command(ks, {auth_seed_word, encode(read_all(local_auth()))}, reply, err))
            barch::err({"cluster: could not make this node's users the cluster's:", err});
        else
            barch::log({"cluster: this node's users are the cluster's"});
    }
}
static void add_cats(barch::shard_ptr a, const std::string& user,const std::string& secret, const heap::string_map<bool> & cats) {
    std::string key;
    if (user.empty()) return;
    for (auto& cat : cats) {
        key = user_cats(user);
        key += cat.first;
        std::string value = cat.second ? "true":"false";
        a->insert(key, value, true);
    }
    if (!secret.empty()) {
        key = user_secret(user);
        a->insert(key, secret, true);
    }
}
namespace barch {
space_overrides read_space_overrides(const std::string& user) {
    barch::space_overrides out;
    auto a = get_auth();
    read_lock read(a);
    std::string prefix = user_space_prefix(user);
    barch::iterator it(a, prefix);
    while (it.ok()) {
        auto k = it.key();
        if (!k.starts_with(prefix))
            break;
        // <space>:<cat> - the space name cannot hold a colon, so the last one splits
        std::string rest = k.sub(prefix.size()).pref(1).to_string();
        auto colon = rest.find_last_of(':');
        if (colon != std::string::npos) {
            out[rest.substr(0, colon)][rest.substr(colon + 1)] = it.value() == "true";
        }
        it.next();
    }
    return out;
}

heap::vector<bool> apply_overrides(const heap::vector<bool>& global,
                                   const heap::string_map<bool>& over) {
    heap::vector<bool> r = global;
    auto& catm = get_category_map();
    // "all" first and the named categories after, so `-all +read` means what it
    // says. The map has no order, so taking them as they came let "all" land
    // after a named one about half the time - TODO 594
    auto all = over.find("all");
    if (all != over.end()) {
        for (size_t i = 0; i < r.size(); ++i)
            r[i] = all->second;
    }
    for (const auto& c : over) {
        if (c.first == "all")
            continue;
        auto i = catm.find(c.first);
        if (i != catm.end() && i->second < r.size()) {
            r[i->second] = c.second;
        }
    }
    return r;
}

bool server_written(const std::string& canonical_space) {
    // `cluster` holds the members and their state, which the cluster writes
    // itself through its Raft group - TODO 610
    return canonical_space == barch::library::space_name || canonical_space == "cluster";
}

heap::vector<bool> rights_in(const std::string& canonical_space,
                             const heap::vector<bool>& global,
                             const space_overrides& overrides) {
    auto o = overrides.find(canonical_space);
    heap::vector<bool> r = o == overrides.end() ? global : apply_overrides(global, o->second);
    if (server_written(canonical_space)) {
        auto w = get_category_map().at("write");
        if (w < r.size())
            r[w] = false;
    }
    return r;
}

/** the change itself, in whichever store get_auth() gives */
static void write_overrides_here(const std::string& user, const std::string& space,
                                 const heap::string_map<bool>& cats) {
    auto a = get_auth();
    unique_latch write(a->get_latch());
    std::string prefix = user_space_cats(user, space);
    // whatever was there goes first, so a SETUSER states the whole rule for this
    // space rather than adding to one nobody can see
    heap::vector<art::value_type> to_del;
    barch::iterator it(a, prefix);
    while (it.ok()) {
        auto k = it.key();
        if (!k.starts_with(prefix))
            break;
        to_del.push_back(k);
        it.next();
    }
    for (auto& k : to_del) {
        a->remove(k);
    }
    for (const auto& c : cats) {
        a->insert(prefix + c.first, c.second ? "true" : "false", true);
    }
}

void write_space_overrides(const std::string& user, const std::string& space,
                           const heap::string_map<bool>& cats) {
    begin_users_run();
    write_overrides_here(user, space, cats);
    end_users_run();
}
}

static heap::vector<bool> web_acl() {
    catmap cats;
    cats["read"] = true;
    cats["write"] = true;
    cats["data"] = true;
    cats["stats"] = true;
    cats["keys"] = true;
    cats["connection"] = true;
    return cats2vec(cats);
}

static bool load_user_cats(const std::string& user, catmap& cats) {
    auto a = get_auth();
    read_lock read(a);
    std::string key = user_cats(user);
    barch::iterator cat_data(a, key);
    while (cat_data.ok()) {
        auto k = cat_data.key();
        auto v = cat_data.value();
        if (!k.starts_with(key))
            break;
        std::string cat = k.sub(key.size()).pref(1).to_string();
        cats[cat] = v == "true";
        cat_data.next();
    }
    return !cats.empty();
}

heap::vector<bool> acl_for_user(const std::string& user) {
    catmap cats;
    if (load_user_cats(user, cats))
        return cats2vec(cats);
    if (user == "web" || user.empty())
        return web_acl();
    if (user == "default")
        return get_all_acl();
    return {};
}

bool authenticate_user(const std::string& user, const std::string& secret,
                       heap::vector<bool>& acl) {
    std::string u = user.empty() ? "default" : user;
    auto a = get_auth();
    if (a->get_size() == 0) {
        acl = get_all_acl();
        return true;
    }
    catmap cats;
    load_user_cats(u, cats);
    std::string key = SECRET_PREFIX;
    key += u;
    read_lock read(a);
    auto s = a->search(key);
    if (!s.null() && s.const_leaf()->get_value() == art::value_type{secret}) {
        acl = cats.empty() ? get_all_acl() : cats2vec(cats);
        return true;
    }
    // web has no secret; AUTH as web is not a login, it is the anonymous user
    if (u == "web" && secret.empty()) {
        acl = acl_for_user("web");
        return true;
    }
    return false;
}

extern "C"
{
    int AUTH(caller& call, const arg_t& argv) {
        if (argv.size() < 3) {
            return call.push_error("AUTH requires at least 3 arguments");
        }
        auto user = argv[1];
        auto secret = argv[2];
        if (user.empty()) user = "default";
        auto a = get_auth();
        if (a->get_size() == 0) {
            call.set_acl("default", get_all_acl());
            return call.push_simple("OK");
        }
        read_lock read(a);
        catmap cats;
        std::string key = user_cats(user.to_string());
        barch::iterator cat_data(a,key);
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
            return call.push_simple("OK");
        }
        return call.push_error("authentication failed");
    }
    int ACL(caller& call, const arg_t& argv) {
        if (argv.size() < 3) {
            return call.push_error("ACL requires at least 3 arguments");
        }
        barch::acl_spec spec(argv);
        if (spec.parse_options() != 0) {
            return call.push_error("ACL syntax error");
        }
        // a `~pattern` parses into acl_spec::filters and nothing has ever read it, so
        // key patterns used to be accepted and then quietly dropped - a client asking
        // for key level rights got OK and no rights, along with whatever categories it
        // sent in the same breath. Refused until key patterns mean something. TODO 136
        if (spec.is_filter) {
            return call.push_error("ACL key patterns are not supported");
        }
        if (!spec.get) {
            if (!spec.del) {
                auto &valid = get_category_map();
                for (auto& cat : spec.cat) {
                    if (cat.first != "all" && !valid.count(cat.first))
                        return call.push_error("ACL category not found");
                }
            }
            // in a cluster the users are the cluster's: the change goes in its log - TODO 676
            std::string err;
            if (barch::route_users_change(words_of(argv), err))
                return err.empty() ? call.push_simple("OK") : call.push_error(err.c_str());
            begin_users_run();
        }
        auto a = get_auth();
        if (spec.get) {
            read_lock read(a);
            std::string key = user_cats(spec.user);
            call.start_array();
            barch::iterator cat_data(a,key);
            heap::vector<art::value_type> to_del;
            while (cat_data.ok()) {

                auto k = cat_data.key();
                auto v = cat_data.value();
                if (!k.starts_with(key)) {
                    break;
                }
                call.push_values({"$" + k.sub(key.size()).pref(1).to_string(),v.to_string()} );
                cat_data.next();
            }
            call.end_array();
            return 0;
        }
        if (spec.del) {
            {
                unique_latch write(a->get_latch());
                std::string key = user_cats(spec.user);
                barch::iterator cat_data(a,key);
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
            }
            end_users_run();
            return call.push_simple("OK");
        }
        auto &valid_categories = get_category_map();
        for (auto& cat : spec.cat) {
            if (cat.first == "all") {
                continue;
            }
            if (!valid_categories.count(cat.first)) {
                return call.push_error("ACL category not found");
            }
        }

        spec.cat.emplace("data",true); // always add the data right
        spec.cat.emplace("auth",true); // always add the data right
        {
            unique_latch write(a->get_latch());
            add_cats(a,spec.user,spec.secret,spec.cat);
        }
        end_users_run();
        return call.push_simple("OK");
    }
}

/* the auth commands as a RESP client sees them */
void register_auth_api(function_map& r) {
    r["AUTH"] = {::AUTH,{"auth"}};
    r["ACL"] = {::ACL,{"write","acl"}};
}
