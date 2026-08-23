//
// Created by teejip on 10/22/25.
//

#include "key_space.h"
#include <thread>
#include <version.h>

#include "shard.h"
#include "keys.h"
#include "module.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "rpc/server.h"
#include "a5hash.h"
#include "configuration.h"
#include "foreign/driver.h"
#include "foreign/sql.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <mutex>
namespace barch {

    static std::string lower_copy(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return s;
    }

    static key_space::foreign_kind parse_foreign_kind(const std::string& raw) {
        auto v = lower_copy(raw);
        if (v.empty() || v == "off" || v == "none" || v == "no" || v == "false")
            return key_space::foreign_kind::off;
        if (v == "mysql") return key_space::foreign_kind::mysql;
        if (v == "postgres" || v == "postgresql") return key_space::foreign_kind::postgres;
        if (v == "luau") return key_space::foreign_kind::luau;
        if (v == "fake") return key_space::foreign_kind::fake;
        return key_space::foreign_kind::off;
    }

    const char *key_space::foreign_kind_name() const {
        switch (opt_foreign) {
            case foreign_kind::mysql: return "mysql";
            case foreign_kind::postgres: return "postgres";
            case foreign_kind::luau: return "luau";
            case foreign_kind::fake: return "fake";
            case foreign_kind::off:
            default: return "off";
        }
    }

    uint64_t key_space::waiter_timeout_ms() const {
        if (foreign_timeout_ms != 0) return foreign_timeout_ms;
        return get_foreign_timeout_ms();
    }

    uint64_t key_space::script_insns() const {
        if (foreign_script_insns != 0) return foreign_script_insns;
        return get_foreign_script_insns();
    }

    uint64_t key_space::pool_max_age_ms() const {
        if (foreign_pool_max_age_ms != 0) return foreign_pool_max_age_ms;
        return get_foreign_pool_max_age_ms();
    }

    void key_space::drop_idle_sql() {
        if (sql)
            sql->drop_idle();
    }

    static bool literal_split_char(const std::string& pat, char& ch) {
        if (pat.size() != 1)
            return false;
        ch = pat[0];
        return true;
    }

    conversion::comparable_key key_space::encode_key(art::value_type v, bool noint) const {
        char ch = 0;
        if (literal_split_char(key_split, ch))
            return conversion::as_composite(v, noint, ch);
        if (key_split_re)
            return conversion::as_composite(v, noint, key_split_re.get());
        return conversion::as_composite(v, noint);
    }

    static void read_u64(KeyValue& kv, const std::string& key, uint64_t& dest) {
        auto s = kv.get(key);
        if (!s.empty())
            conversion::to(s, dest);
    }

    struct key_spaces {
        key_spaces() {
            barch::log({"Starting Barch",
                "\n",
                "\n\tversion","[",BARCH_PROJECT_VERSION,"]",
                "\n\tpage_size","[",(size_t)page_size,"] bytes",
                "maximum_allocation_size [",(size_t)maximum_allocation_size,"] bytes",
                "\n\tshards","[",get_shard_count().size(),"]",
                "\n\tactive_defrag","[",get_active_defrag(),"]",
                "ordered_keys","[",get_ordered_keys(),"]",
                "\n\tmax_module_memory","[",get_max_module_memory()/(1024.0f*1024.0f*1024.0f),"] GB"
                "\n\tsave_interval","[",get_save_interval(),"] ms"
                "\n\tmin_threads","[",thread_pool::get_min_threads(),"]",
                "\n\tresp service threads","[",(thread_pool::get_system_threads()*resp_pool_factor)/100.0f,"] "
                "socket accept threads","[",(thread_pool::get_system_threads()*tcp_accept_pool_factor)/100.0f,"]"
                "\n\tdefault eviction policy","[",get_eviction_policy(),"]",
                "\n\tcompression","[",get_compression_enabled(),"]","\n"});

        };
        ~key_spaces() {

        }
        std::recursive_mutex lock{};
        std::string ks_pattern = "[0-9,A-Z,a-z,_]+";
        std::string ks_pattern_error = "space name does not match the "+ks_pattern+" pattern";
        std::regex name_check{ks_pattern};
        heap::map<std::string, key_space_ptr> spaces{};

    };

    key_spaces& ksp() {
        static key_spaces _ksp;
        return _ksp;
    }


    static std::string decorate(const std::string& name_) {
        if (name_.empty() || name_ == "0")
            return "node";
        return name_ + "_"; // so that system stores dont get clobbered
    }

    static std::string undecorate(const std::string& name_) {
        if (name_ == "node")
            return "";
        std::string r = name_;
        if (!r.empty())
            r.resize(r.size()-1);
        return r;
    }

    std::string ks_undecorate(const std::string& name) {
        return undecorate(name);
    }

    const std::string& get_ks_pattern_error() {
        return ksp().ks_pattern_error;
    }

    void all_shards(const std::function<void(const barch::shard_ptr&)>& cb ) {

        heap::map<std::string, key_space_ptr> spaces;
        {
            std::unique_lock l(ksp().lock);
            spaces = ksp().spaces;
        }
        for (auto &ks : spaces) {
            auto shards = ks.second->get_shards();
            for (auto &shard_ : shards) {
                cb(shard_);
            }
        }
    }

    void all_spaces(const std::function<void(const std::string& name, const barch::key_space_ptr&)>& cb ) {
        heap::map<std::string, key_space_ptr> spaces;
        {
            std::unique_lock l(ksp().lock);
            spaces = ksp().spaces;
        }
        for (auto ks : spaces) {
            auto un = undecorate(ks.first);
            if (un.empty()) un = "(default)";
            cb(un, ks.second);
        }
    }

    bool check_ks_name(const std::string& name_) {
        auto name = decorate(name_);
        return std::regex_match(name, ksp().name_check);
    }
    bool is_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            return false;
        }
        std::unique_lock l(ksp().lock);
        std::string name = decorate(name_);
        auto s = ksp().spaces.find(name);
        return  (s != ksp().spaces.end());
    }
    key_space_ptr get_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            throw_exception<std::invalid_argument>(get_ks_pattern_error().c_str());
        }
        std::unique_lock l(ksp().lock);
        std::string name = decorate(name_);
        auto s = ksp().spaces.find(name);
        if (s != ksp().spaces.end()) {
            return s->second;
        }

        heap::allocator<key_space> alloc;
        // cannot create keyspace without memory
        auto ks = std::allocate_shared<key_space>(alloc, name);
        ksp().spaces[name] = ks;
        return ks;
    }

    bool unload_keyspace(const std::string& name) {
        return flush_keyspace(name);
    }

    bool flush_keyspace(const std::string& name_) {
        bool r = false;
        if (!check_ks_name(name_)) {
            throw_exception<std::invalid_argument>(get_ks_pattern_error().c_str());
        }
        std::string name = decorate(name_);
        key_space_ptr held;
        {
            std::unique_lock l(ksp().lock);
            auto s = ksp().spaces.find(name);
            if (s != ksp().spaces.end()) {
                held = s->second;
                ksp().spaces.erase(s);
                r = true;
            }
        }
        if (held)
            held->fail_foreign_flights();
        return r; // destruction happens in callers thread - so hopefully no dl because shared ptr
    }

    key_space::key_space(const std::string &name) :name(name) {
        if (shards.empty()) {
            // everything allocated while this space is built counts towards startup memory
            uint64_t memory_before = get_total_memory();
            decltype(shards) shards_out;
            if (name == "configuration" || name == "configuration_") {
                opt_shard_count = 1;
            }
            if (name != "configuration_" && name != "node") {
                // cannot configure configuration or the default ns "node" it is what it is
                std::string real = undecorate(name);
                KeyValue kv("configuration"); // this will also be replicated
                auto sc = kv.get(real+".shards");
                if (!sc.empty())
                    conversion::to(sc, opt_shard_count);
                auto ordered = kv.get(real+".ordered");
                if (!ordered.empty())
                    opt_ordered_keys = ordered != "0";
                auto ranged = kv.get(real+".range_sharded");
                if (!ranged.empty())
                    opt_range_sharded = ranged != "0";
                auto foreign = kv.get(real+".foreign");
                if (!foreign.empty()) {
                    auto kind = lower_copy(foreign);
                    bool explicit_off = kind == "off" || kind == "none" || kind == "no" || kind == "false";
                    opt_foreign = parse_foreign_kind(foreign);
                    if (opt_foreign == foreign_kind::off && !explicit_off) {
                        barch::err({"unknown foreign source - ignoring it for space", name, foreign});
                    }
                }
                foreign_dsn = kv.get(real+".foreign_dsn");
                foreign_host = kv.get(real+".foreign_host");
                foreign_user = kv.get(real+".foreign_user");
                foreign_password = kv.get(real+".foreign_password");
                foreign_database = kv.get(real+".foreign_database");
                foreign_query = kv.get(real+".foreign_query");
                foreign_script = kv.get(real+".foreign_script");
                read_u64(kv, real+".foreign_port", foreign_port);
                read_u64(kv, real+".missing_ttl", missing_ttl);
                read_u64(kv, real+".foreign_timeout_ms", foreign_timeout_ms);
                read_u64(kv, real+".foreign_query_timeout_ms", foreign_query_timeout_ms);
                read_u64(kv, real+".foreign_max_inflight", foreign_max_inflight);
                read_u64(kv, real+".foreign_pool_size", foreign_pool_size);
                read_u64(kv, real+".foreign_pool_max_age_ms", foreign_pool_max_age_ms);
                read_u64(kv, real+".foreign_script_insns", foreign_script_insns);
                key_split = kv.get(real+".key_split");
                if (!key_split.empty()) {
                    try {
                        key_split_re = std::make_shared<std::regex>(
                            key_split, std::regex::ECMAScript | std::regex::optimize);
                    } catch (const std::regex_error& e) {
                        barch::err({"key_split is not a regex - ignoring it for space",
                                    name, key_split, e.what()});
                        key_split_re.reset();
                    }
                }
                if (opt_foreign == foreign_kind::mysql || opt_foreign == foreign_kind::postgres) {
                    if (foreign_dsn.empty() && foreign_host.empty()) {
                        barch::err({"foreign source needs a dsn or host - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (foreign_query.empty()) {
                        barch::err({"foreign source needs a query - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign::query_has_placeholder(foreign_query)) {
                        barch::err({"foreign query needs ? or $n or $$ - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (opt_foreign == foreign_kind::mysql
                               && !foreign::prepare_mysql(*this)) {
                        opt_foreign = foreign_kind::off;
                    } else if (opt_foreign == foreign_kind::postgres
                               && !foreign::prepare_postgres(*this)) {
                        opt_foreign = foreign_kind::off;
                    }
                } else if (opt_foreign == foreign_kind::luau) {
                    if (foreign_script.empty()) {
                        barch::err({"luau foreign source needs a script - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign::prepare_luau(*this)) {
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign_dsn.empty() || !foreign_host.empty()) {
                        std::string err;
                        auto resolved = foreign::resolve_dsn(*this, err);
                        auto look = resolved.empty() ? foreign_dsn : resolved;
                        auto dsn_looks_pg = look.find("postgres") != std::string::npos
                            || look.find("dbname=") != std::string::npos;
                        if (dsn_looks_pg)
                            foreign::prepare_postgres(*this);
                        else
                            foreign::prepare_mysql(*this);
                    }
                }
            }
            if (opt_range_sharded && !opt_ordered_keys) {
                // a range only means something where the keys are in order. Refused
                // rather than quietly ignored, so that reading the option back tells
                // the truth about what the space is doing
                barch::err({"range sharding needs ordered keys - ignoring it for space",
                            name});
                opt_range_sharded = false;
            }
            opt_shard_count = std::max<size_t>(opt_shard_count, 1);
            shards_out.resize(opt_shard_count);
            heap::allocator<barch::shard> alloc;
            auto start_time = std::chrono::high_resolution_clock::now();
            size_t shards_loaded = shard_thread_processor(shards_out.size(),[&](size_t shard_num) {
                shard_ptr& shard = shards_out[shard_num];
                shard = std::allocate_shared<barch::shard>(alloc,  name, 0, shard_num);
                shard->opt_ordered_keys = opt_ordered_keys;
                shard->load(true);
            });
            if (shards_out.size() != shards_loaded) {
                abort_with("shard loading threads invalid count");
            }
            statistics::shards = shards_out.size();
            auto end_time = std::chrono::high_resolution_clock::now();
            double millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            shards.swap(shards_out);
            barch::log({"Loaded",shards.size(),"shards in", millis/1000.0f, "s", shards_loaded});
            if (opt_range_sharded) {
                build_range_index();
            }
            // other threads allocate concurrently so only a growth is meaningful here
            uint64_t memory_after = get_total_memory();
            if (memory_after > memory_before) {
                add_startup_memory(memory_after - memory_before);
            }
        }
        start_maintain();
    }
    /**
     * The index is a function of the shards, so a load rebuilds it rather than reading
     * it back - there is no index file, nothing to write atomically and nothing to find
     * out of step with the data it describes.
     *
     * The rebuild only works if the shards are already an ordered partition, which they
     * are if this space was range sharded the last time it was written. If it was hash
     * sharded then every shard holds keys from all over the order, and routing by the
     * boundaries of that would lose most of them. That case is repartitioned: nearly
     * every key moves, which is why it happens once, at load, and says so in the log.
     */
    void key_space::build_range_index() {
        if (rindex.rebuild(shards)) {
            return;
        }
        uint64_t keys = 0;
        for (auto& s : shards) keys += s->get_tree_size();
        barch::log({"key space", name, "is range sharded but its", keys,
                    "keys are not in shard order - repartitioning"});
        auto start_time = std::chrono::high_resolution_clock::now();
        size_t moved = rindex.repartition(shards);
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();
        if (rindex.rebuild(shards)) {
            barch::log({"repartitioned", name, "moving", moved, "keys in",
                        (double) millis / 1000.0, "s"});
        } else {
            // the space still works - every route lands somewhere - but it is not the
            // ordered partition the option asked for, so say so rather than let the
            // ordered operations quietly return partial answers
            barch::err({"could not repartition key space", name,
                        "- range routing will not find every key"});
        }
    }

    void key_space::start_maintain() {
        exiting = false;
        tmaintain = std::thread([&]() -> void {

            try {
                auto tshards = this->get_shards();

                while (!this->thread_control.wait((int64_t)get_maintenance_poll_delay()*1000ll)) {
                   tshards = this->get_shards();
                   repl::distribute();
                    ++statistics::maintenance_cycles;

                   if (opt_range_sharded) {
                       // rebalancing lives here rather than on the insert path. Two
                       // reasons, and the second is the one that matters: an insert that
                       // has to move a block of keys is a latency spike a background
                       // sweep does not have, and putting every write to the table on
                       // one thread is what lets a router validate its route by simply
                       // re-reading the table under the lock it just took.
                       //
                       // A sweep is bounded by how many pairs of shard locks it may
                       // take, not by how much work is left, so it cannot hold up this
                       // thread; what it does not finish, the next one continues
                       try {
                           rindex.sweep(tshards, get_range_shard_budget(),
                                        get_range_shard_tolerance(), tshards.size() * 64);
                       } catch (std::exception& e) {
                           barch::err({"exception rebalancing range shards:", e.what()});
                       }
                   }

                   for (auto s : tshards) {
                       try {
                           s->maintenance();
                       }catch (std::exception& e) {
                           barch::err({"exception in maintenance:",e.what()});
                       }
                       if (exiting) break;
                   }

                   try {
                       drop_idle_sql();
                   } catch (std::exception& e) {
                       barch::err({"exception dropping idle sql:", e.what()});
                   }

                }
            }catch (std::exception& e){
               barch::err({"shard maintenance thread error:",e.what()});
            }
            thread_exit.signal(1);
        });
    }
    void key_space::fail_foreign_flights() {
        heap::vector<abstract_session_ptr> sessions;
        for (auto& sh : shards) {
            if (!sh) continue;
            // DROP used to hold these already; try_lock_for then fails at once
            // (EDEADLK / false) on a non-recursive mutex. Still fail the
            // flights: the space is going away either way.
            std::unique_lock lck(sh->get_latch(), std::defer_lock);
            if (!lck.try_lock_for(std::chrono::milliseconds(sh->lock_to_ms)))
                barch::warn({"foreign unload lock busy", name});
            auto* s = static_cast<shard*>(sh.get());
            s->fail_foreign("FOREIGN space unloaded", sessions);
            for (auto& [k, fl] : s->flights) {
                if (!fl->owns_inflight) continue;
                fl->owns_inflight = false;
                if (foreign_inflight > 0)
                    --foreign_inflight;
            }
        }
        // a space being unloaded wakes every waiter it has, and there is no key behind
        // this one - the block is being failed, not satisfied, so there is no turn to
        // pass on
        for (auto& sess : sessions)
            sess->do_block_continue(std::string());
    }

    key_space::~key_space() {
        exiting = true;
        thread_control.signal(1);
        thread_exit.wait();
        if (tmaintain.joinable())
            tmaintain.join();
        fail_foreign_flights();
        shards.clear();
    }

    shard_ptr key_space::get_local() {
        static std::atomic<uint64_t> sid;
        thread_local shard_ptr shard;
        if (!shard) {
            heap::allocator<key_space> alloc;
            ++sid;
            shard = std::allocate_shared<barch::shard>(alloc,  name + std::to_string(sid.load()), 0, 0);
            //shard->load(true);
        }
        return shard;
    }
    shard_ref key_space::get_ref(size_t shard) {
        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()].get();
        if (r == nullptr) {
            abort_with("shard not found");
        }
        return r;
    }


    std::shared_ptr<abstract_shard> key_space::get(size_t shard) {
        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()];
        if (r == nullptr) {
            abort_with("shard not found");
        }
        return r;
    }

    size_t key_space::get_shard_index(art::value_type key) {
        return get_shard_index(key.chars(), key.size);
    }
#if 0
    static uint64_t hash_fun(const char *str, size_t size) {
        uint64_t hash = 5381;
        int c;
        for (size_t s = 0; s < size; s++) {
            c = str[s];
            hash = ((hash << 5) + hash) + c; // hash * 33 + c
        }
        return hash;
    }
#else
    static uint64_t hash_fun(const char *str, size_t size) {
        //return ankerl::unordered_dense::detail::wyhash::hash(str, size);
        return a5hash(str,size,0);
    }
#endif
    size_t key_space::get_shard_index(const char* key, size_t key_len) {
        const size_t n = get_shard_count();
        if (n == 1) {
            return 0;
        }
        auto shard_key = art::value_type{key,key_len};

        if (opt_range_sharded) {
            // a binary search of at most shard_count boundaries, against a table that is
            // replaced rather than mutated, so this takes no lock and never sees a half
            // written one. It can still be overtaken by a rebalance - see route_moved
            return rindex.route(shard_key);
        }

        return hash_fun(shard_key.chars(), shard_key.size) % n;
    }

    bool key_space::is_stateful_sharding() const {
        // hash routing is a function of the key. range routing is not: the table
        // and which shard holds which key change while the space runs. SAVE,
        // LOAD, RELOAD and SAVEALL freeze the space when this is true. a later
        // method that can move a key returns true here. today only range does.
        return opt_range_sharded;
    }

    bool key_space::route_moved(art::value_type key, const shard_ptr& t) {
        if (!opt_range_sharded || !t) return false;
        return get_shard_index(key) != t->get_shard_number();
    }

    size_t key_space::get_shard_index(const std::string& key) {
        return get_shard_index(key.c_str(), key.size());
    }

    size_t key_space::get_shard_index(ValkeyModuleString **argv) {
        size_t nlen = 0;
        const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
        if (key_ok(n, nlen) != 0) {
            abort_with("invalid shard key");
        }
        return get_shard_index(n,nlen);
    }

    shard_ptr key_space::get(ValkeyModuleString **argv) {
        return get(get_shard_index(argv));
    }

    shard_ptr key_space::get(art::value_type key) {
        return get(get_shard_index(key.chars(), key.size));
    }
    shard_ref key_space::get_ref(art::value_type key) {
        return get_ref(get_shard_index(key.chars(), key.size));
    }
    shard_ref key_space::get_ref(ValkeyModuleString **argv) {
        return get_ref(get_shard_index(argv));
    }

    [[nodiscard]] std::string key_space::get_name() const {
        return name;
    };
    [[nodiscard]] std::string key_space::get_canonical_name() const {
        return undecorate(name);
    };

    const heap::vector<shard_ptr>& key_space::get_shards() {
        return shards;
    };
    void key_space::merge(merge_options options) {
        merge(source(), options);
    }
    void key_space::each_shard(std::function<void(shard_ptr)> f) {
        for (auto& s: shards) {
            f(s);
        }
    }
    size_t key_space::get_shard_count() const {
        return shards.size();
    }
    size_t key_space::hash_buf_size() const {
        return 0;
    }

    bool key_space::buffer_insert(const std::string &key, const std::string &value) {
        try {
            auto fc = [&](const art::node_ptr &) -> void {};
            auto k = encode_key(art::value_type{key});
            auto v = art::value_type{value};
            auto t = this->get(v);
            key_options spec;
            spec.set_hashed(!opt_ordered_keys);
            storage_release r(t);
            t->opt_insert(spec,k.get_value(),v,true,fc);
            return true;
        }catch (std::exception& ) {
            return false;
        }
    }

    void key_space::merge(key_space_ptr into, merge_options options) {
        if (!into) return;
        for (auto &d : shards) {
            auto sn = d->get_shard_number();
            d->merge(into->get(sn),options);
        }
    }
    void key_space::depends(const key_space_ptr& source) {
        this->src = source;
        auto current = source;
        while (current && current.get() != this) {
            current = current->source();
        }
        if (current && current.get() == this) {
            throw_exception<std::invalid_argument>("cannot have cyclic dependencies");
        }
        for (auto &d : shards) {
            auto sn = d->get_shard_number();
            d->depends(source ? source->get(sn) : nullptr);
        }

    }
    key_space_ptr key_space::source() const {
        return this->src;
    }
} // barch