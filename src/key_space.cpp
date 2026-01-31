//
// Created by teejip on 10/22/25.
//

#include "key_space.h"
#include <thread>
#include <version.h>

#include "shard.h"
#include "keys.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "rpc/server.h"

namespace barch {
    struct key_spaces {
        key_spaces() {
            barch::std_log("Starting Barch",
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
                "\n\tcompression","[",get_compression_enabled(),"]","\n");

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
        {
            std::unique_lock l(ksp().lock);
            auto s = ksp().spaces.find(name);
            if (s != ksp().spaces.end()) {
                ksp().spaces.erase(s);
                r = true;
            }
        }
        return r; // destruction happens in callers thread - so hopefully no dl because shared ptr
    }

    key_space::key_space(const std::string &name) :name(name) {
        if (shards.empty()) {
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
            barch::std_log("Loaded",shards.size(),"shards in", millis/1000.0f, "s", shards_loaded);

        }
        start_maintain();
    }
    void key_space::start_maintain() {
        exiting = false;
        tmaintain = std::thread([&]() -> void {

            try {
                auto tshards = this->get_shards();

                while (!this->thread_control.wait((int64_t)get_maintenance_poll_delay()*1000ll)) {
                   tshards = this->get_shards();
                   repl::distribute();
                   for (auto s : tshards) {
                       try {
                           s->maintenance();
                       }catch (std::exception& e) {
                           barch::std_err("exception in maintenance:",e.what());
                       }
                       if (exiting) break;
                   }

                }
            }catch (std::exception& e){
               barch::std_err("shard maintenance thread error:",e.what());
            }
            thread_exit.signal(1);
        });
    }
    key_space::~key_space() {
        exiting = true;
        thread_control.signal(1);
        thread_exit.wait();
        if (tmaintain.joinable())
            tmaintain.join();
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

    std::shared_ptr<abstract_shard> key_space::get(size_t shard) {
        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()];
        if (r == nullptr) {
            abort_with("shard not found");
        }
        r->set_thread_ap();
        return r;
    }

    size_t key_space::get_shard_index(art::value_type key) {
        return get_shard_index(key.chars(), key.size);
    }

    size_t key_space::get_shard_index(const char* key, size_t key_len) {
        if (get_shard_count() == 1) {
            return 0;
        }
        auto shard_key = art::value_type{key,key_len};

        uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(shard_key.chars(), shard_key.size);

        size_t hshard = hash % get_shard_count();
        return hshard;
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
            auto k = conversion::as_composite(key);
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