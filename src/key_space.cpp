//
// Created by teejip on 10/22/25.
//

#include "key_space.h"
#include <thread>
#include "queue_server.h"
#include "shard.h"
#include "keys.h"

namespace barch {
    static std::mutex lock{};
    static std::string ks_pattern = "[0-9,A-Z,a-z,_]+";
    static std::string ks_pattern_error = "space name does not match the "+ks_pattern+" pattern";
    static std::regex name_check(ks_pattern);
    static heap::map<std::string, key_space_ptr> spaces{};
    static std::string decorate(const std::string& name_) {
        if (name_.empty())
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
        return ks_pattern_error;
    }

    void all_shards(const std::function<void(const barch::shard_ptr&)>& cb ) {
        std::unique_lock l(lock);
        for (auto &ks : spaces) {
            for (auto &shard_ : ks.second->get_shards()) {
                cb(shard_);
            }
        }
    }

    void all_spaces(const std::function<void(const std::string& name, const barch::key_space_ptr&)>& cb ) {
        std::unique_lock l(lock);
        for (auto &ks : spaces) {
            auto un = undecorate(ks.first);
            if (un.empty()) un = "(default)";
            cb(un, ks.second);
        }
    }

    bool check_ks_name(const std::string& name_) {
        auto name = decorate(name_);
        return std::regex_match(name, name_check);
    }
    bool is_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            return false;
        }
        std::unique_lock l(lock);
        std::string name = decorate(name_);
        auto s = spaces.find(name);
        return  (s != spaces.end());
    }
    key_space_ptr get_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            throw_exception<std::invalid_argument>(get_ks_pattern_error().c_str());
        }
        std::unique_lock l(lock);
        std::string name = decorate(name_);
        auto s = spaces.find(name);
        if (s != spaces.end()) {
            return s->second;
        }

        heap::allocator<key_space> alloc;
        // cannot create keyspace without memory
        auto ks = std::allocate_shared<key_space>(alloc, name);
        spaces[name] = ks;
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
            std::unique_lock l(lock);
            auto s = spaces.find(name);
            if (s != spaces.end()) {
                spaces.erase(s);
                r = true;
            }
        }
        return r; // destruction happens in callers thread - so hopefully no dl because shared ptr
    }

    key_space::key_space(const std::string &name) :name(name) {
        if (shards.empty()) {
            decltype(shards) shards_out;
            shards_out.resize(barch::get_shard_count().size());
            std::vector<std::thread> loaders{shards_out.size()};
            size_t shard_num = 0;
            auto start_time = std::chrono::high_resolution_clock::now();
            heap::allocator<barch::shard> alloc;
            for (auto &shard : shards_out) {
                loaders[shard_num] = std::thread([shard_num, &shard, &alloc, name]() {
                    shard = std::allocate_shared<barch::shard>(alloc,  name, 0, shard_num);
                    shard->load(true);
                });
                ++shard_num;
            }
            for (auto &loader : loaders) {
                if (loader.joinable())
                    loader.join();
            }
            statistics::shards = shards_out.size();
            auto end_time = std::chrono::high_resolution_clock::now();
            double millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            barch::std_log("Loaded",shards.size(),"shards in", millis/1000.0f, "s");
            start_queue_server() ;
            shards.swap(shards_out);
        }
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
        if (barch::get_shard_count().size() == 1) {
            return 0;
        }
        auto shard_key = art::value_type{key,key_len};

        uint64_t hash = ankerl::unordered_dense::detail::wyhash::hash(shard_key.chars(), shard_key.size);

        size_t hshard = hash % barch::get_shard_count().size();
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

    heap::vector<shard_ptr> key_space::get_shards() {
        return shards;
    };
    void key_space::merge() {
        merge(source());
    }
    void key_space::each_shard(std::function<void(shard_ptr)> f) {
        for (auto& s: shards) {
            f(s);
        }
    }
    void key_space::merge(key_space_ptr into) {
        if (!into) return;
        for (auto &d : shards) {
            auto sn = d->get_shard_number();
            d->merge(into->get(sn));
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