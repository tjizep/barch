//
// Created by teejip on 10/22/25.
//

#ifndef BARCH_KEY_SPACE_H
#define BARCH_KEY_SPACE_H
#include <string>
#include "valkeymodule.h"
#include "abstract_shard.h"
#include "value_type.h"
namespace barch {
    class key_space {
    public:
        typedef std::shared_ptr<key_space> key_space_ptr;

    private:
        heap::vector<shard_ptr> shards{};
        decltype(std::chrono::high_resolution_clock::now) start_time;
        std::string name{};
        key_space_ptr src;
    public:
        key_space(const std::string &name);
        shard_ptr get(size_t shard);
        shard_ptr get(art::value_type key);
        shard_ptr get(ValkeyModuleString **argv) ;
        [[nodiscard]] std::string get_name() const;
        heap::vector<shard_ptr> get_shards();
        size_t get_shard_index(const char* key, size_t key_len);
        size_t get_shard_index(art::value_type key);
        size_t get_shard_index(const std::string& key);
        size_t get_shard_index(ValkeyModuleString **argv) ;
        void depends(const key_space_ptr& dependant);
        key_space_ptr source() const;
    };
    typedef key_space::key_space_ptr key_space_ptr;
    const std::string& get_ks_pattern_error();
    bool check_ks_name(const std::string& name_);
    std::string ks_undecorate(const std::string& name);
    key_space_ptr get_keyspace(const std::string &name);
    void all_shards(const std::function<void(const shard_ptr&)>& cb );
    bool flush_keyspace(const std::string& name);
    bool unload_keyspace(const std::string& name);
    void all_spaces(const std::function<void(const std::string& name, const barch::key_space_ptr&)>& cb );
} // barch

#endif //BARCH_KEY_SPACE_H