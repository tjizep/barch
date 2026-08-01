//
// Created by teejip on 10/14/25.
//

#ifndef BARCH_SHARD_H
#define BARCH_SHARD_H
/**
 * a barch shard
 */
#include "art/art.h"
#include "abstract_shard.h"
#include "merge_options.h"
#include "overflow_hash.h"
#include "vector_stream.h"

namespace barch {
    using namespace art;
    struct query_pair {
        query_pair(abstract_leaf_pair * leaves) : leaves(leaves) {}//, key(key) , value_type key
        query_pair() = default;
        query_pair& operator=(const query_pair&) = default;
        query_pair(const query_pair&) = default;
        abstract_leaf_pair * leaves{};
        //value_type key{};
    };
    /**
     * the one hash function the set agrees on, over raw key bytes. both a stored
     * hashed_key (which has to fetch its bytes through a leaf) and a key_query
     * (which already holds them) must run this, or a lookup will land in the
     * wrong bucket.
     */
    inline size_t hash_key_bytes(value_type key) {
        return ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
    }

    /**
     * what the hash set stores: an address, and nothing else. the key bytes live
     * in the leaf at that address and are fetched on demand, which is what keeps
     * the index small relative to the data it indexes.
     */
    struct hashed_key {
        // we can reduce memory use by setting this to uint32_t
        // but max database size is reduced to 128 gb
        //uint64_t addr{};
        uint32_t addr{};
        node_ptr node(const abstract_leaf_pair* p) const ;
        hashed_key() = default;
        hashed_key(const hashed_key&) = default;
        hashed_key& operator=(const hashed_key&) = default;

        [[nodiscard]] const leaf* get_leaf(const query_pair& q) const;
        [[nodiscard]] value_type get_key(const query_pair& q) const;

        hashed_key(const node_ptr& la) ;
        hashed_key(const logical_address& la) ;

        hashed_key& operator=(const node_ptr& nl);



        [[nodiscard]] size_t hash(const query_pair& q) const {
            return hash_key_bytes(get_key(q));
        }
    };

    /**
     * what a lookup is made of. it is a distinct type from hashed_key on purpose:
     * a query is the caller's own bytes, is never stored in the set, and never
     * needs an address, so it never needs the logical allocator either. that is
     * what lets read only queries run concurrently under a shared lock - the same
     * property art::search has, where the search walks the tree carrying nothing
     * but the caller's key.
     *
     * the hash is taken once, up front, because the set will ask for it on every
     * probe and the bytes cannot move underneath it during a single lookup.
     *
     * it borrows the bytes, so it must not outlive them, and in particular must
     * not be held across a call that can re-enter the shard.
     */
    struct key_query {
        value_type key{};
        size_t h{};
        key_query() = default;
        key_query(const key_query&) = default;
        key_query& operator=(const key_query&) = default;
        explicit key_query(value_type k) : key(k), h(hash_key_bytes(k)) {}
    };

    struct hk_hash{
        hk_hash() = default;
        hk_hash& operator=(const hk_hash&) = default;
        hk_hash(const hk_hash&) = default;
        hk_hash(query_pair& q):q(&q){}
        query_pair* q{};
        using is_transparent = void; // hash and eq agree, so heterogeneous lookup is allowed
        // wyhash output is already avalanched, so tell ankerl not to re-mix it. this
        // belonged on the hash, not on hk_eq where it was declared and never read
        using is_avalanching = void;
        size_t operator()(const hashed_key& k) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
           return k.hash(*q);
        }
        size_t operator()(const key_query& k) const {
            return k.h;
        }

    };
    struct hk_eq{
        hk_eq() = default;
        hk_eq& operator=(const hk_eq&) = default;
        hk_eq(const hk_eq&) = default;
        hk_eq(query_pair& q):q(&q){}
        query_pair* q{};
        using is_transparent = void;
        bool operator()(const hashed_key& l,const hashed_key& r) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
            return l.get_key(*q) == r.get_key(*q);
        }
        // both orders, because ankerl compares (query, stored) and the open
        // addressed part of oh::unordered_set compares (stored, query)
        bool operator()(const hashed_key& l,const key_query& r) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
            return l.get_key(*q) == r.key;
        }
        bool operator()(const key_query& l,const hashed_key& r) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
            return l.key == r.get_key(*q);
        }
    };
    // there's a chance of false sharing so we align them on cache page boundaries alignas(con_alignment)
    struct shard : public abstract_shard, public art::tree{
    public:
    private:
        const std::string EXT = ".dat";
        bool with_stats{true};
        mutable query_pair qp{this};
        mutable hk_hash hk_h{qp};
        mutable hk_eq hk_e{qp};
        mutable oh::unordered_set<hashed_key,hk_hash, hk_eq> h{hk_e,hk_h};
        mutable uint64_t saf_keys_found{};
        mutable uint64_t saf_get_ops{};

        bool remove_from_unordered_set(value_type key);
        void write_extra(std::ostream& of) const ;
        void read_extra(std::istream& of);
        shard_ptr dependencies;
        uint64_t deletes{};
        uint64_t inserts{};
        uint64_t get_modifications() const ;
    public:
        void inc_keys_found() const {
            ++saf_get_ops;
            ++saf_keys_found;
        }
        void remove_leaf(const logical_address& at) override;
        size_t get_jump_size() const {
            return h.size();
        }


        bool transacted = false;
        // to support a transaction
        node_ptr save_root = nullptr;
        uint64_t save_size = 0;
        vector_stream save_stats{};
        std::shared_mutex save_load_mutex{};

        std::atomic<size_t> queue_size{};
        std::chrono::high_resolution_clock::time_point start_save_time {};
        uint64_t mods{};

        node_ptr get_root() const override {
            return root;
        }
        size_t get_queue_size() const override {
            return queue_size;
        }
        size_t inc_queue_size() override {
            return ++queue_size;
        };
        size_t dec_queue_size() override {
            return --queue_size;
        };

        void start_maintain();

        shard(const shard &) = delete;
        // standard constructor
        shard(const node_ptr &root, uint64_t size, size_t shard_number) :
        tree{"node", shard_number, root,size}{
            abstract_shard::opt_evict_all_keys_lru = get_evict_allkeys_lru();
            abstract_shard::opt_evict_volatile_keys_lru = get_evict_volatile_lru();
            barch::repl::clear_route(shard_number);
            start_maintain();

        }
        // name configurable
        shard(const std::string& name, uint64_t size, size_t shard_number) :
        tree{name, shard_number, root,size}{
            barch::repl::clear_route(shard_number);
            start_maintain();

        }
        // special constructor for auth - does not replicate
        shard(const std::string& name,const node_ptr &root, uint64_t size, size_t shard_number) :

        tree{name, shard_number, root,size},
        with_stats(false) {
            nodes.get_main().set_check_mem(false);
            leaves.get_main().set_check_mem(false);
            //repl_client.shard = shard_number;
            barch::repl::clear_route(shard_number);
            start_maintain();
        }
        shard& operator=(const shard&) = delete;

        ~shard() override;

        void load_hash();
        void clear_hash() ;
        bool remove_leaf_from_uset(value_type key) override;
        node_ptr from_unordered_set(value_type key) const;
        node_ptr first() const final ; // can return nullptr
        size_t page(size_t page, heap::vector<uint8_t>&)const final; // can return nullptr
        size_t next_page(size_t page ) const final; // can return nullptr
        node_ptr first(size_t start_page) const;
        bool publish(std::string host, int port) override;
        barch::latch_t& get_latch() override {
            return latch;
        }
        node_ptr make_leaf(value_type key, value_type v, key_options opts ) final;
        art::node_ptr make_leaf(value_type key, value_type v, leaf::ExpiryType ttl , bool is_volatile, bool is_compressed ) final;

        /**
         * register a pull source on this shard/tree
         * currently non-existing hosts will also be added (they can come online later)
         * but at a perf cost if keys are not found
         * keys can also be retrieved asynchronously becoming available later but at greater
         * throughput
         * @param host
         * @param port
         * @return true if host and port combo does not exist
         */

        bool pull(std::string host, int port) final;

        void run_defrag() final;

        bool save(bool stats) final;
        bool _save(bool stats) const;
        bool _load(bool stats);

        bool send(std::ostream& out) final;

        bool load(bool stats) final;
        bool reload() final;

        void load_bloom() final;

        bool retrieve(std::istream& in) final;

        void begin() final;

        void commit() final;

        void rollback() final;

        void clear() final;
        void _clear();

        bool insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc) final;

        bool hash_insert(const key_options &options, value_type key, value_type value, bool update, const NodeResult &fc) final;
        bool hash_erase(logical_address ad) final;
        bool tree_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) final;

        bool opt_rpc_insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) final;
        bool opt_insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc) final;

        bool insert(value_type key, value_type value, bool update, const NodeResult &fc) final;
        bool insert(value_type key, value_type value, bool update) final;
        bool evict(value_type key) final;
        bool evict(const leaf* l) final;
        bool remove(value_type key, const NodeResult &fc) final;
        bool tree_remove(value_type key, const NodeResult &fc) final;
        bool remove(value_type key) final;
        void merge(const shard_ptr& to, merge_options options) final;
        void merge(merge_options options) final;
        /**
         * find a key. if the key does not exist pull sources will be queried for the key
         * if the key is no-were a null is returned
         * @param key any valid value
         * @return not null key if it exists (incl. pull sources)
         */
        node_ptr search(value_type key) final;
        bool is_present(value_type key) final;
        art::node_ptr lower_bound(art::value_type key) final;
        art::node_ptr lower_bound(art::trace_list &trace, art::value_type key) final;
        shard_ptr sources() final;
        uint64_t bytes_in_free_list() final;
        void depends(const std::shared_ptr<abstract_shard> & source) final;
        void release(const std::shared_ptr<abstract_shard> & source) final;
        void glob(const keys_spec &spec, value_type pattern, bool value, const std::function<bool(const leaf &)> &cb)  final ;
        alloc_pair& get_ap() final {
            return *this;
        };
        const alloc_pair& get_ap() const final {
            return *this;
        };
        size_t get_shard_number() const final {
            return this->shard_number;
        }
        uint64_t get_tree_size() const final{
            uint64_t dep_size = 0; //dependencies ? dependencies->get_tree_size() : 0;
            return this->size + dep_size;
        }
        uint64_t get_size() const final {
            uint64_t src_size = 0;
            auto src = dependencies;
            if (src) {
                src_size += src->get_size(); // called recursively but no cycles
            }
            auto total = get_hash_size() + this->get_tree_size() + src_size;
            if (tomb_stones >total) {
                throw_exception<std::runtime_error>("invalid tombstone count");
            }
            return  total - this->tomb_stones;
        };
        uint64_t get_hash_size() const final{
            uint64_t dep_size = 0;//dependencies ? dependencies->get_hash_size() : 0;
            return h.size() + dep_size;
        };
        art::node_ptr tree_minimum() const final;
        art::node_ptr tree_maximum() const final;
        art::node_ptr get_last_leaf_added() const final {
            return last_leaf_added;
        };
        void maintenance() final;
        int range(art::value_type key, art::value_type key_end, CallBack cb, void *data) final;

        int range(art::value_type key, art::value_type key_end, LeafCallBack cb) final;

        bool update(value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater) final;

        void queue_consume() final;

        std::unordered_map<std::string, heap::vector<barch::abstract_session_ptr>> blocked_sessions;
        void add_rpc_blocks(const heap::vector<std::string>& keys, const barch::abstract_session_ptr& ptr) final {
            for (auto& k: keys) {
                add_rpc_block(k,ptr);;
            }
        }
        void unblock_key_(const std::string &k, const barch::abstract_session_ptr& ptr) {
            auto i = blocked_sessions.find(k);
            if (i != blocked_sessions.end()) {
                auto at = i->second.begin();
                for (auto& p : i->second) {
                    if (p == ptr) {
                        i->second.erase(at,at+1);
                    }
                    ++at;
                }
            }
        }
        void add_rpc_block(const std::string& key, const abstract_session_ptr& ptr) final{
            auto i = blocked_sessions.find(key);
            if (i != blocked_sessions.end()) {
                i->second.emplace_back(ptr);
            }else {
                blocked_sessions[key] = {ptr};
            }
        }

        void erase_rpc_blocks(const heap::vector<std::string>& keys, const barch::abstract_session_ptr& ptr) final {
            for (auto& k: keys) {
                unblock_key_(k, ptr);
            }
        }
        void erase_rpc_block(const std::string& key, const abstract_session_ptr& ptr) final {
            unblock_key_(key, ptr);
        }

        void call_unblock(const std::string& k) final {
            heap::vector<barch::abstract_session_ptr> sessions;
            auto i = blocked_sessions.find(k);
            if (i != blocked_sessions.end()) {
                sessions.swap(i->second);
                blocked_sessions.erase(i);
            }

            for (auto& s:sessions) {
                s->do_block_continue();
            }
        }


    };


}


/**
 * Returns the size of a shard.
 */
uint64_t shard_size(barch::shard *t);


#endif //BARCH_SHARD_H