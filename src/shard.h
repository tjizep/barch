//
// Created by teejip on 10/14/25.
//

#ifndef BARCH_SHARD_H
#define BARCH_SHARD_H
/**
 * a barch shard
 */
#include "art.h"

namespace barch {
    using namespace art;
    struct query_pair {
        query_pair(abstract_leaf_pair * leaves, value_type key) : leaves(leaves), key(key) {}
        query_pair() = default;
        query_pair& operator=(const query_pair&) = default;
        query_pair(const query_pair&) = default;
        abstract_leaf_pair * leaves{};
        value_type key{};
    };
    struct hashed_key {
        // we can reduce memory use by setting this to uint32_t
        // but max database size is reduced to 128 gb
        //uint64_t addr{};
        uint32_t addr{};
        node_ptr node(const abstract_leaf_pair* p) const ;
        hashed_key() = default;
        hashed_key(const hashed_key&) = default;
        hashed_key& operator=(const hashed_key&) = default;
        hashed_key(value_type) ;

        [[nodiscard]] const leaf* get_leaf(const query_pair& q) const;
        [[nodiscard]] value_type get_key(const query_pair& q) const;

        hashed_key(const node_ptr& la) ;
        hashed_key(const logical_address& la) ;

        hashed_key& operator=(const node_ptr& nl);



        [[nodiscard]] size_t hash(const query_pair& q) const {
            auto key = get_key(q);
            size_t r = ankerl::unordered_dense::detail::wyhash::hash(key.chars(), key.size);
            return r;
        }
    };
    struct hk_hash{
        hk_hash() = default;
        hk_hash& operator=(const hk_hash&) = default;
        hk_hash(const hk_hash&) = default;
        hk_hash(query_pair& q):q(&q){}
        query_pair* q{};
        size_t operator()(const hashed_key& k) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
           return k.hash(*q);
        }
    };
    struct hk_eq{
        hk_eq() = default;
        hk_eq& operator=(const hk_eq&) = default;
        hk_eq(const hk_eq&) = default;
        hk_eq(query_pair& q):q(&q){}
        query_pair* q{};
        size_t operator()(const hashed_key& l,const hashed_key& r) const {
            if (q == nullptr) {
                abort_with("no query pair");
            }
            return l.get_key(*q) == r.get_key(*q);
        }
    };
    struct shard : public art::tree{
    public:
    private:

        bool with_stats{true};
        //mutable std::unordered_set<hashed_key,hk_hash,std::equal_to<hashed_key>,heap::allocator<hashed_key> > h{};
        //mutable heap::unordered_set<hashed_key,hk_hash > h{};
        mutable query_pair qp{this, {}};
        mutable hk_hash hk_h{qp};
        mutable hk_eq hk_e{qp};
        mutable oh::unordered_set<hashed_key,hk_hash, hk_eq> h{hk_e,hk_h};
        mutable uint64_t saf_keys_found{};
        mutable uint64_t saf_get_ops{};
        bool remove_from_unordered_set(value_type key);
    public:
        void inc_keys_found() const {
            ++saf_get_ops;
            ++saf_keys_found;
        }
        void set_hash_query_context(value_type q);
        void set_hash_query_context(value_type q) const ;
        void set_thread_ap();
        void remove_leaf(const logical_address& at) override;
        size_t get_jump_size() const {
            return h.size();
        }


        composite query{};
        composite cmd_ZADD_q1{};
        composite cmd_ZADD_qindex{};
        bool mexit = false;
        bool transacted = false;
        std::thread tmaintain{}; // a maintenance thread to perform defragmentation and eviction (if required)
        // to support a transaction
        node_ptr save_root = nullptr;
        uint64_t save_size = 0;
        vector_stream save_stats{};
        std::shared_mutex save_load_mutex{};


        barch::repl::client repl_client{};
        std::atomic<size_t> queue_size{};
        bool opt_ordered_keys{true};


        void start_maintain();

        shard(const shard &) = delete;
        // standard constructor
        shard(const node_ptr &root, uint64_t size, size_t shard_number) :
        tree{"node", shard_number, root,size},
        opt_ordered_keys(get_ordered_keys()) {
            opt_all_keys_lru = get_evict_allkeys_lru();
            opt_volatile_keys_lru = get_evict_volatile_lru();
            repl_client.shard = shard_number;
            barch::repl::clear_route(shard_number);
            start_maintain();

        }
        // special constructor for auth
        shard(const std::string& name,const node_ptr &root, uint64_t size, size_t shard_number) :

        tree{name, shard_number, root,size},
        with_stats(false) {
            nodes.get_main().set_check_mem(false);
            leaves.get_main().set_check_mem(false);
            repl_client.shard = shard_number;
            barch::repl::clear_route(shard_number);
            start_maintain();
        }
        shard& operator=(const shard&) = delete;

        virtual ~shard() override;

        void load_hash();
        void clear_hash() ;
        bool remove_leaf_from_uset(value_type key);
        node_ptr from_unordered_set(value_type key) const;

        bool publish(std::string host, int port);

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
        bool pull(std::string host, int port);

        void run_defrag();

        bool save(bool stats = true);

        bool send(std::ostream& out);

        bool load(bool stats = true);

        bool retrieve(std::istream& in);

        void begin();

        void commit();

        void rollback();

        void clear();

        bool insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc);

        bool hash_insert(const key_options &options, value_type key, value_type value, bool update, const NodeResult &fc);
        bool opt_rpc_insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc);
        bool opt_insert(const key_options& options, value_type key, value_type value, bool update, const NodeResult &fc);

        bool insert(value_type key, value_type value, bool update, const NodeResult &fc);
        bool insert(value_type key, value_type value, bool update = true);
        bool evict(value_type key);
        bool evict(const leaf* l);
        bool remove(value_type key, const NodeResult &fc);
        bool remove(value_type key);

        /**
         * find a key. if the key does not exist pull sources will be queried for the key
         * if the key is no-were a null is returned
         * @param key any valid value
         * @return not null key if it exists (incl. pull sources)
         */
        node_ptr search(value_type key);

        bool update(value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater);

        void queue_consume();

    };
    /**
 * gets per module per node type statistics for all art_node* types
 * @return art_statistics
 */
    art_statistics get_statistics();

    /**
     * get statistics for each operation performed
     */
    art_ops_statistics get_ops_statistics();

    /**
     * get replication and network statistics
     */
    art_repl_statistics get_repl_statistics();


}

struct storage_release {
    barch::shard * t{};
    bool locked{false};
    storage_release() = delete;
    storage_release(const storage_release&) = delete;
    storage_release& operator=(const storage_release&) = default;
    storage_release(barch::shard* t, bool cons = true) : t(t) {
        if (cons)
            t->queue_consume();
        t->latch.lock();
    }
    ~storage_release() {
        t->latch.unlock();
    }
};
struct read_lock {
    barch::shard * t{};
    bool locked{false};
    read_lock() = default;
    read_lock(const read_lock&) = delete;
    read_lock& operator=(const read_lock&) = delete;
    read_lock(barch::shard* t, bool consume = true) : t(t) {
        if (consume)
            t->queue_consume();
        t->latch.lock_shared();
    }
    ~read_lock() {
        t->latch.unlock_shared();
    }

};

/**
 * Returns the size of a shard.
 */
uint64_t shard_size(barch::shard *t);


#endif //BARCH_SHARD_H