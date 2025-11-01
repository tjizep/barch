//
// Created by teejip on 10/21/25.
//

#ifndef BARCH_ABSTRACT_SHARD_H
#define BARCH_ABSTRACT_SHARD_H
#include <memory>
#include <shared_mutex>
#include "art.h"
#include "key_options.h"

namespace barch {
    class abstract_shard : public std::enable_shared_from_this<abstract_shard>{
    public:
        typedef std::shared_ptr<abstract_shard> shard_ptr;
        bool opt_ordered_keys = get_ordered_keys();
        bool opt_all_keys_lru = false;
        bool opt_volatile_keys_lru = false;
        virtual ~abstract_shard() = default;
        virtual bool remove_leaf_from_uset(art::value_type key) = 0;
        virtual std::shared_mutex& get_latch() = 0;
        virtual void set_thread_ap() = 0;
        virtual bool publish(std::string host, int port) = 0;
        virtual uint64_t get_tree_size() const = 0;
        virtual uint64_t get_size() const = 0;
        virtual uint64_t get_hash_size() const = 0;
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
        virtual bool pull(std::string host, int port) = 0;

        virtual void run_defrag() = 0;

        virtual bool save(bool stats) = 0;

        virtual bool send(std::ostream& out) = 0;

        virtual bool load(bool stats) = 0;

        virtual bool retrieve(std::istream& in) = 0;

        virtual void begin() = 0;

        virtual void commit() = 0;

        virtual void rollback() = 0;

        virtual void clear() = 0;

        virtual bool insert(const art::key_options& options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool hash_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool tree_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool opt_rpc_insert(const art::key_options& options, art::value_type unfiltered_key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool opt_insert(const art::key_options& options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;

        virtual bool insert(art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) = 0;
        virtual bool insert(art::value_type key, art::value_type value, bool update) = 0;
        virtual bool evict(art::value_type key) = 0;
        virtual bool evict(const art::leaf* l) = 0;
        virtual bool remove(art::value_type key, const art::NodeResult &fc) = 0;
        // does not replicate
        virtual bool tree_remove(art::value_type key, const art::NodeResult &fc) = 0;
        virtual bool remove(art::value_type key) = 0;

        /**
         * find a key. if the key does not exist pull sources will be queried for the key
         * if the key is no-were a null is returned
         * @param key any valid value
         * @return not null key if it exists (incl. pull sources)
         */
        virtual art::node_ptr search(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::value_type key) = 0;
        virtual art::node_ptr lower_bound(art::trace_list &trace, art::value_type key) = 0;
        virtual void glob(const art::keys_spec &spec, art::value_type pattern, const std::function<bool(const art::leaf &)> &cb)  = 0;
        virtual shard_ptr sources() = 0;
        virtual void depends(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual void release(const std::shared_ptr<abstract_shard> & source) = 0;
        virtual art::node_ptr tree_minimum() const = 0;
        virtual art::node_ptr tree_maximum() const = 0;
        virtual art::node_ptr get_last_leaf_added() const = 0;
        virtual art::node_ptr make_leaf(art::value_type key, art::value_type v, art::leaf::ExpiryType ttl , bool is_volatile ) = 0;
        virtual art::value_type filter_key(art::value_type) const = 0;
        virtual art::node_ptr get_root() const = 0;
        virtual int range(art::value_type key, art::value_type key_end, art::CallBack cb, void *data) = 0;
        virtual int range(art::value_type key, art::value_type key_end, art::LeafCallBack cb) = 0;
        virtual bool update(art::value_type key, const std::function<art::node_ptr(const art::node_ptr &leaf)> &updater) = 0;
        virtual void queue_consume() = 0;
        virtual alloc_pair& get_ap() = 0;
        virtual const alloc_pair& get_ap() const = 0;
        virtual size_t get_shard_number() const = 0;
        virtual size_t get_queue_size() const = 0;
        virtual size_t inc_queue_size() = 0;
        virtual size_t dec_queue_size() = 0;
    };
    typedef abstract_shard::shard_ptr shard_ptr;
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
    barch::shard_ptr  t{};
    bool locked{false};
    storage_release() = delete;
    storage_release(const storage_release&) = delete;
    storage_release& operator=(const storage_release&) = default;
    explicit storage_release(const barch::shard_ptr& t, bool cons = true) : t(t) {
        if (cons)
            t->queue_consume();
        auto s = t->sources();
        // TODO: this may cause deadlock
        while (s) {
            s->get_latch().lock_shared();
            s = s->sources();
        }
        t->get_latch().lock();
    }
    ~storage_release() {
        t->get_latch().unlock();
        // TODO: this may cause deadlock we've got to at least test
        auto s = t->sources();
        while (s) {
            s->get_latch().unlock_shared();
            s = s->sources();
        }
    }
};
struct read_lock {
    barch::shard_ptr t{};
    bool locked{false};
    read_lock() = default;
    read_lock(const read_lock&) = delete;
    read_lock& operator=(const read_lock&) = delete;

    explicit read_lock(const barch::shard_ptr& t, bool consume = true) : t(t) {
        if (consume)
            t->queue_consume();
        auto s = t->sources();
        // TODO: this may cause deadlock
        while (s) {
            s->get_latch().lock_shared();
            s = s->sources();
        }
        t->get_latch().lock_shared();
    }

    ~read_lock() {
        t->get_latch().unlock_shared();
        // TODO: this may cause deadlock we've got to at least test
        auto s = t->sources();
        while (s) {
            s->get_latch().unlock_shared();
            s = s->sources();
        }
    }

};
/**
* evict a lru page
*/
uint64_t art_evict_lru(barch::shard_ptr t);

#endif //BARCH_ABSTRACT_SHARD_H