//
// Created by teejip on 10/14/25.
//

#include "shard.h"
#include "module.h"
#include <random>
#include <algorithm>

#include "dictionary_compressor.h"
#include "time_conversion.h"

static std::random_device rd;
static std::mt19937 gen(rd());


using namespace art;
void page_iterator(const heap::buffer<uint8_t> &page_data, unsigned size, std::function<void(const barch::leaf *, uint32_t pos)> cb);
#ifdef _TESTED_
uint64_t art_evict_lru(barch::shard_ptr t) {
    try {
        auto page = t->get_ap().get_leaves().get_lru_page();
        if (!page.second) return 0;
        auto i = page.first.begin();
        auto e = i + page.second;
        auto fc = [](art::node_ptr) -> void {
            ++statistics::keys_evicted;
        };
        while (i != e) {
            const leaf *l = (leaf *) i;
            if (l->key_len() > page.second) {
                abort_with("invalid key or key size");
            }
            if (l->deleted()) {
                i += (l->byte_size() + test_memory);
                continue;
            }
            t->remove(l->get_key(),fc);
            //art_delete(t, l->get_key(), fc);
            i += (l->byte_size() + test_memory);
        }
        ++statistics::pages_evicted;
        return page.second;
    } catch (std::exception &e) {
        barch::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}
#endif

art_statistics barch::get_statistics() {
    art_statistics as{};
    as.heap_bytes_allocated = (int64_t) heap::allocated;
    as.leaf_nodes = (int64_t) statistics::leaf_nodes;
    as.node4_nodes = (int64_t) statistics::n4_nodes;
    as.node16_nodes = (int64_t) statistics::n16_nodes;
    as.node256_nodes = (int64_t) statistics::n256_nodes;
    as.node256_occupants = as.node256_nodes ? ((int64_t) statistics::node256_occupants / as.node256_nodes) : 0ll;
    as.node48_nodes = (int64_t) statistics::n48_nodes;
    barch::all_shards( [&as](const shard_ptr &shard) {
        as.bytes_allocated += (int64_t) shard->get_ap().get_leaves().get_allocated() + shard->get_ap().get_nodes().get_allocated();
    });

    //statistics::addressable_bytes_alloc;
    barch::all_shards( [&as](const shard_ptr &shard) {
        as.bytes_interior += (int64_t)shard->get_ap().get_nodes().get_allocated();
    });
    as.value_bytes_compressed = (int64_t) statistics::value_bytes_compressed;
    as.vacuums_performed = (int64_t) statistics::vacuums_performed;
    as.last_vacuum_time = (int64_t) statistics::last_vacuum_time;
    as.leaf_nodes_replaced = (int64_t) statistics::leaf_nodes_replaced;
    as.pages_evicted = (int64_t) statistics::pages_evicted;
    as.keys_evicted = (int64_t) statistics::keys_evicted;
    as.pages_defragged = (int64_t) statistics::pages_defragged;
    as.exceptions_raised = (int64_t) statistics::exceptions_raised;
    as.maintenance_cycles = (int64_t) statistics::maintenance_cycles;
    as.shards = (int64_t) statistics::shards;
    as.local_calls = (int64_t) statistics::local_calls;
    as.local_calls = (int64_t) statistics::max_spin;
    as.logical_allocated = (int64_t) statistics::logical_allocated;
    as.oom_avoided_inserts = (int64_t) statistics::oom_avoided_inserts;
    as.keys_found = (int64_t) statistics::keys_found;
    as.new_keys_added = (int64_t) statistics::new_keys_added;
    as.keys_replaced = (int64_t) statistics::keys_replaced;

    return as;
}


struct transaction {
    bool was_transacted = false;
    barch::shard *t = nullptr;
    transaction(const transaction&) = default;
    transaction& operator=(const transaction&) = default;
    explicit transaction(barch::shard *t) : t(t) {
        was_transacted = t->transacted;
        if (!was_transacted)
            t->begin();
    }

    ~transaction() {
        if (!was_transacted)
            t->commit();

    }
};

art_ops_statistics barch::get_ops_statistics() {
    art_ops_statistics os{};
    os.delete_ops = (int64_t) statistics::delete_ops;
    os.get_ops = (int64_t) statistics::get_ops;
    os.insert_ops = (int64_t) statistics::insert_ops;
    os.iter_ops = (int64_t) statistics::iter_ops;
    os.iter_range_ops = (int64_t) statistics::iter_range_ops;
    os.lb_ops = (int64_t) statistics::lb_ops;
    os.max_ops = (int64_t) statistics::max_ops;
    os.min_ops = (int64_t) statistics::min_ops;
    os.range_ops = (int64_t) statistics::range_ops;
    os.set_ops = (int64_t) statistics::set_ops;
    os.size_ops = (int64_t) statistics::size_ops;
    return os;
}
art_repl_statistics barch::get_repl_statistics(){
    art_repl_statistics rs;
    rs.bytes_recv = (int64_t) statistics::repl::bytes_recv;
    rs.bytes_sent = (int64_t) statistics::repl::bytes_sent;
    rs.insert_requests = (int64_t) statistics::repl::insert_requests;
    rs.remove_requests = (int64_t) statistics::repl::remove_requests;
    rs.find_requests = (int64_t) statistics::repl::find_requests;
    rs.request_errors = (int64_t) statistics::repl::request_errors;
    rs.redis_sessions = (int64_t) statistics::repl::redis_sessions;
    rs.attempted_routes = (int64_t) statistics::repl::attempted_routes;
    rs.routes_succeeded = (int64_t) statistics::repl::routes_succeeded;
    rs.instructions_failed = (int64_t) statistics::repl::instructions_failed;
    rs.key_add_recv = (int64_t) statistics::repl::key_add_recv;
    rs.key_add_recv_applied = (int64_t) statistics::repl::key_add_recv_applied;
    rs.key_rem_recv = (int64_t) statistics::repl::key_rem_recv;
    rs.out_queue_size = (int64_t) statistics::repl::out_queue_size;
    rs.key_rem_recv_applied = (int64_t) statistics::repl::key_rem_recv_applied;
    rs.routes_succeeded = (int64_t) statistics::repl::routes_succeeded;
    rs.attempted_routes = (int64_t) statistics::repl::attempted_routes;
    return rs;
}
#include "ioutil.h"

template<typename OutStream>
static void stats_to_stream(OutStream &of) {
    writep(of, statistics::n4_nodes);
    writep(of, statistics::n16_nodes);
    writep(of, statistics::n48_nodes);
    writep(of, statistics::n256_nodes);
    writep(of, statistics::node256_occupants);
    writep(of, statistics::leaf_nodes);
    writep(of, statistics::value_bytes_compressed);
    int64_t empty = 0;
    writep(of, empty);

    writep(of, empty);
    writep(of, empty);
    writep(of, empty);
    writep(of, statistics::oom_avoided_inserts);
    writep(of, statistics::logical_allocated);

    if (!of.good()) {
        throw std::runtime_error("art::stats_to_stream: bad output stream");
    }
}

template<typename InStream>
static void stream_to_stats(InStream &in) {
    if (!in.good()) {
        throw std::runtime_error("art::stream_to_stats: bad output stream");
    }
    readp(in, statistics::n4_nodes);
    readp(in, statistics::n16_nodes);
    readp(in, statistics::n48_nodes);
    readp(in, statistics::n256_nodes);
    readp(in, statistics::node256_occupants);
    readp(in, statistics::leaf_nodes);
    readp(in, statistics::value_bytes_compressed);
    int64_t empty = 0;
    readp(in, empty);

    readp(in, empty);
    readp(in, empty);
    readp(in, empty);
    readp(in, statistics::oom_avoided_inserts);
    readp(in, statistics::logical_allocated);
}


barch::hashed_key::hashed_key(const node_ptr& la) {
    if (la.logical.address() > std::numeric_limits<uint32_t>::max()) {
        throw_exception<std::runtime_error>("hashed_key: address too large/out of memory");
    }
    addr = la.logical.address();
}
art::node_ptr barch::hashed_key::node(const abstract_leaf_pair* p) const {
    return logical_address{addr, (abstract_leaf_pair*)p};
}
barch::hashed_key& barch::hashed_key::operator=(const node_ptr& nl) {
    addr = nl.logical.address();
    return *this;
}

barch::hashed_key::hashed_key(const logical_address& la) {
    if (la.address() > std::numeric_limits<uint32_t>::max()) {
        throw_exception<std::runtime_error>("hashed_key: address too large/out of memory");
    }
    addr = la.address();

}

barch::hashed_key::hashed_key(value_type) {
}

const barch::leaf* barch::hashed_key::get_leaf(const query_pair& q) const {
    node_ptr n = logical_address{addr, q.leaves};
    return n.is_leaf ? n.const_leaf() : nullptr;
}
thread_local value_type query_key;
value_type barch::hashed_key::get_key(const query_pair& q) const {
    if (!addr) {
        //throw_exception<std::invalid_argument>("the address for the query is empty");
        return query_key;
    }
    return get_leaf(q)->get_key();
}

void barch::shard::clear_hash() {
    h.clear();
}

void barch::shard::set_hash_query_context(value_type k) {
    query_key = k;
}
void barch::shard::set_hash_query_context(value_type k) const {
    query_key = k;
}
void barch::shard::set_thread_ap() {
}

void barch::shard::remove_leaf(const logical_address& )  {
}
bool barch::shard::remove_leaf_from_uset(value_type key) {
    set_hash_query_context(key);
    auto i = h.find(key);
    if (i != h.end()) {
        node_ptr old{logical_address(i->addr,this)};
        h.erase(i);
        if (old.cl()->is_hashed()) {
            old.free_from_storage();
        }
        return true;
    }
    return false;
}

art::node_ptr barch::shard::from_unordered_set(value_type key) const {
    set_hash_query_context(key);
    auto i = h.find(key);
    if (i != h.end()) {

        inc_keys_found();
        return i->node(this);
    }
    return nullptr;
}

bool barch::shard::remove_from_unordered_set(value_type key) {
    set_hash_query_context(key);
    return h.erase(key) > 0;
}


bool barch::shard::publish(std::string host, int port) {
    repl_client.add_destination(std::move(host), port);
    return true;
}
bool barch::shard::pull(std::string host, int port) {
    repl_client.add_source(std::move(host), port);
    return true;
}
void barch::shard::read_extra(std::istream &in) {
    uint32_t extra = 0;
    readp(in, extra);
    if (extra > 0) {
        uint8_t ordered = 0;
        readp(in, ordered );
        opt_ordered_keys = ordered != 0;
        --extra;
    }
    // to keep backwards compatibility between shards
    while (extra > 0) {
        uint8_t x;
        readp(in, x); // bytes from some future version
    }
}
void barch::shard::write_extra(std::ostream &of) {
    uint32_t extra = 1;
    writep(of, extra);
    uint8_t ordered = opt_ordered_keys ? 1 : 0;
    writep(of, ordered);
    // in future we can extend with more options here
}


bool barch::shard::save(bool stats) {
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    auto *t = this;
    if (nodes.get_main().get_bytes_allocated()==0) return true;
    bool saved = false;
    node_ptr troot;
    size_t tsize;
    auto save_stats_and_root = [&](std::ostream &of) {
        if (!saved) {
            abort_with("synch error");
        }
        uint32_t w_stats = 0;
        if (stats) {
            w_stats = 1;
        }
        writep(of, w_stats);
        if (w_stats == 1) {
            stats_to_stream(of);
        }
        auto root = logical_address(troot.logical);
        writep(of, root);
        writep(of, troot.is_leaf);
        writep(of, tsize);
        write_extra(of);

    };

    auto st = std::chrono::high_resolution_clock::now();
    //transaction tx(this); // stabilize main while saving
    //arena::hash_arena leaves{get_leaves().get_name()};
    //arena::hash_arena nodes{get_nodes().get_name()};
    {
        write_lock release(this->latch); // only lock during partial copy
        tsize = t->size;
        troot = t->root;
        saved = true;
        //leaves.borrow(get_leaves().get_main());
        //nodes.borrow(get_nodes().get_main());
        if (!get_leaves().self_save_extra(EXT, save_stats_and_root)) {
            return false;
        }

        if (!get_nodes().self_save_extra( EXT, [&](std::ostream &) {
        })) {
            return false;
        }
    }

    auto current = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(current - st);
    const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(current - st);
    if (log_saving_messages == 1)
        std_log("saved barch db:", t->size, "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds");
    return true;
}
bool barch::shard::send(std::ostream& unused(out)) {
#ifdef _TEST_COVERED_
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    auto *t = this;
    if (nodes.get_main().get_bytes_allocated()==0) return true;
    bool saved = false;
    node_ptr troot;
    size_t tsize;
    auto save_stats_and_root = [&](std::ostream &of) {
        if (!saved) {
            abort_with("synch error");
        }
        stats_to_stream(of);
        auto root = logical_address(troot.logical);
        writep(of, root);
        writep(of, troot.is_leaf);
        writep(of, tsize);
        write_extra(of);
    };

    auto st = std::chrono::high_resolution_clock::now();
    transaction tx(this); // stabilize main while saving
    arena::hash_arena leaves{get_leaves().get_name()};
    arena::hash_arena nodes{get_nodes().get_name()};
    {
        storage_release release(this->shared_from_this()); // only lock during partial copy
        tsize = t->size;
        troot = t->root;
        saved = true;
        leaves.borrow(get_leaves().get_main());
        nodes.borrow(get_nodes().get_main());
    }
    if (!get_leaves().send_extra(leaves,out, save_stats_and_root)) {
        return false;
    }


    if (!get_nodes().send_extra(nodes, out, [&](std::ostream &) {
    })) {
        return false;
    }

    auto current = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(current - st);
    const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(current - st);

    std_log("sent barch db:", t->size, "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds");
#endif

    return true;
}

bool barch::shard::load(bool) {

    //
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    try {
        write_lock release(this->latch);
        h.clear();
        auto *t = this;
        logical_address root{nullptr};
        bool is_leaf = false;
        // save stats in the leaf storage
        auto load_stats_and_root = [&](std::istream &in) {
            uint32_t w_stats = 0;
            readp(in, w_stats);
            if (w_stats != 0) {
                stream_to_stats(in);
            }
            readp(in, root);
            readp(in, is_leaf);
            readp(in, t->size);
            read_extra(in);

        };
        auto st = std::chrono::high_resolution_clock::now();

        if (!get_nodes().load_extra(EXT, [&](std::istream &) {
        })) {
            return false;
        }
        if (!get_leaves().load_extra(EXT, load_stats_and_root)) {
            return false;
        }
        root = logical_address{root.address(), this};// translate root to the now
        if (is_leaf) {

            t->root = node_ptr{root};
        } else {
            t->root = resolve_read_node(root);
        }
        page_modifications::inc_all_tickers();
        load_hash();
        auto now = std::chrono::high_resolution_clock::now();
        const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(now - st);
        const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(now - st);

        if (log_loading_messages == 1) {
            std_log("Done loading BARCH Shard, keys loaded:", t->size + h.size(), "index mode: [",opt_ordered_keys?"ordered":"unordered","]");

            std_log("loaded barch db in", d.count(), "millis or", (double) dm.count() / 1000000, "seconds");
            std_log("db memory when created", (double) get_total_memory() / (1024 * 1024), "Mb");
        }
    }catch (std::exception &e) {
        std_log("could not load",e.what());
        return false;
    }
    return true;
}
bool barch::shard::retrieve(std::istream& unused(in)) {

#ifdef _TEST_COVERED_
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    try {
        storage_release release(this->shared_from_this());
        auto *t = this;
        logical_address root{nullptr};
        bool is_leaf = false;
        // save stats in the leaf storage
        auto load_stats_and_root = [&](std::istream &in) {
            uint32_t w_stats = 0;
            readp(in, w_stats);
            if (w_stats != 0) {
                stream_to_stats(in);
            }

            readp(in, root);
            readp(in, is_leaf);
            readp(in, t->size);
            read_extra(in);
        };
        auto st = std::chrono::high_resolution_clock::now();

        if (!get_leaves().receive_extra(in, load_stats_and_root)) {
            return false;
        }

        if (!get_nodes().receive_extra(in, [&](std::istream &) {
        })) {
            return false;
        }

        root = logical_address{root.address(), this};// translate root to the now
        if (is_leaf) {

            t->root = node_ptr{root};
        } else {
            t->root = resolve_read_node(root);
        }
        page_modifications::inc_all_tickers();
        load_hash();
        auto now = std::chrono::high_resolution_clock::now();
        const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(now - st);
        const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(now - st);
        std_log("Done loading BARCH, keys loaded:", t->size, "");

        std_log("loaded barch db in", d.count(), "millis or", (float) dm.count() / 1000000, "seconds");
        std_log("db memory when created", (float) get_total_memory() / (1024 * 1024), "Mb");
    }catch (std::exception &e) {
        std_log("could not load",e.what());
        return false;
    }
#endif

    return true;
}

void barch::shard::begin() {
    if (transacted) return;
    save_root = root;
    save_size = size;
    save_stats.clear();
    stats_to_stream(save_stats);
    {
       storage_release release(this->shared_from_this());
        get_leaves().begin();
        get_nodes().begin();

    }
    transacted = true;
}

void barch::shard::commit() {
    if (!transacted) return;
    storage_release release(this->shared_from_this());
    get_leaves().commit();
    get_nodes().commit();
    transacted = false;
}

void barch::shard::rollback() {
    if (!transacted) return;
    storage_release release(this->shared_from_this());
    get_leaves().rollback();
    get_nodes().rollback();
    root = save_root;
    size = save_size;
    save_stats.seek(0);
    stream_to_stats(save_stats);
    transacted = false;
}
void barch::shard::load_bloom() {
    if (!has_static_bloom_filter()) return;
    auto &lc = get_leaves();
    lc.iterate_pages([this](size_t s, size_t unused(page), auto& data) {
        page_iterator(data, s, [this](const leaf *l, uint32_t unused(pos)) {
            if (l->deleted() || l->is_tomb()) return;
            add_bloom(l->get_key());
        });
    });

}
void barch::shard::clear() {
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    storage_release release(this->shared_from_this());
    root = {nullptr};
    size = 0;
    transacted = false;
    tomb_stones = 0;
    shard::blocked_sessions.clear();
    shard::mods = 0;
    shard::saf_get_ops = 0;
    shard::saf_keys_found = 0;
    shard::queue_size = 0;
    create_bloom(has_static_bloom_filter()); // resets the bloom
    get_leaves().clear();
    get_nodes().clear();
    h.clear();
    statistics::n4_nodes = 0;
    statistics::n16_nodes = 0;
    statistics::n48_nodes = 0;
    statistics::n256_nodes = 0;
    statistics::node256_occupants = 0;
    statistics::leaf_nodes = 0;
    statistics::value_bytes_compressed = 0;
    statistics::oom_avoided_inserts = 0;
    statistics::keys_found = 0;
    statistics::new_keys_added = 0;
    statistics::keys_replaced = 0;
    statistics::logical_allocated = 0;
}

bool barch::shard::insert(value_type key, value_type value, bool update, const NodeResult &fc) {
    return this->insert({}, key, value, update, fc);
}
bool barch::shard::insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    if (get_total_memory() > get_max_module_memory()) {
        // do not add data if memory limit is reached
        ++statistics::oom_avoided_inserts;
        return false;
    }
    value_type key = filter_key(unfiltered_key);
    auto before = this->get_size();
    if (opt_ordered_keys) {
        tree_insert(options, key, value, update, fc);
    }else {
        hash_insert(options, key, value, update, fc);
    }
    this->repl_client.insert(latch, options, key, value);
    return this->get_size() > before;
}
bool barch::shard::tree_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) {
    ++inserts;
    return art_insert(this, options, key, value, update, fc);
}
bool barch::shard::hash_insert(const key_options &options, value_type key, value_type value, bool update, const NodeResult &fc) {
    if (get_total_memory() > get_max_module_memory()) {
        // do not add data if memory limit is reached
        ++statistics::oom_avoided_inserts;
        return false;
    }
    ++inserts;
    ++statistics::insert_ops;
    set_hash_query_context(key);
    auto i = h.find(key);
    if (i != h.end()) {
        if (update) {
            auto n = i->node(this);
            leaf *dl = n.l();
            fc(n);
            if (art::is_leaf_direct_replacement(dl, value, options))
            {

                dl->set_value(value);
                dl->set_expiry(options.is_keep_ttl() ? dl->expiry_ms() : options.get_expiry());
                options.is_volatile() ? dl->set_volatile() : dl->unset_volatile();
                last_leaf_added = n;
                ++statistics::keys_replaced;
                return false;
            }
            node_ptr old = logical_address{i->addr,this};
            h.erase(i);
            old.free_from_storage();
            ++statistics::keys_replaced;
        }else {
            return false;
        }
    }else {
        ++statistics::new_keys_added;
    }
    node_ptr l = this->make_leaf(key, value, options);
    l.l()->set_hashed();
    h.insert_unique(l);
    return true;
}

bool barch::shard::opt_rpc_insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    if (get_total_memory() > get_max_module_memory()) {
        // do not add data if memory limit is reached
        ++statistics::oom_avoided_inserts;
        return false;
    }

    std::string tk;
    value_type key = s_filter_key(tk,unfiltered_key);
    add_bloom(key);

    size_t before = size;
    if (options.is_hashed()) {
        hash_insert(options, key, value, update, fc);
    }else {
        art_insert(this, options, key, value, update, fc);
    }
    return size+h.size() > before;
}


bool barch::shard::opt_insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    std::string tk;
    value_type key = s_filter_key(tk,unfiltered_key);
    if (opt_rpc_insert(options, key, value, update, fc)) {
        this->repl_client.insert(latch, options, key, value);
        return true;
    }
    return false;
}

bool barch::shard::insert(value_type key, value_type value, bool update) {
    return this->insert(key, value, update, [](const node_ptr &) {}) ;
}
bool barch::shard::update(value_type unfiltered_key, const std::function<node_ptr(const node_ptr &leaf)> &updater) {
    auto key = filter_key(unfiltered_key);
    auto repl_updateresult = [&](const node_ptr &leaf) {
        auto value = updater(leaf);
        if (value.null()) {
            return value;
        }
        auto l = value.const_leaf();
        key_options options = *l;
        this->repl_client.insert(latch, options, key, l->get_value());
        return value;
    };
    auto i = h.find(key);
    if (i != h.end()) {

        node_ptr old = logical_address{i->addr,this};
        if (old.l()->is_tomb()) {
            old.l()->unset_tomb();
            if (tomb_stones == 0) {
                throw_exception<std::runtime_error>("invalid tombstone count");
            }
            --tomb_stones;
        }
        bool hashed = old.cl()->is_hashed();
        if (!hashed)
            abort_with("no art caching allowed");
        node_ptr n = repl_updateresult(old);

        if (n == old) {

            return false; // nothing to do
        }
        if (!n.null()) {
            n.l()->set_hashed();
            h.erase(i);
            h.insert(n);
            old.free_from_storage();// ok if old is null - nothing will happen
        }
        return !n.null();
    }
    if (!opt_ordered_keys) {
        return false;
    }

    return barch::update(this, key, repl_updateresult);
}
bool barch::shard::evict(const leaf* l) {
    if (l->deleted()) return false;
    size_t before = size;
    if (l->is_hashed()) {
        set_hash_query_context(l->get_key());
        auto i = h.find(l->get_key());
        if (i != h.end()) { // we don't need to de-count delete ops here
            auto n = i->node(this);
            erase_tomb(n.l());
            h.erase(i);
            n.free_from_storage();
            ++statistics::keys_evicted;
            return true;
        }
        // else ...

        return false;
    }
    art_delete(this, l->get_key(), [](const art::node_ptr &){});
    if (size < before) {
        ++statistics::keys_evicted;
    }
    --statistics::delete_ops; // were not counting these deletes
    return size < before;
}
bool barch::shard::evict(value_type unfiltered_key) {
    size_t before = size;

    auto key = filter_key(unfiltered_key);
    node_ptr old = from_unordered_set(key);
    if (!old.null()) {
        auto n = old;
        leaf *dl = n.l();
        if (dl->is_hashed()) {
            erase_tomb(dl);
            h.erase(key);
            n.free_from_storage();
            return true;
        }
    }
    --statistics::delete_ops; // were not counting these deletes
    art_delete(this, key, [](const art::node_ptr &){});
    return size < before;

}
bool barch::shard::tree_remove(value_type key, const NodeResult &fc) {
    auto sbef = size;
    ++deletes;
    art_delete(this, key, fc);
    return sbef < size;
}


bool barch::shard::remove(value_type unfiltered_key, const NodeResult &fc) {
    ++deletes;
    size_t before = size;
    auto key = filter_key(unfiltered_key);
    node_ptr old = from_unordered_set(key);
    if (!old.null()) {
        if (dependencies) {
            // check if exists and insert tombstone else continue with normal erase
            auto dep = dependencies->search(key);
            if (!dep.null()) {
                fc(old);
                bool r = this->hash_insert({},key,{},true,[](node_ptr){});
                if (r) {
                    last_leaf_added.l()->set_tomb();
                    ++tomb_stones;
                    return true;
                }
                return false;
            }
        }
        auto n = old;
        leaf *dl = n.l();
        if (dl->is_hashed()) {
            fc(n);
            erase_tomb(dl);
            h.erase(key);
            n.free_from_storage();

            this->repl_client.remove(latch, key);
            return true;
        }
    }
    if (dependencies) {
        // check if exists and insert tombstone else continue with normal erase
        auto dep = dependencies->search(key);
        if (!dep.null()) {
            fc(dep);
            ++tomb_stones;
            tree_insert({},key,{},true,[](node_ptr){});
            if (!last_leaf_added.null())
                last_leaf_added.l()->set_tomb();

            return true;
        }
    } // else continue

    art_delete(this, key, fc);
    this->repl_client.remove(latch, key);
    return size < before;
}

int barch::shard::range(art::value_type key, art::value_type key_end, CallBack cb, void *data) {
    return art::range(this, key, key_end, cb, data);
}

int barch::shard::range(art::value_type unused(key), art::value_type unused(key_end), LeafCallBack unused(cb)) {
    //return art::range(this, key, key_end, cb);
    return -1;
}
art::value_type barch::shard::filter_key(value_type key) const {
    return tree_filter_key(key);
}
node_ptr barch::shard::make_leaf(value_type key, value_type v, key_options opts ) {
    return tree_make_leaf(key, v, opts);
}
node_ptr barch::shard::make_leaf(value_type key, value_type v, leaf::ExpiryType ttl , bool is_volatile, bool is_compressed ) {
    return tree_make_leaf(key, v, ttl, is_volatile, is_compressed);
}

bool barch::shard::remove(value_type key) {

    return this->remove(key, [](const node_ptr &) {});
}
barch::shard_ptr barch::shard::sources() {
    return dependencies;
}
void barch::shard::depends(const std::shared_ptr<abstract_shard> & source) {

    dependencies = source;
    auto current = this->shared_from_this();
    auto test = dependencies;
    while (test && test != current) {
        test = test->sources();
    }
    if (test == current) {
        dependencies = nullptr;
        throw_exception<std::invalid_argument>("cannot have cyclic dependencies");
    }
}

void barch::shard::release(const std::shared_ptr<abstract_shard> & unused(source)) {
    dependencies = nullptr;
}

art::node_ptr barch::shard::lower_bound(art::value_type key) {
    return art::lower_bound(this, key);
}

art::node_ptr barch::shard::lower_bound(art::trace_list &trace, art::value_type key) {
    return art::lower_bound(trace, this, key);
}

void barch::shard::glob(const keys_spec &spec, value_type pattern, bool value, const std::function<bool(const leaf &)> &cb)  {
    art::glob(this, spec, pattern, value, cb);
}

art::node_ptr barch::shard::search(value_type unfiltered_key) {
    value_type key = this->filter_key(unfiltered_key);

    if (!opt_ordered_keys) {
        auto n = from_unordered_set(key);
        // TODO: check tomb stone flag for dependant deletes
        if (!n.null()) {
            if (dependencies && n.cl()->is_tomb()) {
                return nullptr;
            }
        }
        return n;
    }
    auto r = art_search(this, key);
    if (r.null()) {
        if (dependencies) {
            r = dependencies->search(key); // this can recurse down
            if (!r.null()) {
                return r;
            }
        }
        last_leaf_added = nullptr; // clear it before trying to retrieve
        this->repl_client.find_insert(key);
        return this->last_leaf_added;
    }
    if (r.cl()->is_tomb()) {
        return nullptr;
    }
    // check if r.cl()->is_tombstone() and return nullptr
    return r;
}
art::node_ptr barch::shard::tree_minimum() const {
    auto dmin = dependencies ? dependencies->tree_minimum() : nullptr;
    auto tmin = art_minimum(this);
    if (dmin.is_leaf && tmin.is_leaf) {
        if (dmin.cl()->get_key() < tmin.cl()->get_key()) {
            return dmin;
        }
        return tmin;
    }
    if (dmin.is_leaf) return dmin;
    return tmin;

}
art::node_ptr barch::shard::tree_maximum() const {
    auto dmax = dependencies ? dependencies->tree_maximum() : nullptr;
    auto tmax = art::maximum(this);
    if (dmax.is_leaf && tmax.is_leaf) {
        if (dmax.cl()->get_key() < tmax.cl()->get_key()) {
            return dmax;
        }
        return tmax;
    }
    if (dmax.is_leaf) {
        return dmax;
    }
    return tmax;
}
void barch::shard::queue_consume() {
}

/**
 * just return the size
 */
uint64_t shard_size(barch::shard *s) {
    ++statistics::size_ops;
    try {
        if (s == nullptr)
            return 0;
        return s->get_size();
    } catch (std::exception &e) {
        barch::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}

barch::shard::~shard() {
    repl_client.stop();
    shard::blocked_sessions.clear();
    if (opt_drop_on_release) {
        this->get_leaves().delete_files(EXT);
        this->get_nodes().delete_files(EXT);
    }

}

#include "configuration.h"
#include <functional>

void page_iterator(const heap::buffer<uint8_t> &page_data, unsigned size, std::function<void(const barch::leaf *, uint32_t pos)> cb) {
    if (!size) return;

    auto e = page_data.begin() + size;
    size_t deleted = 0;
    size_t pos = 0;
    for (auto i = page_data.begin(); i < e;) {
        const barch::leaf *l = (barch::leaf *) i;
        if (l->deleted()) {
            deleted++;
        } else {
            cb(l,pos);
        }
        pos += l->byte_size() + test_memory + allocation_padding;
        i += (l->byte_size() + test_memory + allocation_padding);

    }
}
void barch::shard::merge(merge_options options) {
    merge(dependencies,options);
}
void barch::shard::merge(const shard_ptr& to, merge_options options) {
    if (!to) return;
    auto &lc = get_leaves();

    lc.iterate_pages([&to, options](size_t s, size_t , auto& data) {
        page_iterator(data, s, [&](const leaf *l, uint32_t ) {
            if (l->deleted()) return;
            if (l->is_tomb()) {
                to->remove(l->get_key());
                return;
            }
            auto opts = l->options();
            auto v = l->get_value();
            if (options.is_compressed() && !opts.is_compressed()) {
                auto vcomp = dictionary::compress(v);
                if (!vcomp.empty()) {
                    v = vcomp;
                    opts.set_compressed(true);
                }
            }else if (options.is_decompress() && opts.is_compressed()) {
                auto vdec = dictionary::decompress(v);
                if (!vdec.empty()) {
                    opts.set_compressed(false);
                    v = vdec;
                }
            }
            to->insert(opts, l->get_key() ,l->get_value(), true, [](node_ptr){});
        });
    });
}
void barch::shard::load_hash() {
    auto &lc = get_leaves();
    size_t encountered = 0;

    lc.iterate_pages([this,&encountered](size_t s, size_t page, auto& data) {
        page_iterator(data, s, [page,this,&encountered](const leaf *l, uint32_t pos) {
            if (l->deleted()) return;
            if (l->is_tomb()) {
                ++tomb_stones;
            }else {
                add_bloom(l->get_key());
            }

            if (l->is_hashed()) {

                logical_address lad{page,pos,this};

                h.insert_unique(lad); // only possible because we know all keys are unique or should be at least
                ++encountered;
            }
        });
    });

    if (encountered != h.size()) {
        abort_with("hashed keys where not unique");
    }
    if (h.size() > 0) {
        opt_ordered_keys = false;
    }
    if (log_loading_messages == 1)
        std_log("loaded hash [",lc.get_name(),"] keys:",h.size(),", bytes per key:",sizeof(hashed_key));
}
/**
 * "active" defragmentation: takes all the fragmented pages and removes the not deleted keys on those
 * then adds them back again
 * this function isn't supposed to run a lot
 */
void barch::shard::run_defrag() {
    auto fc = [](const node_ptr & unused(n)) -> void {
    };
    auto &lc = get_leaves();


    try {
        if (lc.fragmentation_ratio() > 1) //get_min_fragmentation_ratio())
        {
            heap::vector<size_t> fl;
            {
                write_lock releaser(this->latch);
                fl = lc.create_fragmentation_list(get_max_defrag_page_count());
            }
            key_options options;
            for (auto p: fl) {
                write_lock releaser(this->latch);
                // for some reason we have to not do this while a transaction is active
                if (transacted) return; // try later
                auto page = lc.get_page_buffer(p);

                page_iterator(page.first, page.second, [this,p](const leaf *l, uint32_t pos) {
                    if (l->deleted()) return;
                    size_t c1 = this->size;
                    if (l->is_hashed()) {
                        logical_address lad{p,pos,this};
                        h.erase(lad);
                        free_node(lad);
                    }else {
                        this->evict(l);
                        if (c1 - 1 != this->size) {
                            abort_with("key does not exist anymore");
                        }

                    }

                });

                page_iterator(page.first, page.second, [&fc,&options,this](const leaf *l, uint32_t ) {
                    if (l->deleted()) return;
                    if (l->is_hashed()) {
                        options.set_expiry(l->expiry_ms());
                        options.set_volatile(l->is_volatile());
                        hash_insert(options, l->get_key(), l->get_value(),true,fc);
                        return;
                    }
                    size_t c1 = this->size;
                    options.set_expiry(l->expiry_ms());
                    options.set_volatile(l->is_volatile());
                    art_insert(this, options, l->get_key(), l->get_value(), true, fc);
                    if (c1 + 1 != this->size) {
                        abort_with("key not added");
                    }
                    --statistics::insert_ops;
                    --statistics::new_keys_added;

                });
                ++statistics::pages_defragged;
            }
        }
        ++statistics::vacuums_performed;
    } catch (std::exception &) {
        ++statistics::exceptions_raised;
    }

    //std::this_thread::sleep_for(std::chrono::milliseconds(40)); // chill a little we've worked hard
}
static uint64_t calc_mem_threshold() {
    auto mm = barch::get_max_module_memory() ;
    return mm * barch::get_pre_evict_thresh() ;
}

void abstract_eviction(const std::function<void(const barch::leaf *l)> &fupdate,
                       const std::function<std::pair<heap::buffer<uint8_t>, size_t> ()> &src) {

    if (get_total_memory() < calc_mem_threshold()) return;

    auto page = src();
    page_iterator(page.first, page.second, [fupdate](const barch::leaf *l, uint32_t) {
        if (!l->deleted()) {
            fupdate(l);
        }
    });

}
void abstract_eviction(barch::shard *t,
                       const std::function<bool(const barch::leaf *l)> &predicate,
                       const std::function<std::pair<heap::buffer<uint8_t>, size_t> ()> &src) {
    auto fc = [](const art::node_ptr & unused(n)) -> void {
    };
    auto updater = [predicate,fc,t](const barch::leaf *l) {
        if (!l->deleted() && predicate(l)) {
           t->evict(l);
        }
    };
    abstract_eviction(updater, src);
}

void abstract_lru_eviction(barch::shard *t, const std::function<bool(const barch::leaf *l)> &predicate) {
    if (get_total_memory() < calc_mem_threshold()) return;
    write_lock release(t->latch);
    auto &lc = t->get_leaves();
    abstract_eviction(t, predicate, [&lc]() { return lc.get_lru_page(); });
}
void abstract_random_eviction(barch::shard *t, const std::function<bool(const barch::leaf *l)> &predicate) {
    if (get_total_memory() < calc_mem_threshold()) return;
    storage_release release(t->shared_from_this());
    auto &lc = t->get_leaves();
    auto page_num = lc.max_page_num();

    std::uniform_int_distribution<size_t> dist(1, page_num);
    size_t random_page = dist(gen);
    abstract_eviction(t, predicate, [&lc, random_page]() { return lc.get_page_buffer(random_page); });
}

// used during sweep lru keys for the `stochastic` lru eviction
void abstract_random_update(barch::shard *t, const std::function<void(const barch::leaf *l)> &updater) {
    if (get_total_memory() < calc_mem_threshold()) return;
    storage_release release(t->shared_from_this());
    auto &lc = t->get_leaves();
    auto page_num = lc.max_page_num();

    std::uniform_int_distribution<size_t> dist(1, page_num);
    size_t random_page = dist(gen);

    abstract_eviction(updater, [&lc, random_page]() { return lc.get_page_buffer(random_page); });
}
void abstract_lfu_eviction(barch::shard *t, const std::function<bool(const barch::leaf *l)> &predicate) {
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    auto &lc = t->get_leaves();
    abstract_eviction(t, predicate, [&lc]() { return lc.get_lru_page(); });
}

void run_evict_all_keys_lru(barch::shard *t) {
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    if (!t->abstract_shard::opt_evict_all_keys_lru) return;
    abstract_lru_eviction(t, [](const barch::leaf * unused(l)) -> bool { return true; });
}

void run_evict_volatile_keys_lru(barch::shard *t) {
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    if (!t->abstract_shard::opt_evict_volatile_keys_lru) return;
    abstract_lru_eviction(t, [](const barch::leaf *l) -> bool { return l->is_volatile(); });
}

void run_evict_all_keys_random(barch::shard *t) {
    if (!t->opt_evict_all_keys_random) return;
    abstract_random_eviction(t, [](const barch::leaf *) -> bool { return true; });
}
void run_evict_all_keys_lfu(barch::shard *t) {
    if (!t->opt_evict_all_keys_lfu) return;
    abstract_lfu_eviction(t, [](const barch::leaf * unused(l)) -> bool { return true; });
}

void run_evict_volatile_keys_lfu(barch::shard *t) {
    if (!t->opt_evict_volatile_keys_lfu) return;
    abstract_lfu_eviction(t, [](const barch::leaf *l) -> bool {
        return l->is_volatile();
    });
}

void run_evict_volatile_expired_keys(barch::shard *t) {
    if (!t->abstract_shard::opt_evict_volatile_ttl) return;
    abstract_lru_eviction(t, [](const barch::leaf *l) -> bool {
        return l->is_volatile() && l->expired();
    });
}

void run_sweep_expired_keys(barch::shard *t) {
    abstract_random_eviction(t, [](const barch::leaf *l) -> bool {
        return l->expired();
    });
}

void run_sweep_lru_keys(barch::shard *t) {
    if (!t) return;
    if (!t->opt_evict_all_keys_lru && !t->opt_evict_volatile_keys_lru) return;
    abstract_random_update(t, [t](const barch::leaf *l) {
        if (l->is_lru()) {
            auto n = t->search(l->get_key());
            if (!n.null())
                n.l()->unset_lru();
        }else {
            t->evict(l); // will get cleaned up by defrag
        }
    });
}

uint64_t barch::shard::get_modifications() const {
    return deletes + inserts;
}

void barch::shard::start_maintain() {
    this->mods = get_modifications();
    this->start_save_time = std::chrono::high_resolution_clock::now();


}
void barch::shard::maintenance() {
    try {
        repl_client.poll();
        run_sweep_lru_keys(this);
        run_evict_all_keys_lfu(this);
        run_evict_all_keys_random(this);
        run_evict_volatile_keys_lru(this);
        run_evict_volatile_keys_lfu(this);
        run_evict_volatile_expired_keys(this);
        run_sweep_expired_keys(this);

        // defrag will get rid of memory used by evicted keys if memory is pressured - if its configured
        if (this->opt_active_defrag) {
            run_defrag(); // periodic
        }
        if (saf_keys_found) {
            write_lock l(this->latch);
            statistics::keys_found += saf_keys_found;
            saf_keys_found = 0;
            statistics::get_ops += saf_get_ops;
            saf_get_ops = 0;
        }
        auto currtime = std::chrono::high_resolution_clock::now();
        if (millis(currtime, start_save_time) > get_save_interval()
            || get_modifications() - mods > get_max_modifications_before_save()
        ) {
            //if (get_modifications() - mods > 0) {
            this->save(with_stats);

            start_save_time = std::chrono::high_resolution_clock::now();
            mods = get_modifications();
            //}
        }
    }catch (std::exception& e) {
        barch::std_err(e.what());
    }
    ++statistics::maintenance_cycles;

}
