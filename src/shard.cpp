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
                i += l->next_leaf();
                continue;
            }
            t->remove(l->get_key(),fc);
            //art::erase(t, l->get_key(), fc);
            i += l->next_leaf();
        }
        ++statistics::pages_evicted;
        return page.second;
    } catch (std::exception &e) {
        barch::err({e.what(), __FILE__, __LINE__});
        ++statistics::exceptions_raised;
    }
    return 0;
}
#endif

art_statistics barch::get_statistics() {

    art_statistics as{};
    as.heap_bytes_allocated = (int64_t) heap::allocated;
    as.vmm_bytes_allocated = (int64_t) heap::vmm_allocated;
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
    as.vmm_pages_defragged = (int64_t) statistics::vmm_pages_defragged;
    as.vmm_pages_popped = (int64_t) statistics::vmm_pages_popped;
    as.read_locks_active = (int64_t) statistics::read_locks_active;
    as.write_locks_active = (int64_t) statistics::write_locks_active;

    as.exceptions_raised = (int64_t) statistics::exceptions_raised;
    as.maintenance_cycles = (int64_t) statistics::maintenance_cycles;
    as.shards = (int64_t) statistics::shards;
    as.local_calls = (int64_t) statistics::local_calls;
    as.local_calls = (int64_t) statistics::max_spin;
    as.logical_allocated = (int64_t) statistics::logical_allocated;
    as.bytes_in_free_lists = (int64_t) statistics::bytes_in_free_lists;
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
    rs.barch_requests = (int64_t) statistics::repl::barch_requests;
    rs.request_errors = (int64_t) statistics::repl::request_errors;
    rs.redis_sessions = (int64_t) statistics::repl::redis_sessions;
    rs.attempted_routes = (int64_t) statistics::repl::attempted_routes;
    rs.routes_succeeded = (int64_t) statistics::repl::routes_succeeded;
    rs.instructions_failed = (int64_t) statistics::repl::instructions_failed;
    rs.out_queue_size = (int64_t) statistics::repl::out_queue_size;
    rs.routes_succeeded = (int64_t) statistics::repl::routes_succeeded;
    rs.attempted_routes = (int64_t) statistics::repl::attempted_routes;
    return rs;
}
#include "ioutil.h"

template<typename OutStream>
static void stats_to_stream(OutStream &of, const owned_content_stats &o) {
    // what this shard holds, not what the process holds. the file used to carry the globals,
    // so restoring shard n overwrote the totals that shards 0..n-1 had already contributed.
    writep(of, (int64_t) o.n4);
    writep(of, (int64_t) o.n16);
    writep(of, (int64_t) o.n48);
    writep(of, (int64_t) o.n256);
    writep(of, (int64_t) o.occupants);
    writep(of, (int64_t) o.leaves);
    int64_t empty = 0;
    // value_bytes_compressed and oom_avoided_inserts are counted where no shard is in scope
    // and are not attributable to one, so they are no longer written or restored
    writep(of, empty);
    writep(of, empty);

    writep(of, empty);
    writep(of, empty);
    writep(of, empty);
    writep(of, empty);
    writep(of, (int64_t) o.logical);

    if (!of.good()) {
        throw std::runtime_error("art::stats_to_stream: bad output stream");
    }
}

/**
 * Read a shard's counters back and move the globals by the difference.
 *
 * The same call serves a load and a transaction rollback: on a load `o` is zero so the
 * globals gain the whole of what was saved, and on a rollback `o` holds whatever the
 * transaction did, so the globals give exactly that back.
 */
template<typename InStream>
static void stream_to_stats(InStream &in, owned_content_stats &o) {
    if (!in.good()) {
        throw std::runtime_error("art::stream_to_stats: bad output stream");
    }
    owned_content_stats loaded;
    int64_t v = 0, empty = 0;
    readp(in, v); loaded.n4 = v;
    readp(in, v); loaded.n16 = v;
    readp(in, v); loaded.n48 = v;
    readp(in, v); loaded.n256 = v;
    readp(in, v); loaded.occupants = v;
    readp(in, v); loaded.leaves = v;
    readp(in, empty);
    readp(in, empty);

    readp(in, empty);
    readp(in, empty);
    readp(in, empty);
    readp(in, empty);
    readp(in, v); loaded.logical = v;

    statistics::n4_nodes += (int64_t) loaded.n4 - (int64_t) o.n4;
    statistics::n16_nodes += (int64_t) loaded.n16 - (int64_t) o.n16;
    statistics::n48_nodes += (int64_t) loaded.n48 - (int64_t) o.n48;
    statistics::n256_nodes += (int64_t) loaded.n256 - (int64_t) o.n256;
    statistics::node256_occupants += (int64_t) loaded.occupants - (int64_t) o.occupants;
    statistics::leaf_nodes += (int64_t) loaded.leaves - (int64_t) o.leaves;
    statistics::logical_allocated += (int64_t) loaded.logical - (int64_t) o.logical;

    o.n4 = (int64_t) loaded.n4;
    o.n16 = (int64_t) loaded.n16;
    o.n48 = (int64_t) loaded.n48;
    o.n256 = (int64_t) loaded.n256;
    o.occupants = (int64_t) loaded.occupants;
    o.leaves = (int64_t) loaded.leaves;
    o.logical = (int64_t) loaded.logical;
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

const barch::leaf* barch::hashed_key::get_leaf(const query_pair& q) const {
    if (!addr) return nullptr;
    node_ptr n = logical_address{addr, q.leaves};
    return n.is_leaf ? n.const_leaf() : nullptr;
}
value_type barch::hashed_key::get_key(const query_pair& q) const {
    // address 0 is never a live leaf: it is what a slot vacated by remove() holds.
    // those slots are guarded by has[], so this is only reached defensively - and an
    // empty key compares equal to nothing, since a filtered key always carries its
    // null terminator and so has size >= 1.
    auto l = get_leaf(q);
    return l ? l->get_key() : value_type{};
}

void barch::shard::clear_hash() {
    h.clear();
}

void barch::shard::remove_leaf(const logical_address& )  {
}
bool barch::shard::remove_leaf_from_uset(value_type key) {
    auto i = h.find(key_query{key});
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
    auto i = h.find(key_query{key});
    if (i != h.end()) {

        inc_keys_found();
        return i->node(this);
    }
    return nullptr;
}
node_ptr barch::shard::first(size_t start_page) const {
    auto &lc = get_leaves();
    auto fp = start_page;

    node_ptr the_first = nullptr;
    if (fp){
        while (the_first.null()) {
            auto p = lc.get_page_ptr(fp);
            page_iterator_ptr(p.first, p.second, [fp, &the_first, this](const leaf *l, uint32_t pos) {
                if (l->is_tomb()) {
                }else {
                    logical_address ap{fp, pos, this};
                    the_first = ap;
                }
                return true;
            });
            fp = lc.next_page(fp);
        }

    }
    return the_first;
}
node_ptr barch::shard::first() const {
    auto &lc = get_leaves();
    return first(lc.first_page());
}; // can return nullptr
size_t barch::shard::next_page(size_t page ) const {
    auto &lc = get_leaves();
    return lc.next_page(page);
}
size_t barch::shard::page(size_t page, heap::vector<uint8_t>& buffer) const{
    auto &lc = get_leaves();
    if (page) {
        if (!lc.is_page_allocated(page)) {
            return 0;
        }
        auto p = lc.get_page_ptr(page);
        // append to the buffer
        buffer.insert(buffer.end(), p.first, p.first + p.second);
        return p.second;
    }
    return 0;
}; // can return nullptr

bool barch::shard::remove_from_unordered_set(value_type key) {
    return h.erase(key_query{key}) > 0;
}


bool barch::shard::publish(std::string , int ) {

    return true;
}
bool barch::shard::pull(std::string , int ) {
    throw_exception<std::runtime_error>("implement this");
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
void barch::shard::write_extra(std::ostream &of) const {
    uint32_t extra = 1;

    writep(of, extra);
    uint8_t ordered = opt_ordered_keys ? 1 : 0;
    writep(of, ordered);
    // in future we can extend with more options here
}


bool barch::shard::_save(bool stats) const {
    auto *t = this;
    if ((nodes.get_main().get_bytes_allocated()+leaves.get_main().get_bytes_allocated())==0) return true;
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
            stats_to_stream(of, t->owned);
        }
        auto root = logical_address(troot.logical);
        writep(of, root);
        writep(of, troot.is_leaf);
        writep(of, tsize);
        write_extra(of);

    };


    //transaction tx(this); // stabilize main while saving
    //arena::hash_arena leaves{get_leaves().get_name()};
    //arena::hash_arena nodes{get_nodes().get_name()};
    {
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
    return true;
}
bool barch::shard::save(bool stats) {
    //std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    bool success = false;
    std::unique_lock guard(save_load_mutex);
    saving = true;
    auto st = std::chrono::high_resolution_clock::now();
    {
        shared_latch release(this->latch); // only lock during partial copy
        success = _save(stats);
    }
    auto current = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(current - st);
    const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(current - st);
    if (log_saving_messages == 1)
        log({"saved barch db:", this->size, "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds"});
    saving = false;

    start_save_time = std::chrono::high_resolution_clock::now();
    mods = get_modifications();
    return success;
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
        stats_to_stream(of, t->owned);
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

    log({"sent barch db:", t->size, "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds"});
#endif

    return true;
}
bool barch::shard::reload() {
    try {
        unique_latch release(this->latch);
        return reload_holding_lock();
    }catch (std::exception &e) {
        log({"could not load",e.what()});
        return false;
    }
}
bool barch::shard::reload_holding_lock() {
    // the caller holds the shard write lock. RELOAD takes the whole space so a
    // range sweep cannot move a key between two shards while one is already
    // the snapshot and the other is still live. taking the latch here would
    // wait on that space lock from a worker thread and never return.
    try {
        std::unique_lock guard(save_load_mutex);
        _save(true);
        _clear();
        _load(true);
        return true;
    }catch (std::exception &e) {
        log({"could not load",e.what()});
        return false;
    }
}
bool barch::shard::_load(bool) {
    h.clear();
    auto *t = this;
    logical_address root{nullptr};
    bool is_leaf = false;
    // save stats in the leaf storage
    auto load_stats_and_root = [&](std::istream &in) {
        uint32_t w_stats = 0;
        readp(in, w_stats);
        if (w_stats != 0) {
            stream_to_stats(in, t->owned);
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
        log({"Done loading BARCH Shard, keys loaded:", t->size + h.size(), "index mode: [",opt_ordered_keys?"ordered":"unordered","]"});

        log({"loaded barch db in", d.count(), "millis or", (double) dm.count() / 1000000, "seconds"});
        log({"db memory when created", (double) get_total_memory() / (1024 * 1024), "Mb"});
    }
    return true;
}
bool barch::shard::load(bool) {

    //
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    try {
        unique_latch release(this->latch);
        _load(true);
    }catch (std::exception &e) {
        log({"could not load",e.what()});
        return false;
    }
    return true;
}
bool barch::shard::load_holding_lock() {
    // the caller holds the shard write lock. LOAD takes the whole space so a
    // range sweep cannot move a key between two shards while one is already
    // the file and the other is still live. taking the latch here would wait
    // on that space lock from a worker thread and never return.
    try {
        std::unique_lock guard(save_load_mutex);
        // overwrite: the files replace what is live. without the clear, the
        // arena free list from the file is read into a shard that still holds
        // the old one, and read_emancipated logs "erased should be empty"
        _clear();
        _load(true);
        return true;
    }catch (std::exception &e) {
        log({"could not load",e.what()});
        return false;
    }
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
                stream_to_stats(in, t->owned);
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
        log({"Done loading BARCH, keys loaded:", t->size, ""});

        log({"loaded barch db in", d.count(), "millis or", (float) dm.count() / 1000000, "seconds"});
        log({"db memory when created", (float) get_total_memory() / (1024 * 1024), "Mb"});
    }catch (std::exception &e) {
        log({"could not load",e.what()});
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
    stats_to_stream(save_stats, owned);
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
    stream_to_stats(save_stats, owned);
    transacted = false;
}
void barch::shard::load_bloom() {
    if (!has_static_bloom_filter()) return;
    auto &lc = get_leaves();
    lc.iterate_pages([this](size_t s, size_t unused(page), auto& data) {
        page_iterator(data, s, [this](const leaf *l, uint32_t unused(pos)) {
            if (l->deleted() || l->is_tomb()) return true;
            add_bloom(l->get_key());
            return true;
        });
    });

}
void barch::shard::_clear() {
    root = {nullptr};
    size = 0;
    transacted = false;
    tomb_stones = 0;
    blocked_sessions.clear();
    mods = 0;
    saf_get_ops = 0;
    saf_keys_found = 0;
    queue_size = 0;
    create_bloom(has_static_bloom_filter()); // resets the bloom
    get_leaves().clear();
    get_nodes().clear();
    h.clear();
    // take away what this shard held, rather than zeroing counters the other shards share.
    // the event counters (oom_avoided_inserts, keys_found, new_keys_added, keys_replaced)
    // count things that happened rather than things that exist, so clearing a shard does
    // not unmake them and they are left alone. value_bytes_compressed is counted where no
    // shard is in scope, so it cannot be attributed here either.
    statistics::n4_nodes -= (int64_t) owned.n4;
    statistics::n16_nodes -= (int64_t) owned.n16;
    statistics::n48_nodes -= (int64_t) owned.n48;
    statistics::n256_nodes -= (int64_t) owned.n256;
    statistics::node256_occupants -= (int64_t) owned.occupants;
    statistics::leaf_nodes -= (int64_t) owned.leaves;
    statistics::logical_allocated -= (int64_t) owned.logical;
    owned.zero();
}
void barch::shard::clear() {
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    storage_release release(this->shared_from_this());
    _clear();

}

bool barch::shard::insert(value_type key, value_type value, bool update, const NodeResult &fc) {
    return this->opt_insert({}, key, value, update, fc);
}
bool barch::shard::insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    return opt_rpc_insert(options, unfiltered_key, value, update, fc);
}
bool barch::shard::tree_insert(const art::key_options &options, art::value_type key, art::value_type value, bool update, const art::NodeResult &fc) {
    ++inserts;
    //add_bloom(key);
    return art::insert(this, options, key, value, update, fc);
}

bool barch::shard::hash_erase(logical_address lad) {
    if (&lad.get_ap<alloc_pair>() != &this->get_ap()) {
        abort_with("invalid address pointer");
    }
    size_t s = h.size();
    h.erase(lad);
    free_node(lad);
    return h.size() == s - 1;
}

bool barch::shard::hash_insert(const key_options &options, value_type key, value_type value, bool update, const NodeResult &fc) {
    if (statistics::logical_allocated > get_max_module_memory()) {
        ++statistics::oom_avoided_inserts;
        throw_exception<std::runtime_error>("not enough memory");
    }
    ++inserts;
    ++statistics::insert_ops;
    auto i = h.find(key_query{key});
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
                dl->set_compressed(options.is_compressed());
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
    if (statistics::logical_allocated > get_max_module_memory()) {
        ++statistics::oom_avoided_inserts;
        throw_exception<std::runtime_error>("not enough memory");
    }

    std::string tk;
    value_type key = s_filter_key(tk,unfiltered_key);
    add_bloom(key);
    cancel_flight(key);

    size_t before = size;
    if (options.is_hashed()) {
        hash_insert(options, key, value, update, fc);
    }else {
        art::insert(this, options, key, value, update, fc);
    }
    call_unblock(std::string(key.chars(), key.size));
    return size+h.size() > before;
}


bool barch::shard::opt_insert(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    return opt_rpc_insert(options, unfiltered_key, value, update, fc);
}

bool barch::shard::insert(value_type key, value_type value, bool update) {
    return this->opt_insert({},key, value, update, [](const node_ptr &) {}) ;
}
bool barch::shard::update(value_type unfiltered_key, const std::function<node_ptr(const node_ptr &leaf)> &updater) {
    if (statistics::logical_allocated > get_max_module_memory()) {
        ++statistics::oom_avoided_inserts;
        throw_exception<std::runtime_error>("not enough memory");
    }
    // own the filtered bytes: the updater is caller supplied and the ordered path
    // below re-enters the tree, either of which could otherwise reuse a shared buffer
    std::string kbuf;
    auto key = s_filter_key(kbuf, unfiltered_key);
    cancel_flight(key);
    auto repl_updateresult = [&](const node_ptr &leaf) {
        auto value = updater(leaf);
        if (value.null()) {
            return value;
        }
        return value;
    };
    auto i = h.find(key_query{key});
    if (!opt_ordered_keys){
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
                call_unblock(std::string(key.chars(), key.size));
                return false; // nothing to do
            }
            if (!n.null()) {
                n.l()->set_hashed();
                h.erase(i);
                h.insert(n);
                old.free_from_storage();// ok if old is null - nothing will happen
            }
            call_unblock(std::string(key.chars(), key.size));
            return !n.null();
        }
        call_unblock(std::string(key.chars(), key.size));
        return false;
    }
    bool r = barch::update(this, key, repl_updateresult);
    call_unblock(std::string(key.chars(), key.size));
    return r;
}
bool barch::shard::evict(const leaf* l) {
    if (l->deleted()) return false;
    size_t before = size;
    if (l->is_hashed()) {
        auto i = h.find(key_query{l->get_key()});
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
    art::erase(this, l->get_key(), [](const art::node_ptr &){});
    if (size < before) {
        ++statistics::keys_evicted;
    }
    --statistics::delete_ops; // were not counting these deletes
    return size < before;
}
bool barch::shard::evict(value_type unfiltered_key) {
    size_t before = size;

    std::string kbuf;
    auto key = s_filter_key(kbuf, unfiltered_key);
    node_ptr old = from_unordered_set(key);
    if (!old.null()) {
        auto n = old;
        leaf *dl = n.l();
        if (dl->is_hashed()) {
            erase_tomb(dl);
            h.erase(key_query{key});
            n.free_from_storage();
            return true;
        }
    }
    --statistics::delete_ops; // were not counting these deletes
    art::erase(this, key, [](const art::node_ptr &){});
    return size < before;

}
bool barch::shard::tree_remove(value_type key, const NodeResult &fc) {
    auto sbef = size;
    ++deletes;
    art::erase(this, key, fc);
    return sbef < size;
}


bool barch::shard::remove(value_type unfiltered_key, const NodeResult &fc) {
    ++deletes;
    size_t before = size;
    // this one matters most: key stays live across dependencies->search(key), which
    // filters again, and is still used afterwards by h.erase and the tree paths
    std::string kbuf;
    auto key = s_filter_key(kbuf, unfiltered_key);
    cancel_flight(key);
    struct wake_on_exit {
        shard* s;
        std::string k;
        ~wake_on_exit() { s->call_unblock(k); }
    } wake{this, std::string(key.chars(), key.size)};
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
            h.erase(key_query{key});
            n.free_from_storage();

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

    art::erase(this, key, fc);
    return size < before;
}

int barch::shard::range(art::value_type key, art::value_type key_end, CallBack cb, void *data) {
    return art::range(this, key, key_end, cb, data);
}

int barch::shard::range(art::value_type unused(key), art::value_type unused(key_end), LeafCallBack unused(cb)) {
    //return art::range(this, key, key_end, cb);
    return -1;
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
uint64_t barch::shard::bytes_in_free_list() {
    return get_nodes().get_bytes_in_free_list() + get_leaves().get_bytes_in_free_list();
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

void barch::shard::glob(const keys_spec &spec, value_type pattern, bool value, const std::function<bool(const leaf &)> &cb,
                        const glob_page_list *only, glob_page_list *hits)  {

    if (dependencies) {
        // pull sources have their own page ids. a list from this shard
        // must not constrain or collect theirs.
        dependencies->glob(spec, pattern, value, cb);
    }
    art::glob(this, spec, pattern, value, cb, only, hits);
}

art::node_ptr barch::shard::local_leaf(value_type unfiltered_key) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    if (!opt_ordered_keys) {
        return from_unordered_set(key);
    }
    return art::search(this, key);
}

void barch::shard::insert_cached_miss(value_type unfiltered_key, uint64_t ttl_ms, bool hashed) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    add_bloom(key);

    node_ptr existing = hashed ? from_unordered_set(key) : art::search(this, key);
    const bool was_tomb = !existing.null() && existing.cl()->is_tomb();
    const bool valid_tomb = was_tomb && !existing.cl()->expired();
    const bool live = !existing.null() && !existing.cl()->is_tomb() && !existing.cl()->expired();
    if (live) {
        call_unblock(std::string(key.chars(), key.size));
        return;
    }
    if (valid_tomb && ttl_ms && existing.cl()->is_expiry()) {
        existing.l()->set_expiry(art::now() + static_cast<leaf::ExpiryType>(ttl_ms));
        call_unblock(std::string(key.chars(), key.size));
        return;
    }
    if (valid_tomb && !ttl_ms) {
        call_unblock(std::string(key.chars(), key.size));
        return;
    }

    art::key_options opts;
    opts.set_keep_ttl(false);
    opts.set_hashed(hashed);
    if (ttl_ms)
        opts.set_expiry(art::now() + static_cast<int64_t>(ttl_ms));
    art::value_type empty{};
    if (hashed)
        hash_insert(opts, key, empty, true, [](const node_ptr&) {});
    else
        art::insert(this, opts, key, empty, true, [](const node_ptr&) {});
    node_ptr n = hashed ? from_unordered_set(key) : art::search(this, key);
    if (n.null()) {
        call_unblock(std::string(key.chars(), key.size));
        return;
    }
    n.l()->set_tomb();
    if (!was_tomb)
        ++tomb_stones;
    call_unblock(std::string(key.chars(), key.size));
}

void barch::shard::cancel_flight(value_type key) {
    std::string k(key.chars(), key.size);
    auto it = flights.find(k);
    if (it == flights.end())
        return;
    auto& fl = *it->second;
    if (fl.state != foreign_flight::state::pending)
        return;
    ++fl.generation;
    fl.state = foreign_flight::state::cancelled;
    fl.finished = true;
    fl.swig_cv.notify_all();
}

void barch::shard::fail_foreign(const char* msg, heap::vector<abstract_session_ptr>& sessions) {
    for (auto& [k, fl] : flights) {
        fl->state = foreign_flight::state::failed;
        fl->error = msg;
        fl->finished = true;
        fl->swig_cv.notify_all();
    }
    for (auto& [k, vec] : blocked_sessions) {
        sessions.insert(sessions.end(), vec.begin(), vec.end());
    }
    blocked_sessions.clear();
}

bool barch::shard::is_present(value_type unfiltered_key) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    if (!opt_ordered_keys) {
        auto n = from_unordered_set(key);
        return !n.null();
    }

    auto r = art::search(this, key);
    return !r.null();
}


art::node_ptr barch::shard::search(value_type unfiltered_key) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);

    if (!opt_ordered_keys) {
        auto n = from_unordered_set(key);
        if (!n.null() && n.cl()->is_tomb()) {
            return nullptr;
        }
        return n;
    }

    auto r = art::search(this, key);
    if (r.null()) {
        if (dependencies) {
            r = dependencies->search(key); // this can recurse down
            if (!r.null()) {
                return r;
            }
        }
        last_leaf_added = nullptr; // clear it before trying to retrieve
        // TODO: retrieve if pull is enabled
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
    auto tmin = art::minimum(this);
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
        barch::err({e.what(), __FILE__, __LINE__});
        ++statistics::exceptions_raised;
    }
    return 0;
}

barch::shard::~shard() {
    shard::blocked_sessions.clear();
    if (opt_drop_on_release) {
        this->get_leaves().delete_files(EXT);
        this->get_nodes().delete_files(EXT);
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
            if (l->is_tomb()) {
                to->remove(l->get_key());
                return true;
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
            return true;
        });
    });
}
void barch::shard::load_hash() {
    auto &lc = get_leaves();
    size_t encountered = 0;

    lc.iterate_pages([this,&encountered](size_t s, size_t page, auto& data) {
        page_iterator(data, s, [page,this,&encountered](const leaf *l, uint32_t pos) {

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
            return true;
        });
    });

    if (encountered != h.size()) {
        abort_with("hashed keys where not unique");
    }
    if (h.size() > 0) {
        opt_ordered_keys = false;
    }
    if (log_loading_messages == 1)
        log({"loaded hash [",lc.get_name(),"] keys:",h.size(),", bytes per key:",sizeof(hashed_key)});
}
/**
 * "active" defragmentation: takes all the fragmented pages and removes the not deleted keys on those
 * then adds them back again. it will also attempt to move keys out of the way so that the vm page can
 * be shrunk (if possible)
 * this function isn't supposed to run a lot
 */
static void erase_page(const barch::shard_ptr& shard, const std::pair<heap::buffer<uint8_t>, size_t>& page) {
    page_iterator(page.first, page.second, [shard,page](const leaf *l, uint32_t unused(pos)) {
        bool hashed = l->is_hashed();
        size_t c1 = shard->get_size();
        shard->evict(l);
        if (c1 - 1 != shard->get_size()) {
            if (hashed)
                barch::err({"hashed key not found"});
            else
                barch::err({"ordered key not found"});
            abort_with("key not marked as deleted but it was not found");
        }
        return true;
    });
}
static void defrag_page(const barch::shard_ptr& shard, const std::pair<heap::buffer<uint8_t>, size_t>& page) {
    key_options options;
    auto fc = [](const node_ptr & unused(n)) -> void {
    };
    page_iterator(page.first, page.second, [&fc,&options,shard](const leaf *l, uint32_t ) {
        if (l->is_hashed()) {
            options.set_expiry(l->expiry_ms());
            options.set_volatile(l->is_volatile());
            options.set_compressed(l->is_compressed());
            shard->hash_insert(options, l->get_key(), l->get_value(),true,fc);
            return true;
        }
        size_t c1 = shard->get_tree_size();
        options.set_expiry(l->expiry_ms());
        options.set_volatile(l->is_volatile());
        options.set_compressed(l->is_compressed());
        auto v = l->get_value();
        // TODO: one day add compression here
        shard->tree_insert(options, l->get_key(), v, true, fc);
        if (c1 + 1 != shard->get_tree_size()) {
            abort_with("key not added");
        }
        --statistics::insert_ops;
        --statistics::new_keys_added;
        return true;
    });

    ++statistics::pages_defragged;
}
void barch::shard::run_defrag() {
    if (this->get_size() == 0) return;

    auto &lc = get_leaves();

    {
        unique_latch releaser(this->latch);
        this->shrink();
    }

    auto logical_frag = lc.fragmentation_ratio();
    try {

        if (logical_frag > 0.3) //get_min_fragmentation_ratio())
        {
            heap::vector<size_t> fl;
            {
                unique_latch releaser(this->latch);
                fl = lc.create_fragmentation_list(get_max_defrag_page_count());
            }

            for (auto p: fl) {
                unique_latch releaser(this->latch);
                // for some reason we have to not do this while a transaction is active
                if (transacted) return; // try later
                auto page = lc.get_page_buffer(p);
                erase_page(this->shared_from_this(), page);
                defrag_page(this->shared_from_this(), page);
            }
        }
        ++statistics::vacuums_performed;
    } catch (std::exception &) {
        ++statistics::exceptions_raised;
    }

}
static uint64_t calc_mem_threshold() {
    auto mm = barch::get_max_module_memory() ;
    return mm * barch::get_pre_evict_thresh() ;
}

void abstract_eviction(const std::function<void(const barch::leaf *l)> &fupdate,
                       const std::function<std::pair<heap::buffer<uint8_t>, size_t> ()> &src) {

    if (statistics::logical_allocated < calc_mem_threshold()) return;

    auto page = src();
    page_iterator(page.first, page.second, [fupdate](const barch::leaf *l, uint32_t) {
        if (!l->deleted()) {
            fupdate(l);
        }
        return true;
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
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    unique_latch release(t->latch);
    auto &lc = t->get_leaves();
    abstract_eviction(t, predicate, [&lc]() { return lc.get_lru_page(); });
}
void abstract_random_eviction(barch::shard *t, const std::function<bool(const barch::leaf *l)> &predicate) {
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    storage_release release(t->shared_from_this());
    auto &lc = t->get_leaves();
    auto page_num = lc.max_allocated_page_num();

    std::uniform_int_distribution<size_t> dist(1, page_num);
    size_t random_page = dist(gen);
    abstract_eviction(t, predicate, [&lc, random_page]() { return lc.get_page_buffer(random_page); });

}

// used during sweep lru keys for the `stochastic` lru eviction
void abstract_random_update(barch::shard *t, const std::function<void(const barch::leaf *l)> &updater) {
    if (statistics::logical_allocated < calc_mem_threshold()) return;
    storage_release release(t->shared_from_this());
    auto &lc = t->get_leaves();
    auto page_num = lc.max_allocated_page_num();

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
            unique_latch l(this->latch);
            statistics::keys_found += saf_keys_found;
            saf_keys_found = 0;
            statistics::get_ops += saf_get_ops;
            saf_get_ops = 0;
        }
        auto currtime = std::chrono::high_resolution_clock::now();
        if (millis(currtime, start_save_time) > get_save_interval()
            || get_modifications() - mods > get_max_modifications_before_save()
        ) {
            if (get_modifications() - mods > 0) {

                //log({"saving",get_leaves().get_name(), "modifications",get_modifications(),"time",millis(currtime, start_save_time)});
                this->save(with_stats);

            }
        }
    }catch (std::exception& e) {
        barch::err({e.what()});
    }
}
