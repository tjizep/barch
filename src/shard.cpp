//
// Created by teejip on 10/14/25.
//

#include "shard.h"
#include <sstream>
#include "block_stream.h"
#include "module.h"
#include <random>
#include <algorithm>
#include <cstring>

#include "dictionary_compressor.h"
#include "keys.h"
#include "time_conversion.h"

static thread_local std::mt19937 gen{std::random_device{}()};


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
    as.named_vmm_bytes_allocated = (int64_t) heap::named_vmm_allocated;
    as.leaf_nodes = (int64_t) statistics::leaf_nodes;
    as.node4_nodes = (int64_t) statistics::n4_nodes;
    as.node16_nodes = (int64_t) statistics::n16_nodes;
    as.node256_nodes = (int64_t) statistics::n256_nodes;
    as.node256_occupants = as.node256_nodes ? ((int64_t) statistics::node256_occupants / as.node256_nodes) : 0ll;
    as.node48_nodes = (int64_t) statistics::n48_nodes;
    // all_shards holds nothing while it calls back - it takes ksp().lock only to
    // copy the space map - so these counters were read while writers updated them
    // under the shard latch. A shared latch here rather than atomics on the
    // counters: they are touched on every allocation and statistics are rare, so
    // the cost belongs on this side. See TODO 204.
    // One pass, not two. all_shards holds nothing while it calls back - it takes
    // ksp().lock only to copy the space map - so these counters were read while
    // writers updated them under the shard latch. A shared latch here rather than
    // atomics on the counters: they are touched on every allocation and statistics
    // are rare, so the cost belongs on this side. Both figures come off the same
    // shard, so taking its latch twice was paying for the same walk twice.
    // See TODO 204.
    barch::all_shards( [&as](const shard_ptr &shard) {
        shared_latch release(shard->get_latch());
        const auto interior = (int64_t) shard->get_ap().get_nodes().get_allocated();
        as.bytes_allocated += (int64_t) shard->get_ap().get_leaves().get_allocated() + interior;
        as.bytes_interior += interior;
    });
    as.value_bytes_compressed = (int64_t) statistics::value_bytes_compressed;
    as.vacuums_performed = (int64_t) statistics::vacuums_performed;
    as.last_vacuum_time = (int64_t) statistics::last_vacuum_time;
    as.leaf_nodes_replaced = (int64_t) statistics::leaf_nodes_replaced;
    as.pages_evicted = (int64_t) statistics::pages_evicted;
    as.keys_evicted = (int64_t) statistics::keys_evicted;
    as.files_evicted = (int64_t) statistics::files_evicted;
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
    as.function_timeouts = (int64_t) statistics::function_timeouts;
    as.function_errors = (int64_t) statistics::function_errors;
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
    rs.refused_connections = (int64_t) statistics::repl::refused_connections;
    rs.accept_errors = (int64_t) statistics::repl::accept_errors;
    rs.net_errors = (int64_t) statistics::repl::net_errors;
    return rs;
}
#include "ioutil.h"

namespace barch {
    // the wakes a transaction has raised but not yet sent. Thread local because a
    // transaction runs on the thread that is executing EXEC and nowhere else
    static thread_local unsigned defer_depth = 0;
    static thread_local std::vector<std::pair<abstract_shard*, std::string>> held_wakes;

    bool wakes_deferred() {
        return defer_depth > 0;
    }

    void defer_wake(abstract_shard* shard, const std::string& key) {
        for (auto& h : held_wakes) {
            if (h.first == shard && h.second == key) return;   // once is enough
        }
        held_wakes.emplace_back(shard, key);
    }

    defer_wakes::defer_wakes() {
        ++defer_depth;
    }

    defer_wakes::~defer_wakes() {
        if (--defer_depth > 0) return;
        auto sending = std::move(held_wakes);
        held_wakes.clear();
        for (auto& h : sending) {
            // the latch, because blocked_sessions is the shard's and every other caller
            // of call_unblock holds it - they are inside a write when they wake somebody
            std::unique_lock lock(h.first->get_latch());
            h.first->call_unblock(h.second);
        }
    }
}

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

/*
 * peek_leaf, not const_leaf: this is the probe. hk_hash and hk_eq are the only
 * callers, and they walk candidates to find one whose key matches, so every
 * candidate they reject would otherwise be stamped as recently used on the way
 * past. The key that is actually found gets stamped by the caller that asks for
 * it - from_unordered_set hands back a node_ptr and GET calls const_leaf() on
 * it. See TODO 306.
 */
const barch::leaf* barch::hashed_key::get_leaf(const query_pair& q) const {
    if (!addr) return nullptr;
    node_ptr n = logical_address{addr, q.leaves};
    return n.is_leaf ? n.peek_leaf() : nullptr;
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

void barch::shard::hash_add_leaf(const node_ptr& leaf) {
    if (leaf.null() || !leaf.is_leaf) return;
    hashed_key want{leaf};
    auto i = h.find(key_query{leaf.const_leaf()->get_key()});
    if (i != h.end()) {
        if (i->addr == want.addr) return;
        h.erase(i);
    }
    h.insert_unique(want);
}

void barch::shard::hash_unindex(value_type key) {
    h.erase(key_query{key});
}

void barch::shard::rebuild_hybrid_index() {
    h.clear();
    if (!hybrid_active()) return;
    if (size.load(std::memory_order_relaxed) == 0) return;
    auto &lc = get_leaves();
    lc.iterate_pages([this](size_t s, size_t page, auto& data) {
        if (s == 0) return;
        page_iterator(data, s, [page, this](const leaf *l, uint32_t pos) {
            if (l->is_hashed() || l->deleted()) return true;
            logical_address lad{page, pos, this};
            h.insert_unique(lad);
            return true;
        });
    });
}

/*
 * Carry the eviction policy down to the alloc pair, which is what make_leaf and
 * the two l() accessors read to decide whether to stamp the leaf LRU bit. These
 * were never assigned - the shard had its own pair of flags and nothing joined
 * them up - so no leaf ever carried the bit and run_sweep_lru_keys evicted
 * everything it walked instead of giving a touched key its second chance.
 * See TODO 305.
 *
 * Both flags turn the read path into a writer. Not because of the modify<> that
 * l() switches to - read<> and modify<> both resolve through
 * arena::get_page_data with modify passed as true and the arena ignores the
 * argument, so that part costs nothing - but because of set_leaf_lru itself,
 * which stores to the leaf's flags byte on every read. That is the price of a
 * real LRU and it is only paid when a policy asks for one.
 *
 * Two consequences, neither of them paid for yet. Under a CoW page the store is
 * a first touch, so a plain GET inside a transaction can now copy a page that
 * used to be read only. And GET holds a shared lock, so two threads reading the
 * same key both do `flags |= leaf_lru_flag` on the same byte with no
 * synchronisation - they write the same value, which is why nothing has ever
 * gone wrong, but it is a data race and TSan will say so. See TODO 305.
 */
void barch::shard::apply_lru_options() {
    auto &ap = get_ap();
    /*
     * Compression uses the same bits. A space with compression on and eviction
     * off still wants the clock running - it just compresses what it finds cold
     * instead of dropping it - so the stamping is on for either reason. See
     * TODO 300 and run_compress_cold_keys.
     */
    ap.opt_all_keys_lru = abstract_shard::opt_evict_all_keys_lru.load(std::memory_order_relaxed)
                       || compresses_cold_keys();
    ap.opt_volatile_keys_lru = abstract_shard::opt_evict_volatile_keys_lru.load(std::memory_order_relaxed);
}

void barch::shard::apply_hybrid_keys() {
    if (hybrid_active())
        rebuild_hybrid_index();
    else if (opt_ordered_keys)
        h.clear();
}


bool barch::shard::publish(std::string , int ) {

    return true;
}
bool barch::shard::pull(std::string host, int port) {
    /*
     * Register this shard's pull source, which is a route.
     *
     * This threw "implement this" and had done for as long as `test/pulltest.py`
     * and `test/pulldebug.py` have been failing - neither is registered with
     * ctest, so nothing said so. The mechanism it wanted already exists and is
     * already used by this class: the route table is what fetches a missing key
     * from another barch on demand, it is what `ADDROUTE` sets one shard at a
     * time, and it is what this shard's own constructor clears. `PULL host port`
     * is that said once for every shard rather than five hundred times by hand,
     * which is exactly what routetest.py does in a loop.
     *
     * The return follows what the declaration in abstract_shard.h asks for -
     * true when this is a source the shard did not already have.
     */
    auto had = barch::repl::get_route(shard_number);
    barch::repl::set_route(shard_number, {host, port});
    return !(had.ip == host && had.port == port);
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
    if (extra > 0) {
        uint8_t hybrid = 0;
        readp(in, hybrid);
        opt_hybrid_keys = hybrid != 0;
        --extra;
    }
    if (extra > 0) {
        // how many shards the space had when this was written, so a load with a
        // different count can be refused instead of half working - TODO 314
        uint64_t shards = 0;
        readp(in, shards);
        saved_space_shards = shards;
        --extra;
    }
    /*
     * Fields from a newer version, skipped so an older binary can still read a
     * newer file. `--extra` matters: without it this spins forever on the first
     * field it does not know, which nothing had hit only because nothing had
     * ever written a third one. See TODO 314.
     */
    while (extra > 0) {
        uint8_t x;
        readp(in, x); // bytes from some future version
        --extra;
    }
}
void barch::shard::write_extra(std::ostream &of) const {
    // 3 fields now. An older binary reading this skips the third, which is what
    // the loop at the end of read_extra is for - and which only works since the
    // `--extra` it was missing went in. See TODO 314.
    uint32_t extra = 3;

    writep(of, extra);
    uint8_t ordered = opt_ordered_keys ? 1 : 0;
    writep(of, ordered);
    uint8_t hybrid = opt_hybrid_keys ? 1 : 0;
    writep(of, hybrid);
    uint64_t shards = space_shards.load(std::memory_order_relaxed);
    writep(of, shards);
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
        tsize = t->size.load(std::memory_order_relaxed);
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
/**
 * The snapshots beside the mapped arenas - TODO 262.
 *
 * The same extra blocks `_save` writes into the shard file, so a snapshot restores
 * exactly what a load would have; only the page data is left out, being in the
 * mapping already. Written under the same lock a save takes, and only at shutdown,
 * because that is the only moment nothing will write again.
 */
bool barch::shard::save_snapshot() {
    auto *t = this;
    if ((nodes.get_main().get_bytes_allocated()+leaves.get_main().get_bytes_allocated())==0)
        return true;
    std::unique_lock guard(save_load_mutex);
    shared_latch release(this->latch);
    node_ptr troot = t->root;
    size_t tsize = t->size.load(std::memory_order_relaxed);
    auto save_stats_and_root = [&](std::ostream &of) {
        uint32_t w_stats = 0;
        writep(of, w_stats);
        auto root = logical_address(troot.logical);
        writep(of, root);
        writep(of, troot.is_leaf);
        writep(of, tsize);
        write_extra(of);
    };
    bool ok = get_leaves().snapshot_extra(save_stats_and_root);
    ok = get_nodes().snapshot_extra([](std::ostream &) {}) && ok;
    return ok;
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
        log({"saved barch db:", this->size.load(std::memory_order_relaxed), "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds"});
    saving = false;

    start_save_ns.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    mods.store(get_modifications(), std::memory_order_relaxed);
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
        tsize = t->size.load(std::memory_order_relaxed);
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

    log({"sent barch db:", t->size.load(std::memory_order_relaxed), "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
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
        {
            uint64_t sz = 0;
            readp(in, sz);
            t->size.store(sz, std::memory_order_relaxed);
        }
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
        log({"Done loading BARCH Shard, keys loaded:", get_size(), "index mode: [",opt_ordered_keys?(hybrid_active()?"hybrid":"ordered"):"unordered","]"});

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
            {
            uint64_t sz = 0;
            readp(in, sz);
            t->size.store(sz, std::memory_order_relaxed);
        }
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
        log({"Done loading BARCH, keys loaded:", t->size.load(std::memory_order_relaxed), ""});

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
    storage_release release(this->shared_from_this());
    begin_holding_lock();
}

/*
 * Everything a transaction needs to remember is taken here, under the write
 * latch - TODO 416. The root, the size and the stats used to be read before the
 * latch was, so a write could land between them and the CoW maps. And BEGIN on a
 * space now holds every shard's latch while each one begins (keyspace_api.cpp), so
 * all of them start at the same moment rather than one after the other.
 */
void barch::shard::begin_holding_lock() {
    if (transacted) return;
    save_root = root;
    save_size = size.load(std::memory_order_relaxed);
    save_tombs = tomb_stones.load(std::memory_order_relaxed);
    save_last_leaf = last_leaf_added;
    save_stats.clear();
    stats_to_stream(save_stats, owned);
    {
        // the parts of the file a page walk doesn't carry, as they are now
        std::ostringstream fl, fn, sb;
        get_leaves().write_free_list(fl);
        get_nodes().write_free_list(fn);
        write_stats_block(sb, root, size.load(std::memory_order_relaxed));
        begin_free_leaves = fl.str();
        begin_free_nodes = fn.str();
        begin_stats = sb.str();
        std::ostringstream pl, pn;
        get_leaves().write_stream_prefix(pl, [this](std::ostream& o) {
            write_stats_block(o, root, size.load(std::memory_order_relaxed));
        });
        get_nodes().write_stream_prefix(pn, [](std::ostream&) {});
        begin_prefix_leaves = pl.str();
        begin_prefix_nodes = pn.str();
    }
    get_leaves().begin();
    get_nodes().begin();
    begin_leaves = std::make_unique<arena::hash_arena>(get_leaves().get_name());
    begin_leaves->borrow(get_leaves().get_main());
    begin_nodes = std::make_unique<arena::hash_arena>(get_nodes().get_name());
    begin_nodes->borrow(get_nodes().get_main());
    ++tx_generation;
    transacted = true;
}

void barch::shard::commit() {
    if (!transacted) return;
    storage_release release(this->shared_from_this());
    // before the commit, which can remap the pages these point at
    begin_leaves.reset();
    begin_nodes.reset();
    begin_free_leaves.clear();
    begin_free_nodes.clear();
    begin_stats.clear();
    begin_prefix_leaves.clear();
    begin_prefix_nodes.clear();
    ++tx_generation;
    get_leaves().commit();
    get_nodes().commit();
    transacted = false;
}

void barch::shard::rollback() {
    if (!transacted) return;
    storage_release release(this->shared_from_this());
    begin_leaves.reset();
    begin_nodes.reset();
    begin_free_leaves.clear();
    begin_free_nodes.clear();
    begin_stats.clear();
    begin_prefix_leaves.clear();
    begin_prefix_nodes.clear();
    ++tx_generation;
    // the arenas put their page tables and free lists back as well as dropping
    // the CoW pages - before TODO 417 they only dropped the pages, and the space
    // freed in the transaction got handed out again under the restored tree
    get_leaves().rollback();
    get_nodes().rollback();
    root = save_root;
    size = save_size;
    tomb_stones.store(save_tombs, std::memory_order_relaxed);
    last_leaf_added = save_last_leaf;
    save_stats.seek(0);
    stream_to_stats(save_stats, owned);
    transacted = false;
    // the hybrid index holds leaf addresses from inside the transaction, some
    // of them gone now, and it can't be put back the way the arenas can - it is
    // rebuilt from the leaves instead. The bloom filter keeps keys the
    // transaction added, which costs a false positive, never a wrong answer
    if (hybrid_active())
        rebuild_hybrid_index();
}
/*
 * One page at a time on the calling thread - TODO 416. The same walk KEYS and
 * VALUES do on worker threads (logical_allocator::iterate_pages), without the
 * threads: the page list is taken under a read latch, then each page is copied
 * under a read latch of its own and handed over with no latch held, so `f` can
 * be slow - a backup writing to disk, a script - without holding anyone up.
 *
 * Inside a transaction both come from the BEGIN-time view, so what comes back
 * is the space as it was at BEGIN whatever has been written since. Pages are
 * visited in page number order, which a backup can rely on.
 */
void barch::shard::start_page_walk(bool nodes, page_walk& w) {
    shared_latch guard(this->latch);
    w.nodes = nodes;
    w.in_tx = transacted;
    w.generation = tx_generation;
    w.pages.clear();
    const arena::hash_arena& a = w.in_tx ? *(nodes ? begin_nodes : begin_leaves)
                                         : (nodes ? get_nodes() : get_leaves()).get_main();
    w.pages.reserve(a.get_arena().size());
    for (const auto& kv : a.get_arena()) {
        if (!logical_address::is_null_base(kv.first))
            w.pages.push_back(kv.first);
    }
    std::sort(w.pages.begin(), w.pages.end());
}

int barch::shard::read_walk_page(const page_walk& w, size_t page, heap::buffer<uint8_t>& out,
                                 std::string& err) {
    shared_latch guard(this->latch);
    if (w.in_tx && tx_generation != w.generation) {
        // committed, rolled back or cleared: the pages this walk was reading
        // are gone, and carrying on would mix two states
        err = "the transaction ended during the page walk";
        return -1;
    }
    const arena::hash_arena& a = w.in_tx ? *(w.nodes ? begin_nodes : begin_leaves)
                                         : (w.nodes ? get_nodes() : get_leaves()).get_main();
    if (a.is_free(page))
        return 0;                   // freed since the list was taken
    try {
        const auto* st = (const storage*) a.get_page_data({page, page_size - sizeof(storage), nullptr}, false);
        if (st->write_position == 0)
            return 0;
        out = heap::buffer<uint8_t>{a.get_page_data({page, 0, nullptr}, false), st->write_position};
    } catch (std::exception& e) {
        err = e.what();
        return -1;
    }
    return 1;
}

void barch::shard::tx_state(bool& in_tx, uint64_t& generation) {
    shared_latch guard(this->latch);
    in_tx = transacted;
    generation = tx_generation;
}

/** the block _save writes after the leaf arena's state, stats included */
void barch::shard::write_stats_block(std::ostream& of, const node_ptr& r, uint64_t sz) const {
    uint32_t w_stats = 1;
    writep(of, w_stats);
    stats_to_stream(of, owned);
    auto at = logical_address(r.logical);
    writep(of, at);
    writep(of, r.is_leaf);
    writep(of, sz);
    write_extra(of);
}

void barch::shard::free_list_bytes(bool nodes, std::string& out, bool& in_tx, uint64_t& generation) {
    shared_latch guard(this->latch);
    in_tx = transacted;
    generation = tx_generation;
    if (in_tx) {
        out = nodes ? begin_free_nodes : begin_free_leaves;
        return;
    }
    std::ostringstream os;
    (nodes ? get_nodes() : get_leaves()).write_free_list(os);
    out = os.str();
}

void barch::shard::stats_bytes(std::string& out, bool& in_tx, uint64_t& generation) {
    shared_latch guard(this->latch);
    in_tx = transacted;
    generation = tx_generation;
    if (in_tx) {
        out = begin_stats;
        return;
    }
    std::ostringstream os;
    write_stats_block(os, root, size.load(std::memory_order_relaxed));
    out = os.str();
}

/*
 * The streamed shard - TODO 418. The file save writes a zero version, the arena,
 * then seeks back to stamp the version once it is whole. A stream can't go back,
 * so it says what it is up front and proves it is whole at the end:
 *
 *   u64 magic "BARCHSTR", u64 format 1, u64 storage_version,
 *   u64 shard number, u64 shard count
 *   the leaf arena:  header, allocator state, the shard's stats block,
 *                    u64 page count, then per page
 *                      u64 page, u32 write position, u32 size, u32 fragmentation,
 *                      u64 ticker, u64 physical, u64 logical,
 *                      and the page's bytes up to its write position
 *   the node arena:  the same, without the stats block
 *   u64 trailer "BARCHEND"
 *
 * The file carries every page whole, 512K each however little is on it; this
 * carries what was written, and the page's tail is rebuilt from the fields.
 */
namespace {
    constexpr uint64_t stream_magic = 0x5254534843524142ull;     // "BARCHSTR"
    constexpr uint64_t stream_trailer = 0x444e454843524142ull;   // "BARCHEND"
    constexpr uint64_t stream_format = 1;
    constexpr size_t stream_header_size = 5 * sizeof(uint64_t);
}

bool barch::shard::read_stream_page(bool nodes, uint64_t generation, size_t page, std::ostream& record,
                                    heap::buffer<uint8_t>& bytes, std::string& err) {
    shared_latch guard(this->latch);
    if (!transacted || tx_generation != generation) {
        err = "the transaction ended during the streaming save";
        return false;
    }
    const arena::hash_arena& a = *(nodes ? begin_nodes : begin_leaves);
    try {
        const auto* st = (const storage*) a.get_page_data({page, page_size - sizeof(storage), nullptr}, false);
        writep(record, (uint64_t) page);
        writep(record, (uint32_t) st->write_position);
        writep(record, (uint32_t) st->size);
        writep(record, (uint32_t) st->fragmentation);
        writep(record, (uint64_t) st->ticker);
        writep(record, (uint64_t) st->physical);
        writep(record, (uint64_t) st->logical);
        bytes = st->write_position
            ? heap::buffer<uint8_t>{a.get_page_data({page, 0, nullptr}, false), st->write_position}
            : heap::buffer<uint8_t>{};
    } catch (std::exception& e) {
        err = e.what();
        return false;
    }
    return true;
}

bool barch::shard::stream_save(uint64_t shard_no, uint64_t shard_count, uint64_t generation,
                               std::ostream& out, std::string& err) {
    std::string prefix[2];
    heap::vector<size_t> pages[2];
    {
        shared_latch guard(this->latch);
        if (!transacted || tx_generation != generation) {
            // not the transaction the save began in - a COMMIT got in between
            err = transacted ? "the transaction ended during the streaming save"
                             : "a streaming save runs inside a transaction";
            return false;
        }
        prefix[0] = begin_prefix_leaves;
        prefix[1] = begin_prefix_nodes;
        // every page in the BEGIN-time table, the way the file has them
        for (int n = 0; n < 2; ++n) {
            const auto& table = (n ? begin_nodes : begin_leaves)->get_arena();
            pages[n].reserve(table.size());
            for (const auto& kv : table)
                pages[n].push_back(kv.first);
            std::sort(pages[n].begin(), pages[n].end());
        }
    }
    writep(out, stream_magic);
    writep(out, stream_format);
    writep(out, (uint64_t) storage_version);
    writep(out, shard_no);
    writep(out, shard_count);
    for (int n = 0; n < 2; ++n) {
        out.write(prefix[n].data(), (std::streamsize) prefix[n].size());
        writep(out, (uint64_t) pages[n].size());
        for (size_t page : pages[n]) {
            std::ostringstream record;
            heap::buffer<uint8_t> bytes;
            if (!read_stream_page(n == 1, generation, page, record, bytes, err))
                return false;
            auto r = record.str();
            out.write(r.data(), (std::streamsize) r.size());
            if (!bytes.empty())
                out.write((const char*) bytes.data(), (std::streamsize) bytes.size());
            if (!out) {
                err = "the streaming save was stopped";
                return false;
            }
        }
    }
    writep(out, stream_trailer);
    out.flush();
    if (!out) {
        err = "the streaming save was stopped";
        return false;
    }
    return true;
}

bool barch::shard::stream_load(const char* data, size_t len, uint64_t shard_no,
                               uint64_t shard_count, std::string& err) {
    // checked before anything is touched: a stream for another shard, another
    // layout, or one cut short leaves this shard as it is
    if (len < stream_header_size + sizeof(uint64_t)) {
        err = "the stream for shard " + std::to_string(shard_no) + " is too short";
        return false;
    }
    uint64_t head[5];
    memcpy(head, data, sizeof(head));
    uint64_t tail = 0;
    memcpy(&tail, data + len - sizeof(tail), sizeof(tail));
    if (head[0] != stream_magic || head[1] != stream_format || head[2] != (uint64_t) storage_version) {
        err = "not a barch shard stream, or one from another version";
        return false;
    }
    if (head[3] != shard_no || head[4] != shard_count) {
        err = "the stream is shard " + std::to_string(head[3]) + " of " + std::to_string(head[4]) +
              ", not shard " + std::to_string(shard_no) + " of " + std::to_string(shard_count);
        return false;
    }
    if (tail != stream_trailer) {
        err = "the stream for shard " + std::to_string(shard_no) + " is cut short";
        return false;
    }

    std::unique_lock guard(save_load_mutex);
    storage_release release(this->shared_from_this());
    if (transacted) {
        err = "a streaming load can't run inside a transaction";
        return false;
    }
    _clear();
    memory_in buf(data + stream_header_size, len - stream_header_size - sizeof(tail));
    std::istream in(&buf);
    auto* t = this;
    logical_address at_root{nullptr};
    bool is_leaf = false;
    auto load_stats_and_root = [&](std::istream& i) {
        uint32_t w_stats = 0;
        readp(i, w_stats);
        if (w_stats != 0)
            stream_to_stats(i, t->owned);
        readp(i, at_root);
        readp(i, is_leaf);
        uint64_t sz = 0;
        readp(i, sz);
        t->size.store(sz, std::memory_order_relaxed);
        read_extra(i);
    };
    bool ok = get_leaves().stream_load(in, load_stats_and_root) &&
              get_nodes().stream_load(in, [](std::istream&) {});
    if (ok) {
        // every byte used, and no more
        in.peek();
        ok = in.eof();
    }
    if (!ok) {
        _clear();
        err = "the stream for shard " + std::to_string(shard_no) + " could not be read; the shard is now empty";
        return false;
    }
    at_root = logical_address{at_root.address(), this};
    root = is_leaf ? node_ptr{at_root} : resolve_read_node(at_root);
    page_modifications::inc_all_tickers();
    load_hash();
    load_bloom();
    return true;
}

bool barch::shard::each_page(bool nodes,
                             const std::function<bool(size_t, const heap::buffer<uint8_t>&)>& f,
                             std::string& err) {
    page_walk w;
    start_page_walk(nodes, w);
    for (size_t page : w.pages) {
        heap::buffer<uint8_t> copy;
        int got = read_walk_page(w, page, copy, err);
        if (got < 0)
            return false;
        if (got == 0)
            continue;
        if (!f(page, copy))
            return false;
    }
    return true;
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
    // the arenas go below, and the views borrow their pages
    begin_leaves.reset();
    begin_nodes.reset();
    begin_free_leaves.clear();
    begin_free_nodes.clear();
    begin_stats.clear();
    begin_prefix_leaves.clear();
    begin_prefix_nodes.clear();
    ++tx_generation;
    transacted = false;
    tomb_stones = 0;
    blocked_sessions.clear();
    mods.store(0, std::memory_order_relaxed);
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
    if (hybrid_active())
        hash_unindex(key);
    bool r = art::insert(this, options, key, value, update, fc);
    if (hybrid_active())
        hash_add_leaf(last_leaf_added);
    return r;
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
            leaf *dl = n.modify_leaf();
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

/*
 * Every insert reaches `insert_unlogged` - the three `insert` overloads and
 * `opt_insert` all come here - so this wrapper is the one place a logical write
 * is recorded, and it records each one exactly once. TODO 355.
 *
 * What deliberately does not pass through here, and so is not logged:
 *   - the compression pass, which rewrites a value in place with
 *     `set_shorter_value`. The value it stores is the compressed form of one
 *     already recorded, so logging it would replay compressed bytes as a value;
 *   - eviction, which erases through `art::erase` rather than `remove`. An
 *     eviction is this server deciding it is short of memory, not a change to
 *     the data - though it does mean a replay brings evicted keys back.
 *
 * The key recorded is the one the caller passed, before `s_filter_key`, so a
 * replay goes through the identical path and arrives at the same stored key.
 */
bool barch::shard::opt_rpc_insert(const key_options& options, value_type unfiltered_key,
                                  value_type value, bool update, const NodeResult &fc) {
    const bool added = insert_unlogged(options, unfiltered_key, value, update, fc);
    /*
     * `added` is not "the write happened" - it is "a new key appeared", because
     * insert_unlogged returns whether the key count grew. An overwrite leaves
     * the count alone and returns false, so recording on it alone logs new keys
     * and silently drops every update. That is the worst shape of wrong for a
     * log: it looks like it works.
     *
     * The intent says what happened instead. With `update` set the write always
     * takes effect - either the leaf is replaced in place or `art::insert`
     * replaces it - so there is always something to record. Without it the write
     * only lands when the key was absent, which is exactly what `added` says.
     */
    if (change_log && (update || added)) {
        change_log->append_set(space_name(),
                               std::string(unfiltered_key.chars(), unfiltered_key.size),
                               std::string((const char*) value.bytes, value.size),
                               (int64_t) options.get_expiry(), options.flags,
                               (uint32_t) get_shard_number(),
                               (uint32_t) space_shards.load(std::memory_order_relaxed));
    }
    // the same writes the change log records, for the same reason: the ones that
    // took effect - TODO 422
    if (update || added) {
        if (auto* ix = index_to.load(std::memory_order_acquire))
            ix->changed(unfiltered_key, false);
    }
    return added;
}

bool barch::shard::insert_unlogged(const key_options& options, value_type unfiltered_key, value_type value, bool update, const NodeResult &fc) {
    if (statistics::logical_allocated > get_max_module_memory()) {
        ++statistics::oom_avoided_inserts;
        throw_exception<std::runtime_error>("not enough memory");
    }

    std::string tk;
    value_type key = s_filter_key(tk,unfiltered_key);
    add_bloom(key);
    cancel_flight(key);

    // tree size when ordered (hybrid included), hash size when unordered.
    // size+h.size() double-counts hybrid and makes every update look like a new key.
    size_t before = opt_ordered_keys ? size.load(std::memory_order_relaxed) : h.size();
    if (options.is_hashed() && !hybrid_active()) {
        hash_insert(options, key, value, update, fc);
    }else {
        bool inplace = false;
        if (hybrid_active() && update) {
            auto i = h.find(key_query{key});
            if (i != h.end()) {
                node_ptr n = i->node(this);
                if (!n.null() && n.is_leaf) {
                    leaf *dl = n.modify_leaf();
                    if (dl && art::is_leaf_direct_replacement(dl, value, options)) {
                        fc(n);
                        dl->set_value(value);
                        dl->set_expiry(options.is_keep_ttl() ? dl->expiry_ms() : options.get_expiry());
                        options.is_volatile() ? dl->set_volatile() : dl->unset_volatile();
                        dl->set_compressed(options.is_compressed());
                        last_leaf_added = n;
                        ++statistics::keys_replaced;
                        ++statistics::set_ops;
                        inplace = true;
                    } else {
                        // drop the index while the old leaf is still alive;
                        // art::insert will free it
                        h.erase(i);
                    }
                }
            }
        }
        if (!inplace) {
            art::insert(this, options, key, value, update, fc);
            if (hybrid_active())
                hash_add_leaf(last_leaf_added);
        }
    }
    call_unblock(std::string(key.chars(), key.size));
    return (opt_ordered_keys ? size.load(std::memory_order_relaxed) : h.size()) > before;
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
                if (tomb_stones.load(std::memory_order_relaxed) == 0) {
                    throw_exception<std::runtime_error>("invalid tombstone count");
                }
                tomb_stones.fetch_sub(1, std::memory_order_relaxed);
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
    node_ptr indexed;
    hashed_key saved;
    bool had = false;
    if (hybrid_active()) {
        auto hi = h.find(key_query{key});
        if (hi != h.end()) {
            saved = *hi;
            had = true;
            h.erase(hi);
        }
    }
    auto art_updater = [&](const node_ptr &leaf) {
        auto value = repl_updateresult(leaf);
        indexed = value;
        return value;
    };
    bool r = barch::update(this, key, art_updater);
    if (hybrid_active()) {
        if (r && !indexed.null())
            hash_add_leaf(indexed);
        else if (!r && had)
            h.insert_unique(saved);
    }
    call_unblock(std::string(key.chars(), key.size));
    return r;
}
bool barch::shard::evict(const leaf* l) {
    if (l->deleted()) return false;
    size_t before = size.load(std::memory_order_relaxed);
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
    if (hybrid_active())
        hash_unindex(l->get_key());
    art::erase(this, l->get_key(), [](const art::node_ptr &){});
    if (size.load(std::memory_order_relaxed) < before) {
        ++statistics::keys_evicted;
    }
    --statistics::delete_ops; // were not counting these deletes
    return size.load(std::memory_order_relaxed) < before;
}
bool barch::shard::evict(value_type unfiltered_key) {
    size_t before = size.load(std::memory_order_relaxed);

    std::string kbuf;
    auto key = s_filter_key(kbuf, unfiltered_key);
    node_ptr old = from_unordered_set(key);
    if (!old.null()) {
        auto n = old;
        leaf *dl = n.modify_leaf();
        if (dl->is_hashed()) {
            erase_tomb(dl);
            h.erase(key_query{key});
            n.free_from_storage();
            return true;
        }
    }
    if (hybrid_active())
        hash_unindex(key);
    --statistics::delete_ops; // were not counting these deletes
    art::erase(this, key, [](const art::node_ptr &){});
    return size.load(std::memory_order_relaxed) < before;

}
bool barch::shard::tree_remove(value_type key, const NodeResult &fc) {
    auto sbef = size.load(std::memory_order_relaxed);
    ++deletes;
    if (hybrid_active())
        hash_unindex(key);
    art::erase(this, key, fc);
    return sbef < size.load(std::memory_order_relaxed);
}


/** the other half of TODO 355: one place, one record, on success only */
bool barch::shard::remove(value_type unfiltered_key, const NodeResult &fc) {
    const bool ok = remove_unlogged(unfiltered_key, fc);
    if (ok && change_log) {
        change_log->append_erase(space_name(),
                                 std::string(unfiltered_key.chars(), unfiltered_key.size),
                                 (uint32_t) get_shard_number(),
                                 (uint32_t) space_shards.load(std::memory_order_relaxed));
    }
    if (ok) {
        if (auto* ix = index_to.load(std::memory_order_acquire))
            ix->changed(unfiltered_key, true);
    }
    return ok;
}

bool barch::shard::remove_unlogged(value_type unfiltered_key, const NodeResult &fc) {
    ++deletes;
    size_t before = size.load(std::memory_order_relaxed);
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
    if (!old.null() && old.cl()->is_hashed()) {
        if (auto src = sources()) {
            // check if exists and insert tombstone else continue with normal erase
            auto dep = src->search(key);
            if (!dep.null()) {
                fc(old);
                bool r = this->hash_insert({},key,{},true,[](node_ptr){});
                if (r) {
                    last_leaf_added.l()->set_tomb();
                    tomb_stones.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                return false;
            }
        }
        auto n = old;
        leaf *dl = n.modify_leaf();
        fc(n);
        erase_tomb(dl);
        h.erase(key_query{key});
        n.free_from_storage();

        return true;
    }
    if (hybrid_active())
        hash_unindex(key);
    if (auto src = sources()) {
        // check if exists and insert tombstone else continue with normal erase
        auto dep = src->search(key);
        if (!dep.null()) {
            fc(dep);
            tomb_stones.fetch_add(1, std::memory_order_relaxed);
            tree_insert({},key,{},true,[](node_ptr){});
            if (!last_leaf_added.null())
                last_leaf_added.l()->set_tomb();

            return true;
        }
    } // else continue

    art::erase(this, key, fc);
    return size.load(std::memory_order_relaxed) < before;
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
    return load_dependencies();
}
uint64_t barch::shard::bytes_in_free_list() {
    return get_nodes().get_bytes_in_free_list() + get_leaves().get_bytes_in_free_list();
}
barch::shard_ptr barch::shard::load_dependencies() const {
#if BARCH_HAS_ATOMIC_SHARED_PTR
    return dependencies.load(std::memory_order_acquire);
#else
    return std::atomic_load_explicit(&dependencies, std::memory_order_acquire);
#endif
}

/* publish, then walk the chain from a local copy - see TODO 312 */
void barch::shard::set_dependencies(const shard_ptr& source) {
#if BARCH_HAS_ATOMIC_SHARED_PTR
    dependencies.store(source, std::memory_order_release);
#else
    std::atomic_store_explicit(&dependencies, shard_ptr(source), std::memory_order_release);
#endif
}

void barch::shard::depends(const std::shared_ptr<abstract_shard> & source) {

    set_dependencies(source);
    auto current = this->shared_from_this();
    auto test = source;
    while (test && test != current) {
        test = test->sources();
    }
    if (test == current) {
        set_dependencies(nullptr);
        throw_exception<std::invalid_argument>("cannot have cyclic dependencies");
    }
}

void barch::shard::release(const std::shared_ptr<abstract_shard> & unused(source)) {
    set_dependencies(nullptr);
}

art::node_ptr barch::shard::lower_bound(art::value_type key) {
    return art::lower_bound(this, key);
}

art::node_ptr barch::shard::lower_bound(art::trace_list &trace, art::value_type key) {
    return art::lower_bound(trace, this, key);
}

void barch::shard::glob(const keys_spec &spec, value_type pattern, bool value, const std::function<bool(const leaf &)> &cb,
                        const glob_page_list *only, glob_page_list *hits)  {

    if (auto src = sources()) {
        // pull sources have their own page ids. a list from this shard
        // must not constrain or collect theirs.
        src->glob(spec, pattern, value, cb);
    }
    art::glob(this, spec, pattern, value, cb, only, hits);
}

art::node_ptr barch::shard::local_leaf(value_type unfiltered_key) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    if (!opt_ordered_keys) {
        return from_unordered_set(key);
    }
    if (hybrid_active()) {
        auto n = from_unordered_set(key);
        if (!n.null()) return n;
    }
    return art::search(this, key);
}

bool barch::shard::setBufferAt(value_type unfiltered_key, value_type buf, size_t offset) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    const size_t need = offset + buf.size;
    if (key.size + need > (size_t) maximum_allocation_size)
        return false;

    node_ptr n = local_leaf(key);
    const leaf* cl = (!n.null() && n.is_leaf) ? n.const_leaf() : nullptr;
    const bool tomb = cl && cl->is_tomb() && !cl->expired();
    const bool live = cl && !cl->is_tomb() && !cl->expired();

    if (live && !cl->is_compressed() && cl->val_len() >= need) {
        if (buf.size)
            memcpy(n.l()->val() + offset, buf.bytes, buf.size);
        last_leaf_added = n;
        ++statistics::keys_replaced;
        ++statistics::set_ops;
        call_unblock(std::string(key.chars(), key.size));
        return true;
    }

    if (need == 0 && !live && !tomb)
        return true;

    thread_local heap::vector<uint8_t> s;
    s.clear();
    if (live) {
        auto ov = cl->get_value();
        if (cl->is_compressed())
            ov = dictionary::decompress(this->name, ov);
        s.insert(s.end(), ov.begin(), ov.end());
    }
    if (need > s.size())
        s.resize(need, 0);
    if (buf.size)
        memcpy(s.data() + offset, buf.bytes, buf.size);

    art::key_options opts;
    if (live)
        opts = cl->options();
    else
        opts.set_hashed(!opt_ordered_keys);
    opts.set_compressed(false);
    value_type written{s.data(), s.size()};
    if (!fits_in_leaf(key.size, written.size))
        return false;
    opt_insert(opts, key, written, true, [](const node_ptr&) {});
    return true;
}

std::pair<art::value_type, bool> barch::shard::getBufferAt(value_type unfiltered_key, size_t offset) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    node_ptr n = local_leaf(key);
    if (n.null() || !n.is_leaf)
        return {{}, false};
    auto cl = n.const_leaf();
    if (cl->is_tomb() || cl->expired() || cl->is_compressed())
        return {{}, false};
    auto v = cl->get_value();
    if (offset > v.size)
        return {{}, false};
    return {{v.bytes + offset, v.size - offset}, true};
}

void barch::shard::insert_cached_miss(value_type unfiltered_key, uint64_t ttl_ms, bool hashed) {
    std::string kbuf;
    value_type key = s_filter_key(kbuf, unfiltered_key);
    add_bloom(key);

    node_ptr existing = hashed ? from_unordered_set(key) : (hybrid_active() ? from_unordered_set(key) : art::search(this, key));
    if (existing.null() && hybrid_active() && !hashed)
        existing = art::search(this, key);
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
        tree_insert(opts, key, empty, true, [](const node_ptr&) {});
    node_ptr n = hashed ? from_unordered_set(key) : (hybrid_active() ? from_unordered_set(key) : art::search(this, key));
    if (n.null() && !hashed)
        n = last_leaf_added;
    if (n.null()) {
        call_unblock(std::string(key.chars(), key.size));
        return;
    }
    // art::insert may have erase_tomb'd an in-place rewrite, so count from
    // the leaf as it is now rather than from the one we found at the start
    const bool already_tomb = n.cl()->is_tomb();
    n.l()->set_tomb();
    if (!already_tomb)
        tomb_stones.fetch_add(1, std::memory_order_relaxed);
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
    fl.settle([&] {
        ++fl.generation;
        fl.state = foreign_flight::state::cancelled;
    });
}

void barch::shard::fail_foreign(const char* msg, heap::vector<abstract_session_ptr>& sessions) {
    for (auto& [k, fl] : flights) {
        fl->settle([&] {
            fl->state = foreign_flight::state::failed;
            fl->error = msg;
        });
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
    if (hybrid_active()) {
        auto n = from_unordered_set(key);
        if (!n.null()) return true;
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
    if (hybrid_active()) {
        auto n = from_unordered_set(key);
        if (!n.null()) {
            if (n.cl()->is_tomb()) {
                return nullptr;
            }
            return n;
        }
    }

    auto r = art::search(this, key);
    if (r.null()) {
        if (auto src = sources()) {
            r = src->search(key); // this can recurse down
            if (!r.null()) {
                return r;
            }
        }
        // this used to null last_leaf_added and return it, which returns the same
        // nullptr but writes a shard member from the read path - two GETs missing
        // at once raced on it, and it also threw away the leaf an insert had left
        // there for get_last_leaf_added(). See TODO 214.
        // TODO: retrieve if pull is enabled
        return nullptr;
    }
    if (r.cl()->is_tomb()) {
        return nullptr;
    }
    // check if r.cl()->is_tombstone() and return nullptr
    return r;
}
art::node_ptr barch::shard::tree_minimum() const {
    auto src = load_dependencies();
    auto dmin = src ? src->tree_minimum() : nullptr;
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
    auto src = load_dependencies();
    auto dmax = src ? src->tree_maximum() : nullptr;
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
    merge(sources(), options);
}
void barch::shard::merge(const shard_ptr& to, merge_options options) {
    if (!to) return;
    auto &lc = get_leaves();
    // two spaces, two dictionaries: decompress with the source's and compress
    // with the destination's, or the destination stores bytes it cannot read
    const std::string from_space = this->name;
    const std::string to_space = to->space_name();

    lc.iterate_pages([&to, options, &from_space, &to_space](size_t s, size_t , auto& data) {
        page_iterator(data, s, [&](const leaf *l, uint32_t ) {
            if (l->is_tomb()) {
                to->remove(l->get_key());
                return true;
            }
            auto opts = l->options();
            auto v = l->get_value();
            if (options.is_compressed() && !opts.is_compressed()) {
                auto vcomp = dictionary::compress(to_space, v);
                if (!vcomp.empty()) {
                    v = vcomp;
                    opts.set_compressed(true);
                }
            }else if (options.is_decompress() && opts.is_compressed()) {
                auto vdec = dictionary::decompress(from_space, v);
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
                tomb_stones.fetch_add(1, std::memory_order_relaxed);
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
    if (hybrid_active())
        rebuild_hybrid_index();
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
    // get_size() reads size and tomb_stones, which writers update under the
    // latch. This is only deciding whether there is anything to defragment, so
    // the answer does not have to stay true - it does have to be read without
    // racing the writer. Scoped tight: run_defrag takes its own unique latch
    // further down and must not be holding a shared one when it does.
    // See TODO 213.
    {
        shared_latch guard(this->latch);
        if (this->get_size() == 0) return;
    }

    auto &lc = get_leaves();
    constexpr auto defrag_lock_to = std::chrono::milliseconds(100);

    {
        try_unique_latch releaser(this->latch, defrag_lock_to);
        if (!releaser)
            return;
        // shrinking remaps the committed pages, which a transaction's BEGIN-time
        // view and every untouched-page read point straight into - TODO 416
        if (transacted)
            return;
        this->shrink();
    }

    // fragmentation_ratio() reads `allocated` and the emancipated counter, and
    // every insert writes `allocated` under the latch - so this is the same
    // unlocked statistic read as the get_size() one above, a few lines further
    // down the same function. TSan caught it on CI against a SET. Scoped tight
    // for the same reason: a unique latch is taken below and a shared one must
    // not still be held. See TODO 228.
    float logical_frag;
    {
        shared_latch guard(this->latch);
        logical_frag = lc.fragmentation_ratio();
    }
    try {

        if (logical_frag > 0.3) //get_min_fragmentation_ratio())
        {
            heap::vector<size_t> fl;
            {
                try_unique_latch releaser(this->latch, defrag_lock_to);
                if (!releaser)
                    return;
                fl = lc.create_fragmentation_list(get_max_defrag_page_count());
            }

            for (auto p: fl) {
                try_unique_latch releaser(this->latch, defrag_lock_to);
                if (!releaser)
                    return;
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
/**
 * Keys no eviction policy may take, whatever it says.
 *
 * A stored function is a command, not data: evicting one deletes a command under
 * memory pressure, and since a session keeps whatever it compiled, the connections
 * that already ran it would carry on while new ones met "unknown command". See
 * TODO 98.
 *
 * This sits at the policy level rather than in `shard::evict`, which would be the
 * obvious single place and is the wrong one: `erase_page` calls evict to lift a key
 * out of a fragmented page before adding it back, and aborts if the key does not go.
 * Defragmenting a function is fine and has to keep working - it is eviction that must
 * not happen.
 */
static bool may_evict(const barch::leaf *l) {
    auto k = l->get_key();
    if (k.size && k.bytes[0] == art::tfunction)
        return false;
    /*
     * A stored file is four kinds of key - the name record, the inode, one key per
     * chunk, and the space's `fs:layout` marker - and this sweep sees one leaf at a
     * time with nothing telling it they belong together. Every way it could pick is
     * wrong, and two of them are worse than losing data:
     *
     *   - a chunk out of a live file leaves the name and inode still promising it,
     *     so every read of that file fails from then on with "the file changed
     *     underneath", which is a message about a rewrite and not about this;
     *   - the name record strands the inode and every chunk with nothing able to
     *     name them, so the smallest key of the set is freed and all the big ones
     *     stay resident - under memory pressure it makes memory pressure worse.
     *
     * So none of them go from here. Whole file eviction is `barch::fs::evict_some`,
     * called from the space maintenance thread, which can stage the name, the inode
     * and every chunk into one commit. See TODO 302.
     *
     * The test is on the bytes after the lead type byte rather than on a decoded
     * key, because it has to hold for both shapes the same path can take: a plain
     * string key, and the composite a path containing the space's separator becomes.
     * `fs:` leads either way - the separator can only appear further along.
     */
    if (k.size > 3 && memcmp(k.bytes + 1, "fs:", 3) == 0)
        return false;
    /*
     * A graph node is the same shape of problem with one more table: node and
     * edge records plus the FS inode and chunks a leaf hangs off. Taking one
     * leaf strands the rest the way a half evicted file does, so none of them
     * go from here either. Whole node eviction is future work - see TODO 382;
     * until it exists a graph under memory pressure is not shrinkable this way.
     */
    if (k.size > 6 && memcmp(k.bytes + 1, "graph:", 6) == 0)
        return false;
    return true;
}

void abstract_eviction(barch::shard *t,
                       const std::function<bool(const barch::leaf *l)> &predicate,
                       const std::function<std::pair<heap::buffer<uint8_t>, size_t> ()> &src) {
    auto fc = [](const art::node_ptr & unused(n)) -> void {
    };
    auto updater = [predicate,fc,t](const barch::leaf *l) {
        if (!l->deleted() && may_evict(l) && predicate(l)) {
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
        }else if (may_evict(l)) {
            t->evict(l); // will get cleaned up by defrag
        }
    });
}

/*
 * Background compression, strategy A of TODO 300: one key at a time, chosen by
 * the LRU clock, compressed while readers carry on.
 *
 * Nothing is ever compressed implicitly. A value becomes compressed because
 * this pass picked it after the clock said nobody had read it, or because
 * someone asked for that key by name. A write never compresses what it writes.
 *
 * Mutually exclusive with eviction. A space either evicts its cold keys or
 * compresses them, never both, and the two share the same LRU bits to decide
 * what cold means - see apply_lru_options, which is what turns the stamping on
 * for a space that compresses but does not evict. Doing both in an order
 * (compress first, evict what is still cold afterwards) needs a second bit to
 * tell "cold and compressed" from "cold" and is deliberately not here yet.
 *
 * Why the value cannot be replaced in place. A leaf records its own value
 * length, and byte_size() feeds next_leaf(), which is how the page walker steps
 * from one leaf to the next. Shrink val_len where it sits and every later leaf
 * on that page is read at the wrong offset. So a compressed value has to be
 * reallocated, which insert already does for any value of a different length -
 * is_leaf_direct_replacement (art.cpp:1175) only takes the in-place path when
 * the lengths match exactly. That leaves a hole where the old leaf was, which
 * defrag reclaims. Strategy B in TODO 300 - compress a whole quiet page at once
 * so there is no hole to reclaim - is the alternative this is meant to be
 * measured against.
 */
static bool may_compress(const barch::leaf *l) {
    // hash values are stored as given; the compression path is for plain keys.
    // hash_api.cpp:647 says so and the read paths there do not decompress.
    if (l->is_hashed()) return false;
    if (l->is_compressed()) return false;
    if (l->deleted() || l->is_tomb() || l->expired()) return false;
    // below this size zstd is asked not to bother, and the dictionary
    // compressor would answer empty anyway
    if (l->val_len() < barch::get_min_compressed_size()) return false;
    return true;
}

/* whether this space compresses rather than evicts. both need the LRU bits. */
bool barch::shard::compresses_cold_keys() const {
    // the space's own switch, not the server one - `<space>.compression`
    // overrides it, same as .ordered and .hybrid do
    if (!opt_compression.load(std::memory_order_relaxed)) return false;
    return !(opt_evict_all_keys_lru || opt_evict_volatile_keys_lru
          || opt_evict_all_keys_lfu || opt_evict_volatile_keys_lfu
          || opt_evict_all_keys_random || opt_evict_volatile_keys_random
          || opt_evict_volatile_ttl);
}

void run_compress_cold_keys(barch::shard *t) {
    if (!t->compresses_cold_keys()) return;

    // The rebalancer's pattern: bounded by how much lock time a pass may take
    // rather than by how much work is left, so what one pass does not finish
    // the next one continues. Both numbers are guesses and want tuning against
    // a real workload, same caveat as TODO 34.
    constexpr size_t budget = 32;
    constexpr auto lock_to = std::chrono::milliseconds(50);

    heap::vector<std::string> cold;
    /*
     * The clock tick, and it has to come first. Clearing the bit on everything
     * that was read, and collecting what was already clear, are one decision
     * made against one snapshot - do it the other way round and a key read
     * between the two steps gets compressed anyway. This walk only flips bits,
     * so the write latch is held for a page walk and no zstd.
     */
    {
        try_unique_latch releaser(t->latch, lock_to);
        if (!releaser) return; // busy with user traffic, come back next tick
        auto &lc = t->get_leaves();
        auto page_num = lc.max_allocated_page_num();
        if (!page_num) return;
        std::uniform_int_distribution<size_t> dist(1, page_num);
        size_t page = dist(gen);
        if (!lc.is_page_allocated(page)) return;
        auto buf = lc.get_page_buffer(page);
        if (!buf.second) return;
        page_iterator(buf.first, buf.second, [&](const barch::leaf *l, uint32_t pos) {
            if (l->is_lru()) {
                // read since the last pass: second chance, not a candidate
                logical_address at{page, pos, &t->get_ap()};
                if (auto *m = t->get_leaves().modify<barch::leaf>(at))
                    m->unset_lru();
                return true;
            }
            if (may_compress(l)) {
                auto k = l->get_key();
                cold.emplace_back(k.chars(), k.size);
            }
            return cold.size() < budget;
        });
    }

    /*
     * Now the expensive half, with the write latch dropped. Upgradable lets
     * other readers carry on while zstd runs and keeps writers out, so the
     * value read here cannot be replaced underneath the compressor - that is
     * exactly what debuggable_server_lock.h:793 promises, and what
     * test_writer_blocked_during_upgradable in locktest.cpp holds it to. The
     * write latch is then taken only for as long as it takes to put the shorter
     * value back, and not at all when there was no gain.
     */
    auto fc = [](const art::node_ptr &) -> void {};
    heap::vector<uint8_t> held;
    for (const auto& ks : cold) {
        try_upgradable_latch guard(t->latch, lock_to);
        if (!guard) return;

        value_type key{ks.data(), ks.size()};
        auto n = t->local_leaf(key);
        if (n.null() || !n.is_leaf) continue;
        // peek_leaf, not const_leaf: the compressor looking at a key must not
        // mark it as read, or the next pass would find it hot and never
        // compress anything. See DONE 297.
        const barch::leaf *cl = n.peek_leaf();
        if (!may_compress(cl) || cl->is_lru()) continue;

        auto v = cl->get_value();
        auto c = dictionary::compress(t->name, v);
        // empty means the dictionary is still training - the call fed it a
        // sample, which is how it gets ready - or that it did not shrink
        if (c.empty() || c.size >= v.size) continue;

        // the compressor hands back its own reusable buffer, and the insert
        // below runs after the tree has moved things around, so take a copy
        held.assign(c.bytes, c.bytes + c.size);
        const auto expiry = cl->expiry_ms();
        const bool vol = cl->is_volatile();
        const auto was = (uint64_t) v.size;

        if (!guard.upgrade(lock_to)) return;

        /*
         * Shrink in place rather than reallocating - TODO 308. The leaf keeps
         * its address, so the tree and the hash index do not have to be told
         * anything, and only the tail is handed back instead of the whole leaf
         * becoming a hole. That is what stops compression driving defrag: the
         * reallocating version took defrag passes from 1,672 to 63,000 on a
         * workload with writes in it.
         *
         * Order matters. The leaf is shortened first so `byte_size()` is the new
         * one, then the gap is filled so the page still walks, then the
         * allocator is told. The leaf is re-found under the write latch rather
         * than trusting a pointer taken before the upgrade.
         */
        auto wn = t->local_leaf(key);
        if (wn.null() || !wn.is_leaf) continue;
        barch::leaf *wl = wn.modify_leaf();
        const size_t old_size = wl->byte_size();
        const size_t new_size = old_size - (was - held.size());
        const size_t old_total = alloc_pad(old_size) + test_memory;
        const size_t new_total = alloc_pad(new_size) + test_memory;
        const size_t gap = old_total - new_total;
        // a gap too small, or of a size no leaf header can describe, means this
        // value stays as it is - there is nowhere to put a filler
        // too small to hold even one filler means there is nowhere to record
        // the gap, so this value stays as it is
        if (gap < sizeof(barch::leaf) + 1 + test_memory) continue;

        wl->set_shorter_value(value_type{held.data(), held.size()}, true);
        if (wl->byte_size() != new_size) {
            abort_with("shrunk leaf is not the size it was meant to be");
        }
        if (!barch::leaf::fill_gap((uint8_t *) wl + new_total, gap)) {
            abort_with("could not fill the gap left by compressing in place");
        }
        t->get_leaves().shrink(wn.logical, old_size, new_size);
        statistics::value_bytes_compressed += was - held.size();
    }
}

uint64_t barch::shard::get_modifications() const {
    return deletes + inserts;
}

void barch::shard::start_maintain() {
    this->mods.store(get_modifications(), std::memory_order_relaxed);
    this->start_save_ns.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
}
void run_compress_cold_keys(barch::shard *t);

void barch::shard::maintenance() {
    try {
        run_sweep_lru_keys(this);
        run_evict_all_keys_lfu(this);
        run_evict_all_keys_random(this);
        run_evict_volatile_keys_lru(this);
        run_evict_volatile_keys_lfu(this);
        run_evict_volatile_expired_keys(this);
        run_sweep_expired_keys(this);
        // compression is the other half of the LRU bits - a space either evicts
        // its cold keys or compresses them. See TODO 300.
        run_compress_cold_keys(this);

        // defrag will get rid of memory used by evicted keys if memory is pressured - if its configured
        if (this->opt_active_defrag) {
            run_defrag(); // periodic
        }
        // Take and clear in one step. This used to test the counter unlocked,
        // then take the write latch and clear it, while readers were still
        // incrementing under the shared latch - so the test raced the readers
        // and any increment landing between the read and the clear was lost.
        // exchange needs no latch at all. See TODO 213.
        const uint64_t found = saf_keys_found.exchange(0, std::memory_order_relaxed);
        const uint64_t ops = saf_get_ops.exchange(0, std::memory_order_relaxed);
        if (found || ops) {
            statistics::keys_found += found;
            statistics::get_ops += ops;
        }
        auto curr_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::high_resolution_clock::now().time_since_epoch())
                           .count();
        auto last_ns = start_save_ns.load(std::memory_order_relaxed);
        auto last_mods = mods.load(std::memory_order_relaxed);
        if ((uint64_t) ((curr_ns - last_ns) / 1000000) > get_save_interval()
            || get_modifications() - last_mods > get_max_modifications_before_save()
        ) {
            if (get_modifications() - last_mods > 0) {

                //log({"saving",get_leaves().get_name(), "modifications",get_modifications(),"time",millis(currtime, start_save_time)});
                this->save(with_stats);
                /*
                 * No change log checkpoint here, on purpose - TODO 355.
                 *
                 * A checkpoint says everything before it is in the shard file,
                 * and this saves one shard. Writing one here would claim a whole
                 * space was saved when the other sixteen shards had not been,
                 * and a replay believing it would skip records that are the only
                 * copy of what they describe. The checkpoint belongs where the
                 * whole space is saved at once, which is `save()` in
                 * keyspace_api.cpp.
                 */

            }
        }
    }catch (std::exception& e) {
        barch::err({e.what()});
    }
}
