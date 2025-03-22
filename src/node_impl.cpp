#include <cstdlib>
#include <cstring>
#include <atomic>

#include "art.h"
#include "statistics.h"
#include "nodes.h"
#include "node_impl.h"

#include <algorithm>

art::node_ptr art::make_leaf(value_type key, value_type v, leaf::ExpiryType ttl, bool is_volatile) {
    unsigned val_len = v.size;
    unsigned key_len = key.length();
    unsigned ttl_size = ttl > 0 ? sizeof(ttl):0;
    size_t leaf_size = sizeof(leaf) + key_len + ttl_size + 1 + val_len;
    // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
    auto logical = art::get_leaf_compression().new_address(leaf_size);
    auto *l = new(get_leaf_compression().read<leaf>(logical)) leaf(key_len, val_len, ttl, is_volatile);
    ++statistics::leaf_nodes;
    statistics::addressable_bytes_alloc += leaf_size;
    l->set_key(key);
    l->set_value(v);
    return logical;
}


void art::free_leaf_node(art::leaf* l, compressed_address logical){
    if(l == nullptr) return;
    l->set_deleted();
    art::get_leaf_compression().free(logical, l->byte_size());
    --statistics::leaf_nodes;
    statistics::addressable_bytes_alloc -= (l->byte_size());
}

void art::free_leaf_node(art::node_ptr n)
{
    free_leaf_node(n.l(),n.logical);
}

void art::free_node(art::node_ptr n){
    n.free_from_storage();
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
template<typename Type4, typename Type8>
static art::node* make_node(art::node_ptr_storage& ptr, compressed_address a, art::node_data* node)
{
    if (node->pointer_size == 4)
    {
        return ptr.emplace<Type4>(a, node);
    }else if (node->pointer_size == 8)
    {
        return  ptr.emplace<Type8>(a, node);
    }
    abort();
}
art::node_ptr art::resolve_read_node(compressed_address address)
{
    auto* node= art::get_node_compression().read<art::node_data>(address);
    art::node_ptr_storage ptr;
    switch (node->type)
    {
    case art::node_4:
        return  make_node<art::node4_4,art::node4_8>(ptr, address, node);
    case art::node_16:
        return make_node<art::node16_4,art::node16_8>(ptr, address, node);
    case art::node_48:
        return make_node<art::node48_4,art::node48_8>(ptr, address, node);
    case art::node_256:
        return make_node<art::node256_4,art::node256_8>(ptr, address, node);
    default:
        abort();
    }
}
art::node_ptr art::resolve_write_node(compressed_address address)
{
    auto* node= get_node_compression().modify<node_data>(address);
    node_ptr_storage ptr;
    switch (node->type)
    {
    case art::node_4:
        return  make_node<node4_4,node4_8>(ptr, address, node);
    case art::node_16:
        return make_node<node16_4,node16_8>(ptr, address, node);
    case art::node_48:
        return make_node<node48_4,node48_8>(ptr, address, node);
    case art::node_256:
        return make_node<node256_4,node256_8>(ptr, address, node);
    default:
        throw std::runtime_error("Unknown node type");
    }
}
art::node_ptr art::alloc_node_ptr(unsigned nt, const art::children_t& c)
{
    art::node_ptr ref;

    art::node_ptr_storage ptr;
    switch (nt)
    {
    case art::node_4:
        return ptr.emplace<art::node4_4>()->create().expand_pointers(ref, c);
    case art::node_16:
        return ptr.emplace<art::node16_4>()->create().expand_pointers(ref, c);
    case art::node_48:
        return ptr.emplace<art::node48_4>()->create().expand_pointers(ref, c);
    case art::node_256:
        return ptr.emplace<art::node256_4>()->create().expand_pointers(ref, c);
    default:
        throw std::runtime_error("Unknown node type");
    }
}
art::node_ptr art::alloc_8_node_ptr(unsigned nt)
{

    art::node_ptr_storage ptr;
    switch (nt)
    {
    case art::node_4:
        return ptr.emplace<art::node4_8>();
    case art::node_16:
        return ptr.emplace<art::node16_8>();
    case art::node_48:
        return ptr.emplace<art::node48_8>();
    case art::node_256:
        return ptr.emplace<art::node256_8>();
    default:
        throw std::runtime_error("Unknown node type");
    }
}



/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */

unsigned art::node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) {
    unsigned max_cmp = std::min<int>(std::min<int>(data().partial_len, max_prefix_llength), (int)key_len - (int)depth);
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (data().partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

art::tree::~tree()
{
    mexit = true;
    if(tmaintain.joinable())
        tmaintain.join();
}
#include "configuration.h"
#include <functional>

void page_iterator (const heap::buffer<uint8_t>& page, unsigned size, std::function<void(const art::leaf* )> cb)
{
    if(!size) return;

    auto e = page.begin() + size;
    size_t deleted = 0;
    for (auto i = page.begin();i != e;)
    {
        const art::leaf *l = (art::leaf*)i;
        if (l->key_len > page.byte_size())
        {
            abort();
        }
        if (l->deleted())
        {
            deleted++;
        }else
        {
            cb(l);
        }
        i += (l->byte_size() + test_memory);
    }
}

/**
 * "active" defragmentation: takes all the fragmented pages and removes the not deleted keys on those
 * then adds them back again
 * this function isn't supposed to run a lot
 */
void art::tree::run_defrag()
{
    if(!art::has_leaf_compression()) return;
    auto fc = [](const art::node_ptr& unused(n)) -> void{};
    auto &lc = art::get_leaf_compression();


    try
    {
        if(lc.fragmentation_ratio() > -1)  //art::get_min_fragmentation_ratio())
        {
            auto fl = lc.create_fragmentation_list(art::get_max_defrag_page_count());
            art::key_spec options;
            for(auto p : fl)
            {
                //compressed_release releaser;
                write_lock lock(get_lock());
                auto page = lc.get_page_buffer(p);

                page_iterator(page.first, page.second,[&fc,this](const art::leaf* l)
                {
                    size_t c1 = this->size;
                    art_delete(this, l->get_key(),fc);
                    if (c1-1 != this->size)
                    {
                        abort();
                    }
                });

                page_iterator(page.first, page.second,[&fc,&options,this](const art::leaf* l)
                {
                    size_t c1 = this->size;
                    options.ttl = l->ttl();
                    art_insert(this, options, l->get_key(), l->get_value(),fc);
                    if (c1+1 != this->size)
                    {
                        abort();
                    }
                });
                ++statistics::pages_defragged;
                //if (lc.fragmentation_ratio() < 1.0)
                //return;
            }


        }
    }catch (std::exception& )
    {
        ++statistics::exceptions_raised;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(40)); // chill a little we've worked hard
}
void abstract_eviction(art::tree* t,
    const std::function<bool(const art::leaf* l)>& predicate,
    const std::function<std::pair<heap::buffer<uint8_t>,size_t> ()>& src)
{
    if (heap::get_physical_memory_ratio() < 0.99)
        if (heap::allocated < art::get_max_module_memory()) return;
    auto fc = [](const art::node_ptr& unused(n)) -> void{};
    auto page = src();
    write_lock lock(get_lock());
    page_iterator(page.first, page.second, [t,fc,predicate](const art::leaf* l)
    {
        if (predicate(l))
            art_delete(t, l->get_key(),fc);
    });
}

void abstract_lru_eviction(art::tree* t, const std::function<bool(const art::leaf* l)>& predicate)
{
    if (heap::allocated < art::get_max_module_memory()) return;
    auto &lc = art::get_leaf_compression();
    abstract_eviction(t,predicate,[&lc](){return lc.get_lru_page();});
}

void abstract_lfu_eviction(art::tree* t, const std::function<bool(const art::leaf* l)>& predicate)
{
    if (heap::allocated < art::get_max_module_memory()) return;
    auto &lc = art::get_leaf_compression();
    abstract_eviction(t,predicate,[&lc](){return lc.get_lru_page();});
}

void run_evict_all_keys_lru(art::tree* t)
{
    if (!art::get_evict_allkeys_lru()) return;
    abstract_lru_eviction(t, [](const art::leaf* unused(l)) -> bool {return true;});
}

void run_evict_volatile_keys_lru(art::tree* t)
{
    if (!art::get_evict_volatile_lru()) return;
    abstract_lru_eviction(t, [](const art::leaf* l) -> bool {return l->is_volatile();});
}

void run_evict_all_keys_lfu(art::tree* t)
{
    if (!art::get_evict_allkeys_lfu()) return;
    abstract_lfu_eviction(t, [](const art::leaf* unused(l)) -> bool {return true;});
}

void run_evict_volatile_keys_lfu(art::tree* t)
{
    if (!art::get_evict_volatile_lfu()) return;
    abstract_lfu_eviction(t, [](const art::leaf* l) -> bool {return l->is_volatile();});
}

void run_evict_expired_keys(art::tree* t)
{
    if (!art::get_evict_volatile_ttl()) return;
    abstract_lru_eviction(t, [](const art::leaf* l) -> bool {return l->expired();});
}

void art::tree::start_maintain()
{
    tmaintain = std::thread([&]() -> void
    {
        while (!this->mexit)
        {
            run_evict_all_keys_lru(this);
            run_evict_all_keys_lfu(this);
            run_evict_volatile_keys_lru(this);
            run_evict_volatile_keys_lfu(this);
            run_evict_expired_keys(this);
            // TODO: erase evicted keys if memory is pressured - if its configured
            if (art::get_active_defrag())
                run_defrag(); // periodic

            // we should wait on a join signal not just sleep else server wont stop quickly
            std::this_thread::sleep_for(std::chrono::milliseconds(art::get_maintenance_poll_delay()));
        }
    });
}


