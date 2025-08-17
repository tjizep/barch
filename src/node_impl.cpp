#include <cstdlib>
#include <cstring>
#include <atomic>
#include <random>

#include "art.h"
#include "statistics.h"
#include "nodes.h"
#include "node_impl.h"
#include "time_convertsion.h"

#include <algorithm>
#include "module.h"
static std::random_device rd;
static std::mt19937 gen(rd());

namespace art {

    node_ptr tree::make_leaf(value_type key, value_type v, leaf::ExpiryType ttl, bool is_volatile) {
        return art::make_leaf(*this, key, v, ttl, is_volatile);
    }

    node_ptr make_leaf(alloc_pair& alloc, value_type key, value_type v, leaf::ExpiryType ttl, bool is_volatile ){

        unsigned val_len = v.size;
        unsigned key_len = key.length();
        //unsigned ttl_size = ttl > 0 ? sizeof(ttl) : 0;
        //unsigned sol = sizeof(leaf);
        std::string sk = key.to_string();
        std::string sv = v.to_string();
        key = sk;
        v = sv;
        size_t leaf_size = leaf::make_size(key_len,val_len,ttl,is_volatile);
        // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
        logical_address logical{&alloc};
        auto ldata = alloc.get_leaves().new_address(logical, leaf_size);
        auto *l = new(ldata) leaf(key_len, val_len, ttl, is_volatile);
        if (alloc.is_debug) {
            std_log("allocated leaf at", logical.address(),"size", leaf_size);
        }
        ++statistics::leaf_nodes;
        l->set_key(key);
        l->set_value(v);
        if (l->byte_size() != leaf_size) {
            abort_with("invalid leaf size");
        }
        statistics::max_leaf_size = std::max<uint64_t>(statistics::max_leaf_size, l->byte_size());
        return logical;
    }
}

void art::free_leaf_node(art::leaf *l, logical_address logical) {
    if (l == nullptr) return;
    l->set_deleted();
    logical.get_ap<alloc_pair>().get_leaves().free(logical, l->byte_size());
    --statistics::leaf_nodes;
}

void art::free_leaf_node(art::node_ptr n) {
    free_leaf_node(n.l(), n.logical);
}

void art::free_node(art::node_ptr n) {
    n.free_from_storage();
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
template<typename Type4, typename Type8>
static art::node *make_node(art::node_ptr_storage &ptr, logical_address a, art::node_data *node) {
    if (node->pointer_size == 4) {
        return ptr.emplace<Type4>(a, node);
    } else if (node->pointer_size == 8) {
        return ptr.emplace<Type8>(a, node);
    }
    abort_with("invalid pointer size");
}

art::node_ptr art::resolve_read_node(logical_address address) {
    auto *node = address.get_ap<alloc_pair>().get_nodes().read<node_data>(address);
    node_ptr_storage ptr;
    if (node == nullptr) {
        return node_ptr{nullptr};
    }
    switch (node->type) {
        case node_4:
            return make_node<node4_4, node4_8>(ptr, address, node);
        case node_16:
            return make_node<node16_4, node16_8>(ptr, address, node);
        case node_48:
            return make_node<node48_4, node48_8>(ptr, address, node);
        case node_256:
            return make_node<node256_4, node256_8>(ptr, address, node);
        default:
            abort_with("unknown or invalid node type");
    }
}
namespace art {
    node_ptr resolve_write_node(logical_address address) {
        auto *node = address.get_ap<alloc_pair>().get_nodes().modify<node_data>(address);
        node_ptr_storage ptr;
        switch (node->type) {
            case node_4:
                return make_node<node4_4, node4_8>(ptr, address, node);
            case node_16:
                return make_node<node16_4, node16_8>(ptr, address, node);
            case node_48:
                return make_node<node48_4, node48_8>(ptr, address, node);
            case node_256:
                return make_node<node256_4, node256_8>(ptr, address, node);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr alloc_node_ptr(alloc_pair& alloc, unsigned ptrsize, unsigned nt, const art::children_t &c) {
        if (ptrsize == 8) return alloc_8_node_ptr(alloc, nt);

        node_ptr_storage ptr;
        switch (nt) {
            case node_4:
                return ptr.emplace<node4_4>()->create(alloc).expand_pointers(c);
            case node_16:
                return ptr.emplace<node16_4>()->create(alloc).expand_pointers(c);
            case node_48:
                return ptr.emplace<node48_4>()->create(alloc).expand_pointers(c);
            case node_256:
                return ptr.emplace<node256_4>()->create(alloc).expand_pointers(c);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr tree::alloc_node_ptr(unsigned ptrsize, unsigned nt, const children_t &c) {
        return art::alloc_node_ptr(*this, ptrsize, nt, c);
    }
    node_ptr alloc_8_node_ptr(alloc_pair& alloc, unsigned nt) {
        node_ptr_storage ptr;
        switch (nt) {
            case node_4:
                return ptr.emplace<node4_8>()->create_node(alloc);
            case node_16:
                return ptr.emplace<node16_8>()->create_node(alloc);
            case node_48:
                return ptr.emplace<node48_8>()->create_node(alloc);
            case node_256:
                return ptr.emplace<node256_8>()->create_node(alloc);
            default:
                throw std::runtime_error("Unknown node type");
        }
    }

    node_ptr tree::alloc_8_node_ptr(unsigned nt) {
        return art::alloc_8_node_ptr(*this, nt);
    }
}



/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */


unsigned art::node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) const {
    auto &d = data();
    unsigned max_cmp = std::min<int>(std::min<int>(d.partial_len, max_prefix_llength),
                                     (int) key_len - (int) depth);
    unsigned idx;

    for (idx = 0; idx < max_cmp; idx++) {
        if (d.partial[idx] != key[depth + idx])
            return idx;
    }
    return idx;
}

art::tree::~tree() {
    repl_client.stop();
    mexit = true;
    if (tmaintain.joinable())
        tmaintain.join();
}

#include "configuration.h"
#include <functional>

void page_iterator(const heap::buffer<uint8_t> &page_data, unsigned size, std::function<void(const art::leaf *)> cb) {
    if (!size) return;

    auto e = page_data.begin() + size;
    size_t deleted = 0;
    size_t pos = 0;
    for (auto i = page_data.begin(); i < e;) {
        const art::leaf *l = (art::leaf *) i;
#if 0
        auto ks = l->byte_size();
        if (ks > statistics::max_leaf_size) {
            art::std_log("unusual leaf size",ks);
        }
#endif
        if (l->deleted()) {
            deleted++;
        } else {
            cb(l);
        }
        pos += l->byte_size() + test_memory;
        i += (l->byte_size() + test_memory);

    }
}

/**
 * "active" defragmentation: takes all the fragmented pages and removes the not deleted keys on those
 * then adds them back again
 * this function isn't supposed to run a lot
 */
void art::tree::run_defrag() {
    auto fc = [](const node_ptr & unused(n)) -> void {
    };
    auto &lc = get_leaves();


    try {
        if (lc.fragmentation_ratio() > 0.3) //get_min_fragmentation_ratio())
        {
            heap::vector<size_t> fl;
            {
                storage_release releaser(this->latch);
                fl = lc.create_fragmentation_list(get_max_defrag_page_count());
            }
            key_options options;
            for (auto p: fl) {
                storage_release releaser(this->latch);
                // for some reason we have to not do this while a transaction is active
                if (transacted) continue;
                jump.clear();
                auto page = lc.get_page_buffer(p);

                page_iterator(page.first, page.second, [&fc,this](const leaf *l) {
                    if (l->deleted()) return;
                    size_t c1 = this->size;
                    this->remove(l->get_key(), fc);
                    if (c1 - 1 != this->size) {
                        abort_with("key does not exist anymore");
                    }
                });

                page_iterator(page.first, page.second, [&fc,&options,this](const leaf *l) {
                    if (l->deleted()) return;
                    size_t c1 = this->size;
                    options.set_expiry(l->expiry_ms());
                    options.set_volatile(l->is_volatile());
                    art_insert(this, options, l->get_key(), l->get_value(), true, fc);
                    if (c1 + 1 != this->size) {
                        abort_with("key not added");
                    }
                });
                ++statistics::pages_defragged;
            }
        }
    } catch (std::exception &) {
        ++statistics::exceptions_raised;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(40)); // chill a little we've worked hard
}

void abstract_eviction(art::tree *t,
                       const std::function<bool(const art::leaf *l)> &predicate,
                       const std::function<std::pair<heap::buffer<uint8_t>, size_t> ()> &src) {

    if (statistics::logical_allocated < art::get_max_module_memory()) return;
    auto fc = [](const art::node_ptr & unused(n)) -> void {
    };
    t->clear_hash();
    //write_lock lock(get_lock());
    auto page = src();
    page_iterator(page.first, page.second, [t,fc,predicate](const art::leaf *l) {
        if (!l->deleted() && predicate(l)) {
            ++statistics::keys_evicted;
            art_delete(t, l->get_key(), fc); // will get cleaned up by defrag
        }
    });

}

void abstract_lru_eviction(art::tree *t, const std::function<bool(const art::leaf *l)> &predicate) {
    if (statistics::logical_allocated < art::get_max_module_memory()) return;
    storage_release release(t->latch);
    auto &lc = t->get_leaves();
    abstract_eviction(t, predicate, [&lc]() { return lc.get_lru_page(); });
}
void abstract_random_eviction(art::tree *t, const std::function<bool(const art::leaf *l)> &predicate) {
    if (statistics::logical_allocated < art::get_max_module_memory()) return;
    storage_release release(t->latch);
    auto &lc = t->get_leaves();
    auto page_count = lc.get_page_count();

    if (page_count > 10) {
        std::uniform_int_distribution<size_t> dist(0, page_count - 1);
        size_t random_page = dist(gen);
        abstract_eviction(t, predicate, [&lc, random_page]() { return lc.get_page_buffer(random_page); });
    }
}
void abstract_iter_eviction(art::tree *t, size_t ctr, const std::function<bool(const art::leaf *l)> &predicate) {
    if (statistics::logical_allocated < art::get_max_module_memory()) return;
    storage_release release(t->latch);
    auto &lc = t->get_leaves();
    auto page_count = lc.get_page_count();

    if (page_count > 10) {
        size_t random_page = page_count % ctr;
        //art::std_log("choose page",random_page,"to evict from",ctr);
        abstract_eviction(t, predicate, [&lc, random_page]() { return lc.get_page_buffer(random_page); });
    }
}

void abstract_lfu_eviction(art::tree *t, const std::function<bool(const art::leaf *l)> &predicate) {
    if (statistics::logical_allocated < art::get_max_module_memory()) return;
    auto &lc = t->get_leaves();
    abstract_eviction(t, predicate, [&lc]() { return lc.get_lru_page(); });
}

void run_evict_all_keys_lru(art::tree *t) {
    if (!art::get_evict_allkeys_lru()) return;
    abstract_lru_eviction(t, [](const art::leaf * unused(l)) -> bool { return true; });
}

void run_evict_volatile_keys_lru(art::tree *t) {
    if (!art::get_evict_volatile_lru()) return;
    abstract_lru_eviction(t, [](const art::leaf *l) -> bool { return l->is_volatile(); });
}

void run_evict_all_keys_lfu(art::tree *t) {
    if (!art::get_evict_allkeys_lfu()) return;
    abstract_lfu_eviction(t, [](const art::leaf * unused(l)) -> bool { return true; });
}

void run_evict_volatile_keys_lfu(art::tree *t) {
    if (!art::get_evict_volatile_lfu()) return;
    abstract_lfu_eviction(t, [](const art::leaf *l) -> bool { return l->is_volatile(); });
}

void run_evict_volatile_expired_keys(art::tree *t) {
    if (!art::get_evict_volatile_ttl()) return;
    abstract_lru_eviction(t, [](const art::leaf *l) -> bool { return l->expired(); });
}
void run_sweep_expired_keys(art::tree *t) {
    abstract_random_eviction(t, [](const art::leaf *l) -> bool {
        return l->expired();
    });
}

static uint64_t get_modifications() {
    return statistics::insert_ops + statistics::delete_ops + statistics::set_ops;
}

void art::tree::start_maintain() {

    tmaintain = std::thread([&]() -> void {
        uint64_t jf = get_jump_factor();
        auto start_save_time = std::chrono::high_resolution_clock::now();
        auto mods = get_modifications();
        while (!this->mexit) {
            run_evict_all_keys_lru(this);
            run_evict_all_keys_lfu(this);
            run_evict_volatile_keys_lru(this);
            run_evict_volatile_keys_lfu(this);
            run_evict_volatile_expired_keys(this);
            run_sweep_expired_keys(this);
            {
                storage_release releaser(this->latch);
                if (statistics::logical_allocated < art::get_max_module_memory()) {
                    if (jump.size() < this->size || jf != get_jump_factor()) {
                        rehash_jump();
                        jf = get_jump_factor();
                    }
                }

            }
            // defrag will get rid of memory used by evicted keys if memory is pressured - if its configured
            if (art::get_active_defrag())
                run_defrag(); // periodic
            if (millis(start_save_time) > get_save_interval()
                || get_modifications() - mods > get_max_modifications_before_save()
            ) {
                if (get_modifications() - mods > 0) {
                    this->save(with_stats);
                    start_save_time = std::chrono::high_resolution_clock::now();
                    mods = get_modifications();
                }
            }
            ++statistics::maintenance_cycles;
            //std::uniform_int_distribution<size_t> dist(1, art::get_maintenance_poll_delay());

            // TODO: we should wait on a join signal not just sleep else server wont stop quickly
            //std::this_thread::sleep_for(std::chrono::milliseconds(dist(gen)+5));
            std::this_thread::sleep_for(std::chrono::milliseconds(art::get_maintenance_poll_delay()));
        }
    });
}
