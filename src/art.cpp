#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <vector>
#include <atomic>
#include "art.h"
#include "valkeymodule.h"
#include "statistics.h"
#include "logical_allocator.h"
#include "configuration.h"
#include "glob.h"
#include "keys.h"
#include "logger.h"

extern art::tree *get_art(size_t shard);
/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art::tree *unused(t)) {
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art::node_ptr n) {
    // Break if null
    if (n.null()) return;

    if (n.is_leaf) {
        free_leaf_node(n);
        return;
    }
    // Handle each node type
    int i, idx;
    switch (n->type()) {
        case art::node_4:
        case art::node_16:
        case art::node_256:
            for (i = 0; i < n->data().occupants; i++) {
                free_node(n->get_node(i));
            }
            break;
        case art::node_48:
            for (i = 0; i < 256; i++) {
                idx = n->get_key(i);
                if (!idx) continue;

                free_node(n->get_node(i));
            }
            break;
        default:
            abort_with("unknown or invalid key type");
    }

    // Free ourself on the way up
    free_node(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int tree_destroy(art::tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * find first not less than
 */
static art::trace_element lower_bound_child(art::node_ptr n, const unsigned char *key, unsigned key_len, unsigned depth,
                                            int *is_equal) {
    unsigned char c = 0x00;
    if (n.null()) return {};
    if (n.is_leaf) return {};

    c = key[std::min(depth, key_len)];
    auto r = n->lower_bound_child(c);
    if (is_equal)
        *is_equal = r.second;
    return r.first;
}

static art::node_ptr find_child(art::node_ptr n, unsigned char c) {
    if (n.null()) return nullptr;
    if (n.is_leaf) return nullptr;

    unsigned i = n->index(c);
    return n->get_child(i);
}


/**
 * return last element of trace unless its empty
 */
static art::trace_element &last_el(art::trace_list &trace) {
    if (trace.empty())
        abort_with("the trace is empty");
    return trace.back();
}
#if 0
static const art::trace_element& last_el(const art::trace_list& trace)
{
    if (trace.empty())
        abort();
    return *(trace.rbegin());
}
#endif
static art::node_ptr last_node(const art::trace_list &trace) {
    if (trace.empty())
        return nullptr;
    return trace.rbegin()->child;
}

static art::trace_element first_child_off(art::node_ptr n);

//static art::trace_element last_child_off(art::node_ptr n);
static art::node_ptr inner_maximum(art::node_ptr n);

/**
 * assuming that the path to each leaf is not the same depth
 * we always have to check and extend if required
 * @return false if any non leaf node has no child
 */
static bool extend_trace_min(const art::node_ptr &root, art::trace_list &trace) {
    if (trace.empty()) {
        trace.push_back(first_child_off(root));
    };
    art::trace_element u = last_el(trace);
    while (!u.child.is_leaf) {
        u = first_child_off(u.child);
        if (u.empty()) {
            return false;
        }
        trace.push_back(u);
    }
    return true;
}

static art::trace_element last_child_off(art::node_ptr n) {
    if (n.null()) return {nullptr, nullptr, 0};
    if (n.is_leaf) return {nullptr, nullptr, 0};
    auto idx = n->last_index();

    return {n, n->get_child(idx.first), idx.first, idx.second};
}

static bool extend_trace_max(art::node_ptr root, art::trace_list &trace) {
    if (trace.empty()) {
        trace.push_back(last_child_off(root));
    };
    art::trace_element u = last_el(trace);
    while (!u.child.is_leaf) {
        u = last_child_off(u.child);
        if (u.empty()) return false;
        trace.push_back(u);
    }
    return true;
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
art::node_ptr art_search(const art::tree *t, art::value_type key) {
    ++statistics::get_ops;
    try {
        art::node_ptr n = t->root;
        unsigned depth = 0;
        while (!n.null()) {
            // Might be a leaf
            if (n.is_leaf) {
                const auto *l = n.const_leaf();
                if (l->expired()) return nullptr;

                if (0 == l->compare(key)) {
                    return n;
                }
                return nullptr;
            }
            // Bail if the prefix does not match
            const auto &d = n->data();
            if (d.partial_len) {
                unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
                if (prefix_len != std::min<unsigned>(art::max_prefix_llength, d.partial_len))
                    return nullptr;
                depth += d.partial_len;
                if (depth >= key.length()) {
                    return nullptr;
                }
            }
            //node_ptr p = n;
            unsigned at = n->index(key[depth]);
            n = n->get_child(at);
            //trace_element te = {p,n,at, key[depth]};
            //trace.push_back(te);
            depth++;
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return nullptr;
}

void art::update(tree *t, value_type key, const std::function<node_ptr(const node_ptr &leaf)> &updater) {
    ++statistics::update_ops;
    try {
        art::node_ptr r;
        art::node_ptr n = t->root;
        unsigned depth = 0;
        art::node_ptr last = t->root;
        unsigned last_index = 0;
        while (!n.null()) {
            // Might be a leaf
            if (n.is_leaf) {
                const auto *l = n.const_leaf();
                if (l->expired()) {
                    r = updater(r);
                    return;
                }
                if (0 == l->compare(key)) {
                    auto updated_child = updater(n);
                    if (updated_child.null()) return;
                    if (last.is_leaf) {
                        t->root = updated_child;
                    } else {
                        last = last.modify()->expand_pointers(last, {updated_child});
                        last.modify()->set_child(last_index, updated_child);
                    }
                    free_leaf_node(n);
                } else {
                    r = updater(r);
                }
                return;
            }
            const auto &d = n->data();
            if (d.partial_len) {
                unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
                if (prefix_len != std::min<unsigned>(art::max_prefix_llength, d.partial_len)) {
                    r = updater(r);
                    return;
                }

                depth += d.partial_len;
                if (depth >= key.length()) {
                    r = updater(r);
                    return;
                }
            }
            last = n;
            last_index = n->index(key[depth]);
            n = n->get_child(last_index);
            depth++;
        }
        r = updater(r);
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}

// Find the maximum leaf under a node
static art::node_ptr inner_maximum(art::node_ptr n) {
    // Handle base cases
    if (n.null()) return nullptr;
    if (n.is_leaf) return n;
    return inner_maximum(n->last());
}


// Find the minimum leaf under a node
static art::node_ptr minimum(const art::node_ptr &n) {
    // Handle base cases
    if (n.null()) return nullptr;
    if (n.is_leaf) return n;
    return minimum(n->get_child(n->first_index().first));
}

/**
 * Searches for the lower bound key
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the leaf containing the value pointer is returned.
 */
static bool increment_trace(const art::node_ptr &root, art::trace_list &trace);

static art::node_ptr inner_lower_bound(art::trace_list &trace, const art::tree *t, art::value_type key) {
    art::node_ptr n = t->root;
    unsigned depth = 0;
    int is_equal = 0;

    while (!n.null()) {
        if (n.is_leaf) {
            // Check if the expanded path matches
            auto l = n.const_leaf();
            if (trace.empty())
                return l->expired() ? nullptr : n;

            while (true) {
                auto c = last_el(trace).child;
                if (!c.is_leaf) return nullptr;
                l = c.const_leaf();
                if (l->get_key() < key ||  l->expired()) {
                    if (!increment_trace(t->root, trace)) return nullptr;
                }else {
                    break;
                }
            }
            n = last_el(trace).child;
            return n;
        }
        auto &d = n->data();
        if (d.partial_len) {
            unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
            if (prefix_len != std::min<unsigned>(art::max_prefix_llength, d.partial_len)) {
                break;
            }
            depth += d.partial_len;
            if (depth >= key.length()) {
                break;
            }
        }

        art::trace_element te = lower_bound_child(n, key.bytes, key.length(), depth, &is_equal);
        if (te.child.null()) {
            // there is no lower bound on this node
            increment_trace(t->root, trace);
            break;
        }
        if (!is_equal && !te.child.is_leaf) {
            // only for internal nodes - the lb has skipped some child nodes so we have to go back one
            te = te.parent->previous(te);
            if (te.child.null()) {
                break;
            }
        }
        trace.push_back(te);
        n = te.child;
        depth++;
    }
    if (!extend_trace_min(t->root, trace)) return nullptr;
    while (true) {
        auto c = last_el(trace).child;
        if (!c.is_leaf) return nullptr;
        auto l = c.const_leaf();
        if (l->get_key() < key ||  l->expired()) {
            if (!increment_trace(t->root, trace)) return nullptr;
        }else {
            break;
        }
    }
    n = last_el(trace).child;


    return n;
}

// TODO: this is a fast function that reduces lower bound shimmying - but I'm not sure if itll always work
#if 0
static art::node_ptr inner_min_bound(art::trace_list& trace, const art::tree* t, art::value_type key)
{
    art::node_ptr n = t->root;
    int depth = 0, is_equal = 0;

    while (!n.null())
    {
        if (n.is_leaf)
        {
            auto l = n.const_leaf();
            if (l->expired())
            {
                art_delete((art::tree*)t, l->get_key());
                n = t->root;
                continue;
            }

            return n; // luxury return
        }
        auto& d = n->data();
        if (d.partial_len)
        {
            unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
            if (prefix_len != std::min<unsigned>(art::max_prefix_llength, d.partial_len))
            {
                art::trace_element te = lower_bound_child(n, key.bytes, key.length(), depth+prefix_len, &is_equal);
                if (te.child_ix == d.occupants)
                {
                    if (trace.empty()) return nullptr;
                    if (!extend_trace_max(t->root, trace)) return nullptr;
                    n = last_el(trace).child;
                    continue;
                }
                break;
            }
            depth += d.partial_len;
        }

        art::trace_element te = lower_bound_child(n, key.bytes, key.length(), depth, &is_equal);
        if (!te.empty())
        {
            trace.push_back(te);
        }else
        {
            abort_with("the trace is empty");
        }
        n = te.child;
        depth++;
    }
    if (!extend_trace_min(t->root, trace)) return nullptr;
    n = last_el(trace).child;
    return n;
}
#endif
static art::trace_element first_child_off(art::node_ptr n) {
    if (n.null())
        return {nullptr, nullptr, 0};
    if (n.is_leaf)
        return {nullptr, nullptr, 0};

    auto at = n->first_index();
    auto np = n->get_child(at.first);
    return {n, np, at.first, at.second};
}


static art::trace_element increment_te(const art::trace_element &te) {
    if (te.parent.null()) return {nullptr, nullptr, 0};
    if (te.parent.is_leaf) return {nullptr, nullptr, 0};

    const art::node *n = te.parent.get_node();
    auto np = n->next(te);
    return np;
}

static art::trace_element decrement_te(const art::trace_element &te) {
    if (te.parent.null()) return {nullptr, nullptr, 0};
    if (te.parent.is_leaf) return {nullptr, nullptr, 0};

    const art::node *n = te.parent.get_node();
    return n->previous(te);
}


static bool increment_trace(const art::node_ptr &root, art::trace_list &trace) {
    while (!trace.empty()) {
        auto &last = last_el(trace);
        auto npt = increment_te(last); //last.parent->next(last);
        if (!npt.valid()) {
            trace.pop_back();
        } else {
            trace.back() = npt;
            break;
        }
    }
    if (trace.empty()) {
        return false;
    }
    return extend_trace_min(root, trace);
}

static bool decrement_trace(const art::node_ptr &root, art::trace_list &trace) {
    while (!trace.empty() && last_el(trace) == last_el(trace).parent->first_index()) {
        trace.pop_back();
    }
    if (trace.empty()) {
        return extend_trace_min(root, trace);
    }
    auto &last = last_el(trace);
    trace.back() = decrement_te(last);
    return extend_trace_max(root, trace);
}

art::node_ptr art::lower_bound(const art::tree *t, art::value_type key) {
    ++statistics::lb_ops;
    try {
        art::node_ptr al;
        art::trace_list tl;
        al = inner_lower_bound(tl, t, key);
        if (!al.null()) {
            return al;
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return nullptr;
}

int art::range(const art::tree *t, art::value_type key, art::value_type key_end, CallBack cb, void *data) {
    ++statistics::range_ops;
    try {
        art::trace_list tl;
        auto lb = inner_lower_bound(tl, t, key);
        if (lb.null() || tl.empty()) return 0;
        const art::leaf *al = lb.const_leaf();
        if (al) {
            do {
                art::node_ptr n = last_el(tl).child;
                if (n.is_leaf) {
                    const art::leaf *leaf = n.const_leaf();
                    if (leaf->compare(key_end) <= 0) {
                        // upper bound is not
                        if (!leaf->expired()) {
                            ++statistics::iter_range_ops;
                            int r = cb(data, leaf->get_key(), leaf->get_value());
                            if (r != 0)
                                return r;
                        } //skip this one if it's expired
                    } else {
                        return 0;
                    }
                } else {
                    abort_with("not a leaf");
                }
            } while (increment_trace(t->root, tl));
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}

int art::range(const art::tree *t, art::value_type key, art::value_type key_end, LeafCallBack cb) {
    ++statistics::range_ops;
    try {
        art::trace_list tl;
        auto lb = inner_lower_bound(tl, t, key);
        if (lb.null()) return 0;
        const art::leaf *al = lb.const_leaf();
        if (al) {
            do {
                art::node_ptr n = last_el(tl).child;
                if (n.is_leaf) {
                    const art::leaf *leaf = n.const_leaf();
                    if (leaf->compare(key_end) <= 0) {
                        // upper bound is not
                        if (!leaf->expired()) {
                            ++statistics::iter_range_ops;
                            int r = cb(n);
                            if (r != 0)
                                return r;
                        } //skip this one if it's expired
                    } else {
                        return 0;
                    }
                } else {
                    abort_with("not a leaf");
                }
            } while (increment_trace(t->root, tl));
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}


art::node_ptr art::find(const tree* t, value_type key) {
    //const tree *t = get_art(get_shard(key));
    ++statistics::get_ops;
    try {
        node_ptr n = t->root;
        unsigned depth = 0;
        while (!n.null()) {
            // Might be a leaf
            if (n.is_leaf) {
                const auto *l = n.const_leaf();
                if (l->expired()) return nullptr;

                if (0 == l->compare(key)) {
                    return n;
                }
                return nullptr;
            }
            // Bail if the prefix does not match
            const auto &d = n->data();
            if (d.partial_len) {
                unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
                if (prefix_len != std::min<unsigned>(max_prefix_llength, d.partial_len))
                    return nullptr;
                depth += d.partial_len;
                if (depth >= key.length()) {
                    return nullptr;
                }
            }
            unsigned at = n->index(key[depth]);
            n = n->get_child(at);
            depth++;
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return nullptr;
}

art::iterator::iterator(tree* t) : t(t) {
    auto lb = art_minimum(t); //inner_min_bound(tl, t, key);
    if (lb.null()) return;
    const art::leaf *al = lb.const_leaf();
    if (!al) {
        tl.clear();
    } else {
        c = last_node(tl);
    }
}

art::iterator::iterator(tree* t, value_type key) : t(t) {
    ++statistics::lb_ops;
    try {
        auto lb = inner_lower_bound(tl, t, key); //inner_min_bound(tl, t, key);
        if (lb.null()) return;
        const art::leaf *al = lb.const_leaf();
        if (!al) {
            tl.clear();
        } else {
            c = last_node(tl);
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}

bool art::iterator::previous() {
    c = nullptr;
    bool r = decrement_trace(t->root, tl);
    if (!r) {
        tl.clear();
    } else {
        c = last_node(tl);
        if (!c.is_leaf) {
            c = nullptr;
        }
    }
    return r;
}

bool art::iterator::next() {
    c = nullptr;
    bool r = increment_trace(t->root, tl);
    if (!r) {
        tl.clear();
    } else {
        c = last_node(tl);
        if (!c.is_leaf) {
            c = nullptr;
        }
    }
    return r;
}

bool art::iterator::end() const {
    return !c.is_leaf || tl.empty() || !t || t->size == 0;
}

bool art::iterator::ok() const {
    return !end();
}

art::node_ptr art::iterator::current() const {
    return c;
}

const art::leaf *art::iterator::l() const {
    return current().const_leaf();
}

art::value_type art::iterator::key() const {
    return l()->get_key();
}

art::value_type art::iterator::value() const {
    return l()->get_value();
}

bool art::iterator::remove() const {
    if (end()) return false;
    auto bef = t->size;
    art_delete(t, key());
    return bef > t->size;
}

bool art::iterator::update(std::function<node_ptr(const leaf *l)> updater) {
    if (end()) return false;
    auto &el = last_el(tl);
    art::node_ptr n = el.child;
    if (n.is_leaf) {
        const art::leaf *leaf = n.const_leaf();
        if (!leaf->expired()) {
            node_ptr new_leaf = updater(leaf);
            n = n.modify()->expand_pointers(n, {new_leaf});
            n.modify()->set_child(el.child_ix, new_leaf);
            destroy_node(n);
            return true;
        } //skip this one if it's expired
    }
    return false;
}

bool art::iterator::update(int64_t ttl, bool volat) {
    return update([&](const art::leaf *l) -> node_ptr {
        return t->make_leaf(l->get_key(), l->get_value(), ttl, volat);
    });
}

bool art::iterator::update(int64_t ttl) {
    return update([&](const art::leaf *l) -> node_ptr {
        return t->make_leaf(l->get_key(), l->get_value(), ttl, l->is_volatile());
    });
}

bool art::iterator::update(value_type value, int64_t ttl, bool volat) {
    return update([&](const art::leaf *l) -> node_ptr {
        return t->make_leaf(l->get_key(), value, ttl, volat);
    });
}

bool art::iterator::update(value_type value) {
    return update([&](const art::leaf *l) -> node_ptr {
        return t->make_leaf(l->get_key(), value, l->ttl(), l->is_volatile());
    });
}

static art::trace_element first(const art::trace_element &el) {
    if (el.parent.is_leaf) return {};
    if (el.parent.null()) return {};
    auto fst = el.parent->first_index();
    return {el.parent, el.parent->get_child(fst.first), fst.first, fst.second};
}

static art::trace_element next(const art::trace_element &el) {
    if (el.parent.is_leaf) return {};
    if (el.parent.null()) return {};
    return el.parent->next(el);
}
#ifdef __UNUSED_FUNCTION__
static art::trace_element previous (const art::trace_element& el)
{
    if (el.parent.is_leaf) return {};
    if (el.parent.null()) return {};
    return el.parent->previous(el);
}
#endif
static int64_t descendants(const art::trace_element &t) {
    if (t.child.null()) return 0;
    if (t.child.is_leaf) {
        return 1;
    }
    return t.child->data().descendants;
}
#ifdef __UNUSED_FUNCTION__
static art::trace_element last(const art::trace_element& el)
{
    auto li = el.parent->last_index();
    return {el.parent,el.parent->get_child(li),li};
}
#endif
static int64_t total(const art::trace_element &start, const art::trace_element &end) {
    int64_t r = 0;
    if (start.parent.null()) return 0;
    art::trace_element i = start, e = {};
    while (i != e && i != end) {
        r += descendants(i);
        i = next(i);
    };
    return r;
}

static int64_t indexed_distance(const art::trace_list &a, const art::trace_list &b) {
    if (b.empty() || a.empty()) return 0;
    if (b.back() == a.back()) return 0;
    int64_t r = descendants(a.back()); //  + descendants(b.back());
    size_t depth = std::min(a.size(), b.size());
    for (size_t i = 0; i < depth; ++i) {
        if (a[i] == b[i]) {
            continue; // there's nothing between them
        }

        if (a[i].parent == b[i].parent) {
            r += total(next(a[i]), b[i]);
        } else {
            r += total(next(a[i]), {});
            r += total(first(b[i]), b[i]);
        }
    }

    for (size_t i = depth; i < a.size(); ++i) {
        r += total(next(a[i]), {});
    }

    for (size_t i = depth; i < b.size(); ++i) {
        r += total(first(b[i]), b[i]);
    }
    return r;
}

// computes distance while incrementing a trace list as efficiently as it can
int64_t art::fast_distance(const trace_list &ia, const trace_list &b) {
    if (ia.empty()) return 0;
    if (b.empty()) return 0;
    if (ia[0].parent != b[0].parent) return 0;
    return indexed_distance(ia, b);
}

int64_t art::iterator::distance(const iterator &other) const {
    iterator a = *this;
    iterator b = other;
    int64_t r = 0;
    while (a.ok() && a.key() <= b.key()) {
        //log_encoded_key(a.key());
        ++r;
        auto kprev = a.key();
        a.next();
        if (a.key() < kprev) {
            abort_with("invalid key order");
        }
    }
    return r;
}

int64_t art::iterator::distance(value_type other, bool traced) const {
    iterator a = *this;
    int64_t r = 0;
    auto prev = a.key();
    while (a.ok() && a.key() <= other) {
        if (traced)
            log_encoded_key(a.key());
        if (a.key() < prev) {
            log_encoded_key(a.key());
            log_encoded_key(prev);
            abort_with("invalid key order");
        }
        prev = a.key();
        ++r;
        a.next();
    }
    return r;
}

int64_t art::iterator::fast_distance(const iterator &other) const {
    return art::fast_distance(this->tl, other.tl);
}

void art::iterator::log_trace() const {
    size_t ctr = 0;
    std_log("=======-iterator trace-========");
    std_log("  tree size: ", this->t->size);
    log_encoded_key(key());
    for (auto &el: tl) {
        auto tp = el.parent->type();
        auto checked = el.parent->check_data();
        std_log(++ctr, "address:", el.parent.logical.address(), "type:", el.parent->data().type, "child index:",
                el.child_ix, el.k, tp, checked);
    }
    std_log("=====-end iterator trace-======");
}

/**
 * Returns the minimum valued leaf
 */
art::node_ptr art_minimum(art::tree *t) {
    ++statistics::min_ops;
    try {
        auto l = minimum(t->root);
        if (l.null()) return nullptr;
        return l;
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return nullptr;
}

/**
 * Returns the maximum valued leaf
 */
art::node_ptr art::maximum(art::tree *t) {
    ++statistics::max_ops;
    try {
        auto l = inner_maximum(t->root);
        if (l.null()) return nullptr;
        return l;
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return nullptr;
}

static unsigned longest_common_prefix(const art::leaf *l1, const art::leaf *l2, int depth) {
    unsigned max_cmp = std::min<unsigned>(l1->key_len, l2->key_len) - depth;
    unsigned idx;
    for (idx = 0; idx < max_cmp; idx++) {
        if (l1->key()[depth + idx] != l2->key()[depth + idx])
            return idx;
    }
    return idx;
}


/**
 * Calculates the index at which the prefixes mismatch
 */
;

static int prefix_mismatch(const art::node_ptr &n, art::value_type key, unsigned depth) {
    int kd = key.length() - depth; // this can be negative ?
    int max_cmp = std::min<int>(std::min<int>(art::max_prefix_llength, n->data().partial_len), kd);
    int idx;
    auto &dat = n->data();
    for (idx = 0; idx < max_cmp; idx++) {
        if (dat.partial[idx] != key[depth + idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (dat.partial_len > art::max_prefix_llength) {
        // Prefix is longer than what we've checked, find a leaf
        const art::leaf *l = minimum(n).const_leaf();
        max_cmp = std::min<unsigned>(l->key_len, key.length()) - depth; // may be negative
        for (; idx < max_cmp; idx++) {
            if (l->key()[idx + depth] != key[depth + idx])
                return idx;
        }
    }
    return idx;
}

static art::node_ptr recursive_insert(art::tree *t, const art::key_spec &options, art::node_ptr n, art::node_ptr &ref,
                                      art::value_type key, art::value_type value, int depth, int *old, int replace,const NodeResult &fc) {
    // If we are at a nullptr node, inject a leaf
    if (n.null()) {
        ref = t->make_leaf(key, value, options.ttl);
        t->last_leaf_added = ref;
        return nullptr;
    }
    // If we are at a leaf, we need to replace it with a node
    if (n.is_leaf) {
        const art::leaf *l = n.const_leaf();
        // Check if we are updating an existing value
        if (l->compare(key) == 0) {
            *old = 1;
            if (replace) {
                fc(n);
                art::leaf *dl = n.l();

                if (dl->val_len == value.size && !l->expired())
                {
                    dl->set_value(value);
                }
                else
                {
                    ref = t->make_leaf(key, value, options.keepttl ? dl->ttl() : options.ttl, dl->is_volatile());
                    t->last_leaf_added = ref;
                    // create a new leaf to carry the new value
                    ++statistics::leaf_nodes_replaced;
                    return n;
                }
            }
            return nullptr;
        }
        art::node_ptr l1 = n;
        // Create a new leaf
        art::node_ptr l2 = t->make_leaf(key, value);
        t->last_leaf_added = l2;
        // New value, we must split the leaf into a node_4, pasts the new children to get optimal pointer size
        auto new_stored = t->alloc_node_ptr(initial_node_ptr_size, art::initial_node, {l1, l2});
        auto *new_node = new_stored.modify();
        // Determine longest prefix
        l = n.const_leaf();
        unsigned longest_prefix = longest_common_prefix(l, l2.const_leaf(), depth);
        new_node->data().partial_len = longest_prefix;
        memcpy(new_node->data().partial, key.bytes + depth,
               std::min<unsigned>(art::max_prefix_llength, longest_prefix));
        // Add the leafs to the new node_4
        ref = new_node;
        ref.modify()->add_child(l->key()[depth + longest_prefix], ref, l1);
        if (l1.is_leaf) // because l1 isnt going to be in the path
        {
            ++ref.modify()->data().descendants;
        }
        auto l2k = l2.const_leaf()->key()[depth + longest_prefix];
        auto l2idx = ref.modify()->add_child(l2k, ref, l2);
        t->push_trace({ref, l2, l2idx, l2k});
        return nullptr;
    }

    // Check if given node has a prefix
    if (n->data().partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, depth);
        if ((uint32_t) prefix_diff >= n->data().partial_len) {
            depth += n->data().partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node and a new leaf
        art::node_ptr new_leaf = make_leaf(*t, key, value);
        t->last_leaf_added = new_leaf;
        auto new_node = t->alloc_node_ptr(initial_node_ptr_size, art::initial_node, {n, new_leaf});
        // pass children to get opt. ptr size
        ref = new_node;
        new_node.modify()->data().partial_len = prefix_diff;
        memcpy(new_node.modify()->data().partial, n->data().partial,
               std::min<int>(art::max_prefix_llength, prefix_diff));
        // Adjust the prefix of the old node
        auto &modn = n.modify()->data();
        if (n->data().partial_len <= art::max_prefix_llength) {
            auto ck = modn.partial[prefix_diff];
            ref.modify()->add_child(ck, ref, n); // descendants of n will be added to ref
            modn.partial_len -= (prefix_diff + 1);
            memmove(modn.partial, modn.partial + prefix_diff + 1,
                    std::min<int>(art::max_prefix_llength, modn.partial_len));
        } else {
            modn.partial_len -= (prefix_diff + 1);
            const auto *l = minimum(n).const_leaf();
            auto ck = l->get_key()[depth + prefix_diff];
            ref.modify()->add_child(ck, ref, n);
            memcpy(modn.partial, l->key() + depth + prefix_diff + 1,
                   std::min<int>(art::max_prefix_llength, modn.partial_len));
        }

        // Insert the new leaf (safely considering optimal pointer sizes)
        auto idx = ref.modify()->add_child(key[depth + prefix_diff], ref, new_leaf);

        t->push_trace({ref, new_leaf, idx, key[depth]});

        return nullptr;
    }
    // if node doesnt have a prefix search more
RECURSE_SEARCH:;

    // Find a child to recurse to
    unsigned pos = n->index(key[depth]);
    art::node_ptr child = n->get_node(pos);

    if (!child.null()) {
        if (!n.is_leaf) {
            t->push_trace({n, child, pos, key[depth]});
        }
        art::node_ptr nc = child;
        auto r = recursive_insert(t, options, child, nc, key, value, depth + 1, old, replace,fc);
        if (nc != child) {
            n = n.modify()->expand_pointers(ref, {nc});
            n.modify()->set_child(pos, nc);
        }
        return r;
    }

    // No child, node goes within us
    art::node_ptr l = t->make_leaf(key, value);
    t->last_leaf_added = l;
    n = n.modify()->expand_pointers(ref, {l});
    auto idx = n.modify()->add_child(key[depth], ref, l);
    t->push_trace({ref, l, idx, key[depth]});
    return nullptr;
}

/**
 * inserts a new value into the art tree
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert
(art::tree *t
 , const art::key_spec &options
 , art::value_type key
 , art::value_type value
 , bool replace
 , const NodeResult &fc) {
    try {
        int old_val = 0;
        t->clear_trace();

        art::node_ptr old = recursive_insert(t, options, t->root, t->root, key, value, 0, &old_val, replace ? 1 : 0, fc);
        if (!old_val) {
            t->size++;
            t->update_trace(+1);
            ++statistics::insert_ops;
        } else {
            ++statistics::set_ops;
        }
        if (!old.null()) {
            if (!old.is_leaf) {
                abort_with("not a leaf");
            }
            //fc(old);
            free_leaf_node(old);
        } else if (old_val == 1) {
            //fc(old);
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}

void art_insert(art::tree *t, const art::key_spec &options, art::value_type key, art::value_type value,
                const NodeResult &fc) {
    art_insert(t, options, key, value, true, fc);
}

/**
 * inserts a new value into the art tree (no replace)
 * @arg t the tree
 * @arg key the key
 * @arg key_len the length of the key
 * @arg value opaque value.
 * @return null if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void art_insert_no_replace(art::tree *t, const art::key_spec &options, art::value_type key, art::value_type value,
                           const NodeResult &fc) {
    ++statistics::insert_ops;
    try {
        int old_val = 0;
        art::node_ptr r = recursive_insert(t, options, t->root, t->root, key, value, 0, &old_val, 0,fc);
        if (r.null()) {
            t->size++;
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}


static void remove_child(art::node_ptr n, art::node_ptr &ref, unsigned char c, unsigned pos) {
    n.modify()->remove(ref, pos, c);
}

static const art::node_ptr recursive_delete(art::tree *t, art::node_ptr n, art::node_ptr &ref, art::value_type key,
                                            int depth) {
    // Search terminated
    if (n.null()) return nullptr;

    // Handle hitting a leaf node
    if (n.is_leaf) {
        const art::leaf *l = n.const_leaf();
        if (l->compare(key.bytes, key.length(), depth) == 0) {
            ref = nullptr;
            return n;
        }
        return nullptr;
    }

    // Bail if the prefix does not match
    auto &d = n->data();
    if (d.partial_len) {
        unsigned prefix_len = n->check_prefix(key.bytes, key.length(), depth);
        if (prefix_len != std::min<unsigned>(art::max_prefix_llength, d.partial_len)) {
            return nullptr;
        }
        depth += d.partial_len;
    }

    // Find child node
    auto k = key[depth];
    unsigned idx = n->index(key[depth]);
    art::node_ptr child = n->get_node(idx);
    if (child.null())
        return nullptr;
    t->push_trace({n, child, idx, k});
    // If the child is leaf, delete from this node
    if (child.is_leaf) {
        const art::leaf *l = child.const_leaf();
        if (l->compare(key.bytes, key.length(), depth) == 0) {
            art::node_ptr ref_bef = ref;
            remove_child(n, ref, key[depth], idx);
            if (ref_bef != ref) {
                t->pop_trace();
                if (!ref.is_leaf)
                    t->push_trace({ref, child, idx, k});
            }
            return child;
        }
        return nullptr;
    } else {
        // Recurse

        art::node_ptr new_child = child;
        auto r = recursive_delete(t, child, new_child, key, depth + 1);
        if (new_child != child) {
            if (!n->ok_child(new_child)) {
                ref = n.modify()->expand_pointers(ref, {new_child});
                ref.modify()->set_child(idx, new_child);
            } else
                n.modify()->set_child(idx, new_child);
        }
        return r;
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return nullptr if the item was not found, otherwise
 * the value pointer is returned.
 */
void art_delete(art::tree *t, art::value_type key) {
    art_delete(t, key, [](const art::node_ptr & unused(n)) {
    });
}

void art_delete(art::tree *t, art::value_type key, const NodeResult &fc) {
    ++statistics::delete_ops;
    try {
        t->clear_trace();
        art::node_ptr l = recursive_delete(t, t->root, t->root, key, 0);
        if (!l.null()) {
            t->size--;
            t->update_trace(-1);
            if (!l.const_leaf()->expired())
                fc(l);
            free_node(l);
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}

// Recursively iterates over the tree
static int recursive_iter(art::node_ptr n, CallBack cb, void *data) {
    // Handle base cases
    if (n.null()) return 0;
    if (n.is_leaf) {
        const art::leaf *l = n.const_leaf();
        ++statistics::iter_ops;
        return cb(data, l->get_key(), l->get_value());
    }

    int idx, res;
    switch (n->type()) {
        case art::node_4:
            for (int i = 0; i < n->data().occupants; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case art::node_16:
            for (int i = 0; i < n->data().occupants; i++) {
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        case art::node_48:
            for (int i = 0; i < 256; i++) {
                idx = n->get_key(i);
                if (!idx) continue;

                res = recursive_iter(n->get_child(idx - 1), cb, data);
                if (res) return res;
            }
            break;

        case art::node_256:
            for (int i = 0; i < 256; i++) {
                if (!n->has_child(i)) continue;
                res = recursive_iter(n->get_child(i), cb, data);
                if (res) return res;
            }
            break;

        default:
            abort_with("unknown or invalid node type");
    }
    return 0;
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art::tree *t, CallBack cb, void *data) {
    ++statistics::iter_start_ops;
    try {
        if (!t) {
            return -1;
        }
        return recursive_iter(t->root, cb, data);
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
        return -1;
    }
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_compare(const art::leaf *n, art::value_type prefix) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t) prefix.length()) return 1;

    // Compare the keys
    return memcmp(n->key(), prefix.bytes, prefix.length());
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art::tree *t, art::value_type key, CallBack cb, void *data) {
    ++statistics::iter_start_ops;
    try {
        if (!t) {
            return -1;
        }

        art::node_ptr n = t->root;
        unsigned prefix_len, depth = 0;
        while (!n.null()) {
            // Might be a leaf
            if (n.is_leaf) {
                // Check if the expanded path matches
                if (0 == leaf_prefix_compare(n.const_leaf(), key)) {
                    const auto *l = n.const_leaf();
                    return cb(data, l->get_key(), l->get_value());
                }
                return 0;
            }

            // If the depth matches the prefix, we need to handle this node
            if (depth == key.length()) {
                const art::leaf *l = minimum(n).const_leaf();
                if (0 == leaf_prefix_compare(l, key))
                    return recursive_iter(n, cb, data);
                return 0;
            }

            // Bail if the prefix does not match
            if (n->data().partial_len) {
                prefix_len = prefix_mismatch(n, key, depth);

                // Guard if the mis-match is longer than the max_prefix_llength
                if (prefix_len > n->data().partial_len) {
                    prefix_len = n->data().partial_len;
                }

                // If there is no match, search is terminated
                if (!prefix_len) {
                    return 0;

                    // If we've matched the prefix, iterate on this node
                } else if (depth + prefix_len == key.length()) {
                    return recursive_iter(n, cb, data);
                }

                // if there is a full match, go deeper
                depth = depth + n->data().partial_len;
            }

            // Recursively search
            n = find_child(n, key[depth]);
            depth++;
        }
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}

/**
 * just return the size
 */
uint64_t art_size(art::tree *t) {
    ++statistics::size_ops;
    try {
        if (t == nullptr)
            return 0;

        return t->size;
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}

uint64_t art_evict_lru(art::tree *t) {
    try {
        auto page = t->get_leaves().get_lru_page();
        if (!page.second) return 0;
        auto i = page.first.begin();
        auto e = i + page.second;
        auto fc = [](art::node_ptr) -> void {
            ++statistics::keys_evicted;
        };
        while (i != e) {
            const art::leaf *l = (art::leaf *) i;
            if (l->key_len > page.second) {
                abort_with("invalid key or key size");
            }
            if (l->deleted()) {
                i += (l->byte_size() + test_memory);
                continue;
            }
            art_delete(t, l->get_key(), fc);
            i += (l->byte_size() + test_memory);
        }
        ++statistics::pages_evicted;
        return page.second;
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
    return 0;
}

void art::glob(tree * t, const keys_spec &spec, value_type pattern,
               const std::function<bool(const leaf &l)> &cb) {
    try {
        int64_t counter = 0;
        // this is a multi-threaded iterator and care should be taken
        t->get_leaves().iterate_pages(t->latch,
            [&](size_t size, size_t unused(padd), const heap::buffer<uint8_t> &page)-> bool {
                if (!size) return true;
                auto i = page.begin();
                auto e = i + size;
                uint64_t misses = 0;
                while (i != e) {
                    const leaf *l = (const leaf *) i;
                    if (l->key_len > size) {
                        throw std::runtime_error("art::glob: key too long");
                    }
                    if (!(l->deleted() || l->expired())) {
                        if (!spec.count && ++counter > spec.max_count) {
                            return false;
                        }
                        if (tstring != *l->key()) // glob on string keys only
                        {
                            return true;
                        }
                        if (1 == glob::stringmatchlen(pattern, l->get_clean_key(), 0)) {
                            if (!cb(*l)) {
                                return false;
                            }
                        } else {
                            ++misses;
                        }
                    }
                    i += (l->byte_size() + test_memory);
                }

                return true;
            });
    } catch (std::exception &e) {
        art::log(e, __FILE__, __LINE__);
        ++statistics::exceptions_raised;
    }
}

art_statistics art::get_statistics() {
    art_statistics as{};
    as.heap_bytes_allocated = (int64_t) heap::allocated;
    as.leaf_nodes = (int64_t) statistics::leaf_nodes;
    as.node4_nodes = (int64_t) statistics::n4_nodes;
    as.node16_nodes = (int64_t) statistics::n16_nodes;
    as.node256_nodes = (int64_t) statistics::n256_nodes;
    as.node256_occupants = as.node256_nodes ? ((int64_t) statistics::node256_occupants / as.node256_nodes) : 0ll;
    as.node48_nodes = (int64_t) statistics::n48_nodes;
    for (size_t shard : art::get_shard_count()) {
        as.bytes_allocated += (int64_t) get_art(shard)->get_leaves().get_allocated() + get_art(shard)->get_nodes().get_allocated();
    }

    //statistics::addressable_bytes_alloc;
    for (auto shard : art::get_shard_count()) {
        as.bytes_interior += (int64_t) get_art(shard)->get_nodes().get_allocated();
    }
    as.page_bytes_compressed = (int64_t) statistics::page_bytes_compressed;
    as.page_bytes_uncompressed = (int64_t) statistics::page_bytes_uncompressed;
    as.pages_uncompressed = (int64_t) statistics::pages_uncompressed;
    as.pages_compressed = (int64_t) statistics::pages_compressed;
    as.max_page_bytes_uncompressed = (int64_t) statistics::max_page_bytes_uncompressed;
    as.vacuums_performed = (int64_t) statistics::vacuums_performed;
    as.last_vacuum_time = (int64_t) statistics::last_vacuum_time;
    as.leaf_nodes_replaced = (int64_t) statistics::leaf_nodes_replaced;
    as.pages_evicted = (int64_t) statistics::pages_evicted;
    as.keys_evicted = (int64_t) statistics::keys_evicted;
    as.pages_defragged = (int64_t) statistics::pages_defragged;
    as.exceptions_raised = (int64_t) statistics::exceptions_raised;
    return as;
}


struct transaction {
    bool was_transacted = false;
    art::tree *t = nullptr;
    transaction(const transaction&) = default;
    transaction& operator=(const transaction&) = default;
    explicit transaction(art::tree *t) : t(t) {
        was_transacted = t->transacted;
        if (!was_transacted)
            t->begin();
    }

    ~transaction() {
        if (!was_transacted)
            t->commit();

    }
};

art_ops_statistics art::get_ops_statistics() {
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

#include "ioutil.h"

template<typename OutStream>
static void stats_to_stream(OutStream &of) {
    writep(of, statistics::n4_nodes);
    writep(of, statistics::n16_nodes);
    writep(of, statistics::n48_nodes);
    writep(of, statistics::n256_nodes);
    writep(of, statistics::node256_occupants);
    writep(of, statistics::leaf_nodes);
    writep(of, statistics::page_bytes_compressed);
    writep(of, statistics::page_bytes_uncompressed);
    writep(of, statistics::pages_uncompressed);
    writep(of, statistics::pages_compressed);
    writep(of, statistics::max_page_bytes_uncompressed);
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
    readp(in, statistics::page_bytes_compressed);
    readp(in, statistics::page_bytes_uncompressed);
    readp(in, statistics::pages_uncompressed);
    readp(in, statistics::pages_compressed);
    readp(in, statistics::max_page_bytes_uncompressed);
}

bool art::tree::save() {
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    auto *t = this;
    if (nodes.get_main().get_bytes_allocated()==0) return true;
    bool saved = false;
    node_ptr troot;
    size_t tsize;
    auto save_stats_and_root = [&](std::ofstream &of) {
        if (!saved) {
            abort_with("synch error");
        }
        stats_to_stream(of);
        auto root = logical_address(troot.logical);
        writep(of, root);
        writep(of, troot.is_leaf);
        writep(of, tsize);
    };

    auto st = std::chrono::high_resolution_clock::now();
    transaction tx(this); // stabilize main while saving
    arena::hash_arena leaves{get_leaves().get_name()};
    arena::hash_arena nodes{get_nodes().get_name()};
    {
        storage_release release(latch); // only lock during partial copy
        tsize = t->size;
        troot = t->root;
        saved = true;
        leaves.borrow(get_leaves().get_main());
        nodes.borrow(get_nodes().get_main());
    }
    if (!get_leaves().save_extra(leaves, ".dat", save_stats_and_root)) {
        return false;
    }


    if (!get_nodes().save_extra(nodes, ".dat", [&](std::ofstream &) {
    })) {
        return false;
    }

    auto current = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(current - st);
    const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(current - st);

    std_log("saved barch db:", t->size, "keys written in", d.count(), "millis or", (float) dm.count() / 1000000,
            "seconds");
    return true;
}

bool art::tree::load() {

    //
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    try {
        storage_release release(latch);
        auto *t = this;
        logical_address root{nullptr};
        bool is_leaf = false;
        // save stats in the leaf storage
        auto load_stats_and_root = [&](std::ifstream &in) {
            stream_to_stats(in);

            readp(in, root);
            readp(in, is_leaf);
            readp(in, t->size);
        };
        auto st = std::chrono::high_resolution_clock::now();

        if (!get_nodes().load_extra(".dat", [&](std::ifstream &) {
        })) {
            return false;
        }
        if (!get_leaves().load_extra(".dat", load_stats_and_root)) {
            return false;
        }
        root = logical_address{root.address(), this};// translate root to the now
        if (is_leaf) {

            t->root = node_ptr{root};
        } else {
            t->root = resolve_read_node(root);
        }
        page_modifications::inc_all_tickers();
        auto now = std::chrono::high_resolution_clock::now();
        const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(now - st);
        const auto dm = std::chrono::duration_cast<std::chrono::microseconds>(now - st);
        std_log("Done loading BARCH, keys loaded:", t->size, "");

        std_log("loaded barch db in", d.count(), "millis or", (float) dm.count() / 1000000, "seconds");
        std_log("db memory when created", (float) heap::allocated / (1024 * 1024), "Mb");
    }catch (std::exception &e) {
        std_log("could not save",e.what());
        return false;
    }
    return true;
}

void art::tree::begin() {
    if (transacted) return;
    save_root = root;
    save_size = size;
    save_stats.clear();
    stats_to_stream(save_stats);
    {
       storage_release release(latch);
        get_leaves().begin();
        get_nodes().begin();

    }
    transacted = true;
}

void art::tree::commit() {
    if (!transacted) return;
    storage_release release(latch);
    get_leaves().commit();
    get_nodes().commit();
    transacted = false;
}

void art::tree::rollback() {
    if (!transacted) return;
    storage_release release(latch);
    get_leaves().rollback();
    get_nodes().rollback();
    root = save_root;
    size = save_size;
    save_stats.seek(0);
    stream_to_stats(save_stats);
    transacted = false;
}

void art::tree::clear() {
    std::unique_lock guard(save_load_mutex); // prevent save and load from occurring concurrently
    storage_release release(latch);
    root = {nullptr};
    size = 0;
    transacted = false;
    get_leaves().clear();
    get_nodes().clear();
    statistics::n4_nodes = 0;
    statistics::n16_nodes = 0;
    statistics::n48_nodes = 0;
    statistics::n256_nodes = 0;
    statistics::node256_occupants = 0;
    statistics::leaf_nodes = 0;
    statistics::page_bytes_compressed = 0;
    statistics::page_bytes_uncompressed = 0;
    statistics::pages_uncompressed = 0;
    statistics::pages_compressed = 0;
    statistics::max_page_bytes_uncompressed = 0;
    //destroy_leaf_compression();
    //destroy_node_compression();
}

void art::tree::update_trace(int direction) {
#if 1
    if (!trace.empty()) {
        if (trace[0].parent != root) {
            std_log("trace root invalid");
            abort_with("invalid trace root");
        }
        auto trd = trace[0].parent->data().descendants;
        if (trd + direction != size) {
            std_err("descendant count invalid", trd, "!=", size);
            abort_with("invalid descendant count");
        }
        for (auto &ut: trace) {
            ut.parent.modify()->data().descendants += direction;
        }
        trd = trace[0].parent->data().descendants;
        if (trd != size) {
            std_err("descendant count invalid", trd, "!=", size);
            abort_with("invalid descendant count");
        }
    }
#endif
}
