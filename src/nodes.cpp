#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <vector>
#include <atomic>

#include "art.h"
#include "statistics.h"
#include "vector.h"
#include "nodes.h"


void free_node(art_leaf *n){
    if(!n) return;
    unsigned kl = n->key_len;
    ValkeyModule_Free(n);
    --statistics::leaf_nodes;
    statistics::node_bytes_alloc -= (sizeof(art_leaf) + kl);
}

void free_node(node_ptr n){
    if (n.is_leaf) {
        free_node(n.leaf());
    } else {
        free_node(n.node);
    }

}
/**
 * free a node while updating statistics 
 */
void free_node(art_node *n) {
    // Break if null
    if (!n) return;

    n->~art_node();
    // Free ourself on the way up
    ValkeyModule_Free(n);
}


art_node::art_node () {}
art_node::~art_node() {}
/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */

unsigned art_node::check_prefix(const unsigned char *key, unsigned key_len, unsigned depth) {
    unsigned max_cmp = std::min<unsigned>(std::min<unsigned>(partial_len, max_prefix_llength), key_len - depth);
    unsigned idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

art_node4::art_node4() { 
    statistics::node_bytes_alloc += sizeof(art_node4);
    statistics::interior_bytes_alloc += sizeof(art_node4);
    statistics::n4_nodes++;
}

art_node4::~art_node4() {
    statistics::node_bytes_alloc -= sizeof(art_node4);
    statistics::interior_bytes_alloc -= sizeof(art_node4);
    statistics::n4_nodes--;
}

uint8_t art_node4::type() const {
    return node_4;
}

void art_node4::remove(node_ptr& ref, unsigned pos, unsigned char ) {
   
    remove_child(pos);

    // Remove nodes with only a single child
    if (num_children == 1) {
        node_ptr child = get_child(0);
        if (!child.is_leaf) {
            // Concatenate the prefixes
            unsigned prefix = partial_len;
            if (prefix < max_prefix_llength) {
                partial[prefix] = keys[0];
                prefix++;
            }
            if (prefix < max_prefix_llength) {
                unsigned sub_prefix = std::min<unsigned>(child->partial_len, max_prefix_llength - prefix);
                memcpy(partial+prefix, child->partial, sub_prefix);
                prefix += sub_prefix;
            }

            // Store the prefix in the child
            memcpy(child->partial, partial, std::min<unsigned>(prefix, max_prefix_llength));
            child->partial_len += partial_len + 1;
        }
        ref = child;
        free_node(this);
        
    }
}

void art_node4::add_child(unsigned char c, node_ptr &ref, node_ptr child)
{

    if (num_children < 4) {
        unsigned idx = index(c, OPERATION_BIT::gt|OPERATION_BIT::eq);
        // Shift to make room
        memmove(keys+idx+1, keys+idx, num_children - idx);
        memmove(children+idx+1, children+idx,
                (num_children - idx)*sizeof(void*));
        insert_type(idx);
        // Insert element
        keys[idx] = c;
        set_child(idx, child);
        num_children++;

    } else {
        auto *new_node = alloc_node<art_node16>();

        // Copy the child pointers and the key map
        new_node->set_children(0, this, num_children);
        new_node->set_keys(keys, num_children);
        copy_header(new_node, this);
        ref = new_node;
        free_node(this);
        new_node->add_child(c, ref, child);
    }
}

node_ptr art_node4::last() const {
    unsigned  idx = num_children - 1;
    return get_child(idx);
}

unsigned art_node4::last_index() const {
    return num_children - 1;
}

std::pair<trace_element, bool> art_node4::lower_bound_child(unsigned char c)
{
    for (unsigned i=0 ; i < num_children; i++) {

        if (keys[i] >= c && has_child(i)){
            return {{this,get_child(i),i},keys[i] == c};
        }
    }
    return {{nullptr,nullptr,num_children},false};
}

trace_element art_node4::next(const trace_element& te)
{
    unsigned i = te.child_ix + 1;
    if (i < num_children) {
        return {this,get_child(i),i};
    }
    return {};
}
trace_element art_node4::previous(const trace_element& te)
{
    unsigned i = te.child_ix;
    if (i > 0) {
        return {this,get_child(i),i-1};
    }
    return {};
}

art_node16::art_node16() { 
    statistics::node_bytes_alloc += sizeof(art_node16);
    statistics::interior_bytes_alloc += sizeof(art_node16);
    ++statistics::n16_nodes;
}
art_node16::~art_node16() {
    statistics::node_bytes_alloc -= sizeof(art_node16);
    statistics::interior_bytes_alloc -= sizeof(art_node16);
    --statistics::n16_nodes;
}

uint8_t art_node16::type() const {
    return node_16;
}

unsigned art_node16::index(unsigned char c, unsigned operbits) const {
    unsigned i = bits_oper16(keys, nuchar<16>(c), (1 << num_children) - 1, operbits);
    if (i) {
        i = __builtin_ctz(i);
        return i;
    }
    return num_children;
}

void art_node16::remove(node_ptr& ref, unsigned pos, unsigned char) {
    remove_child(pos);
    
    if (num_children == 3) {
        auto *new_node = alloc_node<art_node4>();
        copy_header(new_node, this);
        new_node->set_keys(keys, 3);
        new_node->set_children(0, this, 3);

        ref = new_node;   
        free_node(this); // ???
    }
} 
void art_node16::add_child(unsigned char c, node_ptr& ref, node_ptr child) {
    if (num_children < 16) {
        unsigned mask = (1 << num_children) - 1;
        
        unsigned bitfield = bits_oper16(nuchar<16>(c), keys, mask, OPERATION_BIT::lt);
        
        // Check if less than any
        unsigned idx;
        if (bitfield) {
            idx = __builtin_ctz(bitfield);
            memmove(keys+idx+1,keys+idx,num_children-idx);
            memmove(children+idx+1,children+idx,
                    (num_children-idx)*sizeof(void*));
        } else
            idx = num_children;
        
        insert_type(idx);

        // Set the child
        keys[idx] = c;
        set_child(idx, child);
        num_children++;

    } else {
        auto *new_node = alloc_node<art_node48>();

        // Copy the child pointers and populate the key map
        new_node->set_children(0, this, num_children);
        for (unsigned i=0;i< num_children;i++) {
            new_node->keys[keys[i]] = i + 1;
        }
        copy_header(new_node, this);
        ref = new_node;
        free_node(this);
        new_node->add_child(c, ref, child);
    }

}
node_ptr art_node16::last() const {
    return get_child(num_children - 1);
}
unsigned art_node16::last_index() const{
    return num_children - 1;
}

std::pair<trace_element, bool> art_node16::lower_bound_child(unsigned char c)
{
    unsigned mask = (1 << num_children) - 1;
    unsigned bf = bits_oper16(keys, nuchar<16>(c), mask, OPERATION_BIT::eq | OPERATION_BIT::gt); // inverse logic
    if (bf) {
        unsigned i = __builtin_ctz(bf);
        return {{this,get_child(i),i}, keys[i]==c};
    }
    return {{nullptr,nullptr,num_children},false};
}
trace_element art_node16::next(const trace_element& te)
{
    unsigned i = te.child_ix + 1;
    if (i < num_children) {
        return {this,get_child(i),i};// the keys are ordered so fine I think
    }
    return {};
}
trace_element art_node16::previous(const trace_element& te)
{
    unsigned i = te.child_ix;
    if (i > 0) {
        return {this,get_child(i),i-1};// the keys are ordered so fine I think
    }
    return {};
}


art_node48::art_node48(){ 
    statistics::node_bytes_alloc += sizeof(art_node48);
    statistics::interior_bytes_alloc += sizeof(art_node48);
    ++statistics::n48_nodes;
}
art_node48::~art_node48() {
    statistics::node_bytes_alloc -= sizeof(art_node48);
    statistics::interior_bytes_alloc -= sizeof(art_node48);
    --statistics::n48_nodes;
}

uint8_t art_node48::type() const{
    return node_48;
}

unsigned art_node48::index(unsigned char c) const {
    unsigned  i = keys[c];
    if (i)
        return i-1;
    return 256;
}
    
    
 void art_node48::remove(node_ptr& ref, unsigned pos, unsigned char key){
    if((unsigned)keys[key] -1 != pos) {
        abort();
    }
    if (keys[key] == 0){
        return;
    }
    if (children[pos] == nullptr) {
        abort();
    }
    keys[key] = 0;
    children[pos] = nullptr;
    types[pos] = false;
    num_children--;
    
    if (num_children == 12) {
        art_node16 *new_node = alloc_node<art_node16>();
        copy_header(new_node, this);
        unsigned child = 0;
        for (unsigned i = 0; i < 256; i++) {
            pos = keys[i];
            if (pos) {
                node_ptr nn = get_child(pos - 1);
                if (!nn) {
                    abort();
                }
                new_node->keys[child] = i;
                new_node->set_child(child, nn); 
                child++;
            }
        }
        ref = new_node;    
        free_node(this);
    }
    
}

void art_node48::add_child(unsigned char c, node_ptr& ref, node_ptr child) {
    if (num_children < 48) {
        unsigned pos = 0; 
        while (has_child(pos)) pos++;
        // not we do not need to call insert_type an empty child is found
        set_child(pos, child);
        keys[c] = pos + 1;
        num_children++;
    } else {
        auto *new_node = alloc_node<art_node256>();
        for (unsigned i = 0;i < 256; i++) {
            if (keys[i]) {
                node_ptr nc = get_child(keys[i] - 1);
                if(!nc) {
                    abort();
                }
                new_node->set_child(i, nc);
            }
        }
        copy_header(new_node, this);
        statistics::node256_occupants += new_node->num_children;
        ref = new_node;
        free_node(this);
        new_node->add_child(c, ref, child);
    }
}

node_ptr art_node48::last() const {
    return get_child(last_index());
}
unsigned art_node48::last_index() const {
    unsigned idx=255;
    while (!keys[idx]) idx--;
    return idx - 1;
}
unsigned art_node48::first_index() const {
    unsigned uc = 0; // ?
    unsigned i;
    for (; uc < 256;uc++){
        i = keys[uc];
        if(i > 0){
            return i-1;
        }
    }
    return uc;
}

std::pair<trace_element, bool> art_node48::lower_bound_child(unsigned char c)
{
    /*
    * find first not less than
    * todo: make lb faster by adding bit map index and using __builtin_ctz as above
    */
    unsigned uc = c;
    unsigned i = 0;
    for (; uc < 256;uc++){
        i = keys[uc];
        if (i > 0) {
            trace_element te = {this, get_child(i-1),i-1};
            return {te, (i == c)};
        }
    }
    return {{nullptr,nullptr,256},false};

}
trace_element art_node48::next(const trace_element& te)
{
    unsigned uc = te.child_ix, i;

    for (; uc > 0; --uc){
        i = keys[uc];
        if(i > 0){
            return {this,get_child(i-1),i-1};
        }
    }
    return {};
}
trace_element art_node48::previous(const trace_element& te)
{
    unsigned uc = te.child_ix + 1, i;
    for (; uc < 256;uc++){
        i = keys[uc];
        if(i > 0){
            return {this,get_child(i-1),i-1};
        }
    }
    return {};
}


art_node256::art_node256() { 
    statistics::node_bytes_alloc += sizeof(art_node256);
    statistics::interior_bytes_alloc += sizeof(art_node256);
    ++statistics::n256_nodes;
}

art_node256::~art_node256() {
    statistics::node256_occupants -= num_children;
    statistics::node_bytes_alloc -= sizeof(art_node256);
    statistics::interior_bytes_alloc -= sizeof(art_node256);
    --statistics::n256_nodes;
}
uint8_t art_node256::type() const {
    return node_256;
}

unsigned art_node256::index(unsigned char c) const {
    if (children[c])
        return c;
    return 256;
}
    
 void art_node256::remove(node_ptr& ref, unsigned pos, unsigned char key) {
    if(key != pos) {
        abort();
    }
    children[key] = nullptr;
    types.set(key, false);
    num_children--;
    --statistics::node256_occupants;
    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (num_children == 37) {
        auto *new_node = alloc_node<art_node48>();
        ref = new_node;
        copy_header(new_node, this);
    
        unsigned pos = 0;
        for (unsigned i = 0; i < 256; i++) {
            if (has_any(i)) {
                new_node->set_child(pos, get_child(i)); //[pos] = n->children[i];
                new_node->keys[i] = pos + 1;
                pos++;
            }
        }
        
        free_node(this);   
    }
}

void art_node256::add_child(unsigned char c, node_ptr&, node_ptr child) {
    if(!has_child(c)) {
        ++statistics::node256_occupants;
        ++num_children; // just to keep stats ok
    }
    set_child(c, child);
}
node_ptr art_node256::last() const {
    return get_child(last_index());
}
unsigned art_node256::last_index() const {
    unsigned idx = 255;
    while (!has_child(idx)) idx--;
    return idx;
}

unsigned art_node256::first_index() const {
    unsigned uc = 0; // ?
    for (; uc < 256; uc++){
        if(children[uc]) {
            return uc;
        }
    }
    return uc;
}

std::pair<trace_element, bool> art_node256::lower_bound_child(unsigned char c)
{
    for (unsigned i = c; i < 256; ++i) {
        if (has_child(i)) {
            // because nodes are ordered accordingly
            return {{this,get_child(i), i}, (i == c)};
        }
    }
    return {{nullptr,nullptr,256},false};
}

trace_element art_node256::next(const trace_element& te)
{
    for (unsigned i = te.child_ix+1; i < 256; ++i) { // these aren't sparse so shouldn't take long
        if (has_child(i)) {// because nodes are ordered accordingly
            return {this,get_child(i),i};
        }
    }
    return {};
}

trace_element art_node256::previous(const trace_element& te)
{
    if(!te.child_ix) return {};
    for (unsigned i = te.child_ix-1; i > 0; --i) { // these aren't sparse so shouldn't take long
        if (has_child(i)) {// because nodes are ordered accordingly
            return {this,get_child(i),i};
        }
    }
    return {};
}

