#pragma once
#include <cstdint>
#include <bitset>
#include <vector>
#include "valkeymodule.h"
#include "statistics.h"
#include "vector.h"

#pragma once
#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define MAX_PREFIX_LEN 12

/**
 * utility to create N copies of unsigned character C c
 */
template<unsigned N, typename C = unsigned char>
struct nuchar {
    C data[N];
    nuchar(C c){
        memset(data, c, sizeof(data));
    }
    operator const C* () const {
        return data;
    } 
};

struct art_node;
struct art_leaf;

struct node_ptr {
    
    bool is_leaf;
    node_ptr() : is_leaf(false), node(nullptr) {}
    node_ptr(const node_ptr& p) : is_leaf(p.is_leaf), node(p.node) {}
    
    node_ptr(art_node* p) : is_leaf(false), node(p) {}
    node_ptr(art_leaf* p) : is_leaf(true), l(p) {}
    node_ptr(const nullptr_t n) : is_leaf(false), l(n) {}
    
    bool null() const {
        return node == nullptr;
    }

    union {
        art_node* node;
        art_leaf* l;
    };
    void set(nullptr_t n) {
        node = n;
        is_leaf = false;
    }
    void set(art_node* n) {
        node = n;
        is_leaf = false;
    }
    
    void set(art_leaf* lf) {
        l = lf;
        is_leaf = true;
    }

    node_ptr& operator = (art_node* n){
        set(n);
        return *this;
    }
    
    node_ptr& operator = (art_leaf* l){
        set(l);
        return *this;
    }

    node_ptr& operator = (nullptr_t l){
        set(l);
        return *this;
    }

    node_ptr& operator = (const node_ptr& l){
        is_leaf = l.is_leaf;
        node = l.node;
        return *this;
    }

    art_leaf* leaf(){
        if (!is_leaf)
            abort();
        return is_leaf ? l : nullptr;
    }

    const art_leaf* leaf() const {
        if (!is_leaf)
            abort();
        return is_leaf ? l : nullptr;
    }
    
    art_node * operator -> (){
        if(is_leaf) {
            abort();
        }
        return node;
    }

    const art_node * operator -> () const {
         if(is_leaf) {
            abort();
        }
        return node;
    }

    operator const art_node* () const {
        return node;
    }
    operator art_node* () {
        return node;
    }
    
};
struct trace_element {
    node_ptr el;
    node_ptr child;
    unsigned child_ix;
    bool empty() const {
        return el.null() && child.null() && child_ix == 0;
    }
};

struct art_node {
    uint8_t partial_len = 0;
    uint8_t num_children = 0;
    unsigned char partial[MAX_PREFIX_LEN];
    art_node();
    virtual ~art_node();
    [[nodiscard]] virtual uint8_t type() const = 0;
    virtual void set_leaf(unsigned at) = 0;
    virtual void set_child(unsigned at, node_ptr child) = 0;
    [[nodiscard]] virtual bool is_leaf(unsigned at) const = 0;
    [[nodiscard]] virtual bool has_child(unsigned at) const = 0;
    virtual node_ptr get_node(unsigned at) = 0;
    [[nodiscard]] virtual const node_ptr get_node(unsigned at) const = 0;
    virtual node_ptr get_child(unsigned at) = 0;
    [[nodiscard]] virtual const node_ptr get_child(unsigned at) const = 0;
    [[nodiscard]] virtual unsigned index(unsigned char, unsigned operbits) const = 0;
    [[nodiscard]] virtual unsigned index(unsigned char c) const = 0;
    [[nodiscard]] virtual node_ptr find(unsigned char, unsigned operbits) const = 0;
    [[nodiscard]] virtual node_ptr find(unsigned char) const = 0;
    [[nodiscard]] virtual node_ptr last() const = 0;
    [[nodiscard]] virtual unsigned last_index() const = 0;
    virtual void remove(node_ptr& ref, unsigned pos, unsigned char key) = 0;
    virtual void add_child(unsigned char c, node_ptr& ref, node_ptr child) = 0;
    [[nodiscard]] virtual const unsigned char& get_key(unsigned at) const = 0;
    virtual unsigned char& get_key(unsigned at) = 0;
    unsigned check_prefix(const unsigned char *, unsigned, unsigned);
    [[nodiscard]] virtual unsigned first_index() const = 0;
    [[nodiscard]] virtual std::pair<trace_element, bool> lower_bound_child(unsigned char c) = 0;
};


typedef std::vector<trace_element> trace_list;

extern art_node* alloc_node(uint8_t type);
extern void free_node(art_node *n);
void free_node(art_leaf *n);

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
struct art_leaf {
    void *value;
    uint32_t key_len;
    unsigned char key[];
    /**
     * Checks if a leaf matches
     * @return 0 on success.
     */
    unsigned compare(const unsigned char *key, unsigned key_len, unsigned depth) {
        (void)depth;
        // Fail if the key lengths are different
        if (this->key_len != (uint32_t)key_len) return 1;

        // Compare the keys starting at the depth
        return memcmp(this->key, key, key_len);
    }
} ;

/**
 * node content to do common things related to keys and pointers on each node
 */
template<unsigned SIZE, unsigned KEYS>
struct node_content : public art_node {
    node_content() : types(0) {};
    ~node_content() override{};
    void set_leaf(unsigned at) final {
        if (SIZE < at) 
            abort();
        types.set(at, true);
    }
    void set_child(unsigned at, node_ptr node) final {
        if (SIZE < at) 
            abort();
        types.set(at, node.is_leaf);
        children[at] = node;
    }
    [[nodiscard]] bool is_leaf(unsigned at) const final {
        if (SIZE < at) 
            abort();
        bool is = types.test(at);
        return is;
    }
    [[nodiscard]] bool has_child(unsigned at) const final {
        if (SIZE < at) 
            abort();
        return children[at] ;
    }
    [[nodiscard]] const node_ptr get_node(unsigned at) const final {
        
        if (at < SIZE) 
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);
        
        return nullptr;
    }
    node_ptr get_node(unsigned at) final {
        
        if (at < SIZE) 
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);
        
        return nullptr;
    }
    
    node_ptr get_child(unsigned at) final {
        return get_node(at);
    }
    [[nodiscard]] const node_ptr get_child(unsigned at) const final {
        return get_node(at);
    }


    // TODO: NB check where this function is used
    [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override {
        unsigned i; 
        if (KEYS < num_children) {
            return num_children;
        }
        if (operbits & (OPERATION_BIT::eq & OPERATION_BIT::gt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] >= c)
                    return i; 
            }
            if (operbits == (OPERATION_BIT::eq & OPERATION_BIT::gt)) return num_children;
        }
        if (operbits & (OPERATION_BIT::eq & OPERATION_BIT::lt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] <= c)
                    return i; 
            }
            if (operbits == (OPERATION_BIT::eq & OPERATION_BIT::lt)) return num_children;
        }
        if (operbits & OPERATION_BIT::eq) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] == c)
                    return i; 
            }
        }
        if (operbits & OPERATION_BIT::gt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] > c)
                    return i;
            }
        }
        if (operbits & OPERATION_BIT::lt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] < c)
                    return i;
            }
        }
        return num_children;
    }
    [[nodiscard]] unsigned index(unsigned char c) const override {
        return index(c, OPERATION_BIT::eq);
    }
    [[nodiscard]] node_ptr find(unsigned char c) const override {
        return get_child(index(c));
    }
    [[nodiscard]] node_ptr find(unsigned char c, unsigned operbits) const override {
        return get_child(index(c,operbits));
    }
    [[nodiscard]] unsigned first_index() const override {
        return 0;
    };

    unsigned char keys[KEYS]{};

    [[nodiscard]] const unsigned char& get_key(unsigned at) const final {
        if(at < KEYS)
            return keys[at];
        abort();
    }

    unsigned char& get_key(unsigned at) final {
        if(at < KEYS)
            return keys[at];
        abort();
    }

    bool has_any(unsigned pos){
        if (SIZE < pos) 
            abort();
        
        return children[pos] != nullptr;
    }
    
    void set_keys(const unsigned char* other_keys, unsigned count){
        if (KEYS < count )
            abort();
        memcpy(keys, other_keys, count);
    }
    [[nodiscard]] const art_node * get_children() const {
        return (art_node *)children;
    }
    art_node* get_children() {
        return &children[0];
    }
    void insert_type(unsigned pos) {
        if (SIZE < pos) 
            abort();
        unsigned count = num_children;
        for (unsigned p = count; p > pos; --p) {
            types[p] = types[p - 1]; 
        }
    }
    void remove_type(unsigned pos) {
        if (SIZE < pos) 
            abort();
        unsigned count = num_children;
        for (unsigned p = pos; p < count -1; ++p) {
            types[p] = types[p + 1]; 
        }
    }
    template<typename S>
    void set_children(unsigned dpos, const S* other, unsigned spos, unsigned count){
        if (dpos < SIZE && count < SIZE) {
            
            memcpy(children + dpos, other->get_children() + spos, count*sizeof(void*));

            for(unsigned t = 0; t < count; ++t) {
                types[t+dpos] = other->types[t+spos];
            }
        }else {
            abort();
        }
    }
    template<typename S>
    void set_children(unsigned pos, const S* other, unsigned count){
        set_children(pos, other, 0, count);
    }
    
    void remove_child(unsigned pos) {
        if(pos < KEYS && KEYS == SIZE) {
            memmove(keys+pos, keys+pos+1, num_children - 1 - pos);
            memmove(children+pos, children+pos+1, (num_children - 1 - pos)*sizeof(void*));
            remove_type(pos);
            children[num_children - 1] = nullptr;
            num_children--;
        } else {
            abort();
        }
        
        
    }
    template<typename D, typename S> 
    void copy_header(D *dest, S *src) {
        if (src->num_children > SIZE) {
            abort();
        }
        dest->num_children = src->num_children;
        dest->partial_len = src->partial_len;
        memcpy(dest->partial, src->partial, std::min<unsigned>(MAX_PREFIX_LEN, src->partial_len));
    }
    std::bitset<SIZE> types;
    //std::bitset<SIZE> encoded;
protected:
    
    union
    {
        art_leaf *leaves[SIZE]{};
        art_node *children[SIZE];
    };
};

struct art_node16;
struct art_node48;
struct art_node256;

/**
 * Small node with only 4 children
 */

struct art_node4 final : public node_content<4, 4> {
    
    art_node4();
    ~art_node4() override;
    [[nodiscard]] virtual uint8_t type() const ;
    void remove(node_ptr& ref, unsigned pos, unsigned char key) override;

    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override ;
    [[nodiscard]] node_ptr last() const override ;
    [[nodiscard]] unsigned last_index() const override ;
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override;
};

/**
 * Node with 16 children
 */
struct art_node16 final : public node_content<16,16> {
    art_node16();
    ~art_node16() override;
    [[nodiscard]] uint8_t type() const override ;
    [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override ;
    
    
    // unsigned pos = l - children;
    void remove(node_ptr& ref, unsigned pos, unsigned char key) override;
    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override;
    [[nodiscard]] node_ptr last() const override;
    [[nodiscard]] unsigned last_index() const override;
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override;
    
};

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
struct art_node48 final : public node_content<48,256> {    
    art_node48();
    ~art_node48() override;
    [[nodiscard]] uint8_t type() const override ;
    [[nodiscard]] unsigned index(unsigned char c) const override ;
    
    void remove(node_ptr& ref, unsigned pos, unsigned char key) override;
    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override ;
    [[nodiscard]] node_ptr last() const override ;
    [[nodiscard]] unsigned last_index() const override;
    [[nodiscard]] unsigned first_index() const override;
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override;
    
} ;

/**
 * Full node with 256 children
 */
struct art_node256 final : public node_content<256,0> {
    art_node256();
    ~art_node256() override;
    [[nodiscard]] uint8_t type() const override ;
    [[nodiscard]] unsigned index(unsigned char c) const override ;
    
    void remove(node_ptr& ref, unsigned pos, unsigned char key) override ;

    void add_child(unsigned char c, node_ptr&, node_ptr child) override ;
    [[nodiscard]] node_ptr last() const override ;
    [[nodiscard]] unsigned last_index() const override ;
    [[nodiscard]] unsigned first_index() const override ;
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override ;

};
template <typename T> 
static T* get_node(art_node* n){
    return static_cast<T*>(n);
}
template <typename T> 
static T* get_node(node_ptr n){
    return static_cast<T*>(n.node);
}
template <typename T> 
static const T* get_node(const node_ptr n){
    return static_cast<const T*>(n.node);
}
template <typename T> 
static const T* get_node(const art_node* n) {
    return static_cast<T*>(n);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */

template<typename art_node_t>
art_node_t* alloc_node() {
    return new (ValkeyModule_Calloc(1, sizeof(art_node_t))) art_node_t();
}

/**
 * free a node while updating statistics 
 */
extern void free_node(art_leaf *n);

extern void free_node(node_ptr n);

extern void free_node(art_node *n);

