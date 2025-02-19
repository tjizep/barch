#pragma once
#include <algorithm>
#include <cstdint>
#include <bitset>
#include <limits>
#include <vector>
#include <bits/ios_base.h>

#include "valkeymodule.h"
#include "statistics.h"
#include "simd.h"
#include <array>
#include "compress.h"

template<typename I>
int64_t i64max()
{
    return std::numeric_limits<I>::max();
}
template<typename I>
int64_t i64min()
{
    return std::numeric_limits<I>::min();
}

enum node_kind
{
    node_4 = 1u,
    node_16 = 2u,
    node_48 = 3u,
    node_256 = 4u
};

enum constants
{
    max_prefix_llength = 10u,
    max_alloc_children = 8u,
    initial_node = node_4
};
struct art_leaf;
typedef compressed_address logical_leaf;
extern compress& get_leaf_compression();

template<typename art_node_t>
struct node_ptr_t {
    
    bool is_leaf;
    node_ptr_t() : is_leaf(false), resolver(nullptr), node(nullptr) {};
    node_ptr_t(const node_ptr_t& p) : is_leaf(p.is_leaf), resolver(p.resolver), logical(p.logical), node(p.node) {}
    
    explicit node_ptr_t(art_node_t* p) : is_leaf(false), resolver(nullptr), node(p) {}
    node_ptr_t(const art_node_t* p) : is_leaf(false), resolver(nullptr), node(const_cast<art_node_t*>(p)) {}
    //node_ptr_t(art_leaf* p) : is_leaf(true), l(p) {}
    // TODO: maybe this mechanism should work for interior nodes - one day
    node_ptr_t(compressed_address cl) : is_leaf(true), resolver(&get_leaf_compression()), logical(cl), l(nullptr)
    {
        l = resolver->resolve<art_leaf>(logical);
    }

    //node_ptr_t(const art_leaf* p) : is_leaf(true), l(const_cast<art_leaf*>(p)) {}
    node_ptr_t(std::nullptr_t) : is_leaf(false), resolver(nullptr), l(nullptr) {}

    bool null() const {
        return node == nullptr;
    }
    compress *resolver;
    compressed_address logical {};
    union {
        art_node_t* node;
        art_leaf* l;
    };
    void set(nullptr_t n) {
        node = n;
        is_leaf = false;
        logical = nullptr;
        resolver = nullptr;
    }
    void set(art_node_t* n) {
        node = n;
        logical = nullptr;
        is_leaf = false;
        resolver = nullptr;
    }
    
    void set(const logical_leaf& lf) {
        logical = lf;
        if(!resolver) resolver = &get_leaf_compression();
        l = resolver->resolve<art_leaf>(lf);
        is_leaf = true;
    }
    bool operator == (const node_ptr_t& p) const
    {
        return node == p.node && is_leaf == p.is_leaf;
    }
    bool operator != (const node_ptr_t& p) const
    {
        return node != p.node || is_leaf != p.is_leaf;
    }
    bool operator == (nullptr_t) const
    {
        return node == nullptr && is_leaf == false;
    }
    bool operator != (nullptr_t) const
    {
        return node != nullptr || is_leaf == false;
    }
    bool operator == (const art_node_t* p) const
    {
        return node == p && is_leaf == false;
    }
    bool operator != (const art_node_t* p) const
    {
        return node != p || is_leaf == true;
    }
    bool operator == (const art_leaf* p) const
    {
        return l == p && is_leaf ;
    }
    bool operator != (const art_leaf* p) const
    {
        return l != p || !is_leaf;
    }
    node_ptr_t& operator = (const node_ptr_t& n){
        this->is_leaf = n.is_leaf;
        this->node = n.node;
        this->logical = n.logical;
        this->resolver = n.resolver;
        check();
        return *this;
    }
    node_ptr_t& operator = (const art_node_t* n){
        set(const_cast<art_node_t*>(n));
        return *this;
    }

    node_ptr_t& operator = (art_node_t* n){
        set(n);
        return *this;
    }
    
    node_ptr_t& operator = (const logical_leaf& l){
        set(l);
        return *this;
    }

    node_ptr_t& operator = (nullptr_t l){
        set(l);
        return *this;
    }
    void check() const
    {
        if (is_leaf)
        {
            if(resolver == nullptr)
            {
                 abort();
            }
            if(logical.null() && l != nullptr)
            {
                abort();
            }
            if(!logical.null() && l == nullptr)
            {
                abort();
            }
        }
    }

    art_leaf* leaf(){
        if (!is_leaf)
            abort();
        check();
        return l;
    }
    art_node_t* get_node()
    {
        if(is_leaf) {
            abort();
        }
        check();
        return node;
    }
    const art_node_t* get_node() const
    {
        if(is_leaf) {
            abort();
        }

        return node;
    }

    [[nodiscard]] const art_leaf* leaf() const {
        if (!is_leaf)
            abort();
        check();
        return l;
    }
    
    art_node_t * operator -> () const {
        if(is_leaf) {
            abort();
        }
        return node;
    }

    explicit operator const art_node_t* () const {
        return get_node();
    }

    explicit operator art_node_t* () {
        return get_node();
    }
    
};



struct art_node {
    typedef node_ptr_t<art_node> node_ptr;
    struct trace_element {
        node_ptr parent = nullptr;
        node_ptr child = nullptr;
        unsigned child_ix = 0;
        unsigned char k = 0;
        [[nodiscard]] bool empty() const {
            return parent.null() && child.null() && child_ix == 0;
        }
    };
    typedef std::array<node_ptr, max_alloc_children> children_t;

    uint8_t partial_len = 0;
    uint8_t num_children = 0;
    unsigned char partial[max_prefix_llength]{};
    art_node();
    virtual ~art_node();
    [[nodiscard]] virtual uint8_t type() const = 0;
    virtual void set_leaf(unsigned at) = 0;
    virtual void set_child(unsigned at, node_ptr child) = 0;
    [[nodiscard]] virtual bool is_leaf(unsigned at) const = 0;
    [[nodiscard]] virtual bool has_child(unsigned at) const = 0;
    virtual node_ptr get_node(unsigned at) = 0;
    [[nodiscard]] virtual node_ptr get_node(unsigned at) const = 0;
    virtual node_ptr get_child(unsigned at) = 0;
    [[nodiscard]] virtual node_ptr get_child(unsigned at) const = 0;
    [[nodiscard]] virtual unsigned index(unsigned char, unsigned operbits) const = 0;
    [[nodiscard]] virtual unsigned index(unsigned char c) const = 0;
    [[nodiscard]] virtual node_ptr find(unsigned char, unsigned operbits) const = 0;
    [[nodiscard]] virtual node_ptr find(unsigned char) const = 0;
    [[nodiscard]] virtual node_ptr last() const = 0;
    [[nodiscard]] virtual unsigned last_index() const = 0;
    virtual void remove(node_ptr& ref, unsigned pos, unsigned char key) = 0;
    virtual void add_child(unsigned char c, node_ptr& ref, node_ptr child) = 0;
    virtual void add_child_inner(unsigned char , node_ptr){}
    [[nodiscard]] virtual const unsigned char& get_key(unsigned at) const = 0;
    virtual unsigned char& get_key(unsigned at) = 0;
    unsigned check_prefix(const unsigned char *, unsigned, unsigned);
    [[nodiscard]] virtual unsigned first_index() const = 0;
    [[nodiscard]] virtual std::pair<trace_element, bool> lower_bound_child(unsigned char c) = 0;
    [[nodiscard]] virtual trace_element next(const trace_element& te) = 0;
    [[nodiscard]] virtual trace_element previous(const trace_element& te) = 0;
    virtual void set_keys(const unsigned char* other_keys, unsigned count) = 0;
    [[nodiscard]] virtual const unsigned char* get_keys() const = 0;
    virtual void set_key(unsigned at, unsigned char k) = 0;
    virtual void copy_from(node_ptr s) = 0;
    virtual void copy_header(node_ptr src) = 0;
    virtual void set_children(unsigned dpos, const art_node* other, unsigned spos, unsigned count) = 0;
    [[nodiscard]] virtual bool child_type(unsigned at) const = 0;
    // returns true if node does not need to be rebuilt
    [[nodiscard]] virtual bool ok_child(node_ptr np) const = 0;
    [[nodiscard]] virtual bool ok_children(const children_t& child) const = 0;
    [[nodiscard]] virtual unsigned ptr_size() const = 0;

    [[nodiscard]] virtual node_ptr expand_pointers(node_ptr& ref, const children_t& child)  = 0;
    void check_object() const
    {

    }
};
typedef art_node::node_ptr node_ptr;
typedef art_node::trace_element trace_element;
typedef art_node::children_t children_t;

art_node* alloc_node(unsigned nt, const children_t& child);
art_node* alloc_8_node(unsigned nt); // magic 8 ball

typedef std::vector<trace_element> trace_list;

uint64_t get_node_base();

void free_node(art_node *n);
void free_node(art_leaf *n);

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
struct art_leaf {
    void *value;
    unsigned short key_len;
    unsigned char key[];
    /**
     * Checks if a leaf matches
     * @return 0 on success.
     */
    int compare(const unsigned char *key, unsigned key_len, unsigned depth) const {
        (void)depth;
        // TODO: compare is broken will fail some edge cases
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
        check_object();
        if (SIZE <= at)
            abort();
        types.set(at, true);
    }
    void set_child(unsigned at, node_ptr node) final {
        check_object();
        if (SIZE <= at)
            abort();
        types.set(at, node.is_leaf);
        if(node.is_leaf)
        {
            leaves[at] = node.logical;
        }else
        {
            children[at] = node;
        }

    }
    [[nodiscard]] bool is_leaf(unsigned at) const final {
        check_object();
        if (SIZE <= at)
            abort();
        bool is = types.test(at);
        return is;
    }
    [[nodiscard]] bool has_child(unsigned at) const final {
        check_object();
        if (SIZE <= at)
            abort();
        return children[at] ;
    }
    [[nodiscard]] node_ptr expand_pointers(node_ptr&,const children_t& ) override
    {   check_object();
        return this;
    };
    [[nodiscard]] node_ptr get_node(unsigned at) const final {
        check_object();
        if (at < SIZE)
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);
        
        return nullptr;
    }
    node_ptr get_node(unsigned at) final {
        check_object();
        if (at < SIZE)
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);
        
        return nullptr;
    }
    
    node_ptr get_child(unsigned at) final {
        check_object();
        return get_node(at);
    }
    [[nodiscard]] node_ptr get_child(unsigned at) const final {
        check_object();
        return get_node(at);
    }
    [[nodiscard]] bool ok_child(node_ptr ) const override
    {   check_object();
        return true;
    }
    [[nodiscard]] bool ok_children(const children_t& ) const override
    {
        return true;
    }


    // TODO: NB check where this function is used
    [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override {
        check_object();
        unsigned i;
        if (KEYS < num_children) {
            return num_children;
        }
        if (operbits & (eq & gt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] >= c)
                    return i; 
            }
            if (operbits == (eq & gt)) return num_children;
        }
        if (operbits & (eq & lt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] <= c)
                    return i; 
            }
            if (operbits == (eq & lt)) return num_children;
        }
        if (operbits & eq) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] == c)
                    return i; 
            }
        }
        if (operbits & gt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] > c)
                    return i;
            }
        }
        if (operbits & lt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] < c)
                    return i;
            }
        }
        return num_children;
    }
    [[nodiscard]] unsigned index(unsigned char c) const override {
        check_object();
        return index(c, eq);
    }
    [[nodiscard]] node_ptr find(unsigned char c) const override {
        check_object();
        return get_child(index(c));
    }
    [[nodiscard]] node_ptr find(unsigned char c, unsigned operbits) const override {
        check_object();
        return get_child(index(c,operbits));
    }
    [[nodiscard]] unsigned first_index() const override {
        check_object();
        return 0;
    };

    unsigned char keys[KEYS]{};
    [[nodiscard]] const unsigned char* get_keys() const override
    {   check_object();
        return keys;
    }
    [[nodiscard]] const unsigned char& get_key(unsigned at) const final {
        check_object();
        if(at < KEYS)
            return keys[at];
        abort();
    }

    void set_key(unsigned at, unsigned char k) final
    {   check_object();
        auto max_keys = KEYS;
        if(at < max_keys)
        {
            keys[at] = k;
            return;
        }
        abort();
    }

    unsigned char& get_key(unsigned at) final {
        check_object();
        if(at < KEYS)
            return keys[at];
        abort();
    }

    [[nodiscard]] bool has_any(unsigned pos) const {
        check_object();
        if (SIZE <= pos)
            abort();
        
        return children[pos] != nullptr;
    }
    
    void set_keys(const unsigned char* other_keys, unsigned count) override{
        check_object();
        if (KEYS < count )
            abort();
        memcpy(keys, other_keys, count);
    }
    void insert_type(unsigned pos) {
        check_object();
        if (SIZE <= pos)
            abort();
        unsigned count = num_children;
        for (unsigned p = count; p > pos; --p) {
            types[p] = types[p - 1]; 
        }
    }
    void remove_type(unsigned pos) {
        check_object();
        if (SIZE <= pos)
            abort();
        unsigned count = num_children;
        for (unsigned p = pos; p < count -1; ++p) {
            types[p] = types[p + 1]; 
        }
    }
    [[nodiscard]] bool child_type(unsigned at) const override
    {   check_object();
        return types[at];
    }

    void set_children(unsigned dpos, const art_node* other, unsigned spos, unsigned count) override {
        check_object();
        if (dpos < SIZE && count <= SIZE) {
            
            for(unsigned d = dpos; d < count; ++d )
            {
                children[d] = other->get_child(d+spos).node;
            }
            for(unsigned t = 0; t < count; ++t) {
                types[t+dpos] = other->child_type(t+spos);
            }
        }else {
            abort();
        }
    }
    template<typename S>
    void set_children(unsigned pos, const S* other, unsigned count){
        check_object();
        set_children(pos, other, 0, count);
    }
    
    void remove_child(unsigned pos) {
        check_object();
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
    void copy_header(node_ptr src) override {
        check_object();
        if (src->num_children > SIZE) {
            abort();
        }
        this->num_children = src->num_children;
        this->partial_len = src->partial_len;
        memcpy(this->partial, src->partial, std::min<unsigned>(max_prefix_llength, src->partial_len));
    }
    void copy_from(node_ptr s) override
    {   check_object();
        if(s->num_children > SIZE)
        {
            abort();
        }
        this->copy_header(s);
        set_keys(s->get_keys(), s->num_children);
        set_children(0, s, 0, s->num_children);
        if (num_children != s->num_children)
        {
            abort();
        }

    }
    [[nodiscard]] virtual unsigned ptr_size() const
    {   check_object();
        return sizeof(art_leaf*);
    };
    std::bitset<SIZE> types;
    //std::bitset<SIZE> encoded;
protected:
    
    union
    {
        logical_leaf leaves[SIZE]{};
        art_node *children[SIZE];
    };
};


template<typename EncodingType>
bool ok(const art_node* node, uintptr_t base)
{
    if(node == nullptr) return true;
    if(sizeof(EncodingType) == sizeof(uintptr_t)) return true;

    auto uval = reinterpret_cast<int64_t>(node);

    int64_t ival = uval-base ;
    int64_t imax = i64max<EncodingType>();
    int64_t imin = i64min<EncodingType>();
    return (ival > imin && ival < imax  );
}
template<typename EncodingType, bool IsLeaf>
struct encoded_element {
private:
    EncodingType &value;
    uintptr_t base;
public:
    encoded_element(EncodingType &value, uintptr_t base) : value(value), base(base) {}

    encoded_element& operator=(nullptr_t)
    {
        value = 0;
        return *this;
    }
    encoded_element& operator=(const node_ptr& t)
    {
        if(t.is_leaf != IsLeaf)
        {
            abort();
        }
        if(t.is_leaf)
        {
            set_leaf(t.logical);
        }else
        {
            set_node(t.get_node());
        }
        return *this;
    }
    void set_leaf(const logical_leaf& ptr)
    {
        if(!IsLeaf)
        {
            abort();
        }
        value = ptr.null() ? 0 : ptr.address();
    }
    [[nodiscard]] logical_leaf get_leaf() const
    {
        if(!IsLeaf)
        {
            abort();
        }
        return logical_leaf(value);
    }
    void set_node(const art_node* ptr)
    {
        if(IsLeaf)
        {
            abort();
        }
        if(sizeof(EncodingType) == sizeof(uintptr_t))
        {
            value = (EncodingType)(uintptr_t)ptr;
            return;
        }
        if(ptr == nullptr)
        {
            value = 0;
        }else
        {

            value = (reinterpret_cast<int64_t>(ptr) - base);

        }
    }
    [[nodiscard]] art_node* get_node()
    {
        if(IsLeaf)
        {
            abort();
        }

        if(sizeof(EncodingType) == sizeof(uintptr_t))
        {
            return reinterpret_cast<art_node*>(value);
        }
        if(!value) return nullptr;
        return reinterpret_cast<art_node*>((int64_t)value + base);
    }
    [[nodiscard]] const art_node* cget() const
    {
        if(IsLeaf)
        {
            abort();
        }

        return const_cast<encoded_element*>(this)->get_node();
    }

    encoded_element& operator=(const art_node* ptr)
    {
        set_node(ptr);
        return *this;
    }
    encoded_element& operator=(const compressed_address ptr)
    {
        set_leaf(ptr);
        return *this;
    }

    explicit operator logical_leaf()
    {
        return get_leaf();
    }
    explicit operator node_ptr()
    {
        if(IsLeaf)
        {
            return node_ptr(get_leaf());
        }
        return node_ptr(get_node());
    }

    explicit operator node_ptr() const
    {
        if(IsLeaf)
        {
            return node_ptr(get_leaf());
        }
        return cget(); // well have to just cast it
    }


    bool exists() const
    {
        return value != 0;
    }
    bool empty() const
    {
        return value == 0;
    }
};

template<typename EncodedType, bool IsLeaf, int SIZE>
struct node_array
{
    typedef encoded_element<EncodedType, IsLeaf> ProxyType;
    EncodedType data[SIZE];
    [[nodiscard]] uintptr_t get_offset() const
    {
        return (uintptr_t)this;
    }
    [[nodiscard]] bool ok(const node_ptr& p) const
    {
        if(p.is_leaf) return true;
        return ::ok<EncodedType>((const art_node*)p, get_offset());
    }

    ProxyType operator[](unsigned at)
    {
        return ProxyType(data[at], get_offset());
    }
    ProxyType operator[](unsigned at) const
    {
        return ProxyType(const_cast<EncodedType&>(data[at]), get_offset());
    }

};

template<unsigned SIZE, unsigned KEYS, typename i_ptr_t>
struct encoded_node_content : public art_node {

    // test somewhere that sizeof ChildElementType == sizeof LeafElementType
    typedef i_ptr_t ChildElementType;
    typedef i_ptr_t LeafElementType;
    typedef node_array<i_ptr_t, false, SIZE> ChildArrayType;
    typedef node_array<i_ptr_t, true, SIZE> LeafArrayType;

    encoded_node_content() : types(0)
    {
    };
    ~encoded_node_content() override{};
    void set_leaf(unsigned at) final {
        check_object();
        if (SIZE <= at)
            abort();
        types.set(at, true);
    }
    void set_child(unsigned at, node_ptr node) final {
        check_object();
        if (SIZE <= at)
            abort();
        types.set(at, node.is_leaf);
        if(node.is_leaf)
        {
            leaves[at] = node.logical;
        }else
        {
            children[at] = node;
        }

    }
    [[nodiscard]] bool is_leaf(unsigned at) const final {
        check_object();
        if (SIZE <= at)
            abort();
        bool is = types.test(at);
        return is;
    }
    [[nodiscard]] bool has_child(unsigned at) const final {
        check_object();
        if (SIZE <= at)
            abort();
        return children[at].exists() ;
    }
    [[nodiscard]] node_ptr get_node(unsigned at) const final {
        check_object();
        if (at < SIZE)
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);

        return nullptr;
    }

    node_ptr get_node(unsigned at) final {
        check_object();
        if (at < SIZE)
            return types[at] ? node_ptr(leaves[at]) : node_ptr(children[at]);

        return nullptr;
    }

    node_ptr get_child(unsigned at) final {
        check_object();
        return get_node(at);
    }
    [[nodiscard]] node_ptr get_child(unsigned at) const final {
        check_object();
        return get_node(at);
    }
    [[nodiscard]] bool ok_child(node_ptr np) const override
    {   check_object();
        if(np.is_leaf)
            return leaves.ok(np);
        return children.ok(np);
    }
    [[nodiscard]] bool ok_children(const children_t& child) const override
    {   check_object();
        for(auto np: child)
        {
            // TODO: compress leaf addresses again
            if(np.is_leaf)
            {
               return leaves.ok(np);

            }else
            {
                return children.ok(np);
            }
        }
        return true;
    }


    // TODO: NB check where this function is used
    [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override {
        check_object();
        unsigned i;
        if (KEYS < num_children) {
            return num_children;
        }
        if (operbits & (eq & gt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] >= c)
                    return i;
            }
            if (operbits == (eq & gt)) return num_children;
        }
        if (operbits & (eq & lt) ) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] <= c)
                    return i;
            }
            if (operbits == (eq & lt)) return num_children;
        }
        if (operbits & eq) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] == c)
                    return i;
            }
        }
        if (operbits & gt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] > c)
                    return i;
            }
        }
        if (operbits & lt) {
            for (i = 0; i < num_children; ++i) {
                if (keys[i] < c)
                    return i;
            }
        }
        return num_children;
    }
    [[nodiscard]] unsigned index(unsigned char c) const override {
        check_object();
        return index(c, eq);
    }
    [[nodiscard]] node_ptr find(unsigned char c) const override {
        check_object();
        return get_child(index(c));
    }
    [[nodiscard]] node_ptr find(unsigned char c, unsigned operbits) const override {
        check_object();
        return get_child(index(c,operbits));
    }
    [[nodiscard]] unsigned first_index() const override {
        check_object();
        return 0;
    };

    unsigned char keys[KEYS]{};
    [[nodiscard]] const unsigned char* get_keys() const override
    {   check_object();
        return keys;
    }
    [[nodiscard]] const unsigned char& get_key(unsigned at) const final {
        check_object();
        if(at < KEYS)
            return keys[at];
        abort();
    }

    void set_key(unsigned at, unsigned char k) final
    {   check_object();

        auto max_keys = KEYS;
        if(at < max_keys)
        {
            keys[at] = k;
            return;
        }
        abort();
    }

    unsigned char& get_key(unsigned at) final {
        check_object();
        if(at < KEYS)
            return keys[at];
        abort();
    }

    bool has_any(unsigned pos){
        check_object();
        if (SIZE <= pos)
            abort();

        return children[pos].exists();
    }

    void set_keys(const unsigned char* other_keys, unsigned count) override{
        check_object();
        if (KEYS < count )
            abort();
        memcpy(keys, other_keys, count);
    }

    void insert_type(unsigned pos) {
        check_object();
        if (SIZE <= pos)
            abort();
        unsigned count = num_children;
        for (unsigned p = count; p > pos; --p) {
            types[p] = types[p - 1];
        }
    }
    void remove_type(unsigned pos) {
        check_object();
        if (SIZE < pos)
            abort();
        unsigned count = num_children;
        for (unsigned p = pos; p < count -1; ++p) {
            types[p] = types[p + 1];
        }
    }
    [[nodiscard]] bool child_type(unsigned at) const override
    {   check_object();
        return types[at];
    }

    void set_children(unsigned dpos, const art_node* other, unsigned spos, unsigned count) override {
        check_object();
        if (dpos < SIZE && count <= SIZE) {

            for(unsigned d = dpos; d < count; ++d )
            {
                auto n = other->get_child(d+spos);
                if(n.is_leaf)
                {
                    leaves[d] = n.logical;
                }else
                    children[d] = n;
            }
            for(unsigned t = 0; t < count; ++t) {
                types[t+dpos] = other->child_type(t+spos);
            }
        }else {
            abort();
        }
    }
    template<typename S>
    void set_children(unsigned pos, const S* other, unsigned count){
        check_object();
        set_children(pos, other, 0, count);
    }

    void remove_child(unsigned pos) {
        check_object();
        if(pos < KEYS && KEYS == SIZE) {
            memmove(keys+pos, keys+pos+1, num_children - 1 - pos);
            memmove(children.data+pos, children.data+pos+1, (num_children - 1 - pos)*sizeof(ChildElementType));
            remove_type(pos);
            keys[num_children - 1] = 0;
            children[num_children - 1] = nullptr;
            num_children--;
        } else {
            abort();
        }


    }
    void copy_header(node_ptr src) override {
        check_object();
        if (src->num_children > SIZE) {
            abort();
        }
        this->num_children = src->num_children;
        this->partial_len = src->partial_len;
        memcpy(this->partial, src->partial, std::min<unsigned>(max_prefix_llength, src->partial_len));
    }
    void copy_from(node_ptr s) override
    {
        check_object();
        if(s->num_children > SIZE)
        {
            abort();
        }
        this->copy_header(s);
        set_keys(s->get_keys(), s->num_children);
        set_children(0, (art_node*)s, 0, s->num_children);
        if (num_children != s->num_children)
        {
            abort();
        }

    }
    [[nodiscard]] node_ptr expand_pointers(node_ptr& ref, const children_t& children) override
    {
        check_object();
        if(ok_children(children)) return this;
        node_ptr n = alloc_8_node(type());
        n->copy_from(this);
        free_node(this);
        ref = n;
        return n;

    }
    [[nodiscard]] unsigned ptr_size() const override
    {
        check_object();
        return sizeof(ChildElementType);
    };
    std::bitset<SIZE> types;
protected:
    union
    {
        LeafArrayType leaves{};
        ChildArrayType children;
    };
};


/**
 * Small node with only 4 children
 */
template<typename IntegerPtr>
struct art_node4_v final : public encoded_node_content<4, 4, IntegerPtr> {
    typedef encoded_node_content<4, 4, IntegerPtr> this_type;
    art_node4_v(){
        statistics::node_bytes_alloc += sizeof(art_node4_v);
        statistics::interior_bytes_alloc += sizeof(art_node4_v);
        ++statistics::n4_nodes;
    }
    ~art_node4_v() override{
        statistics::node_bytes_alloc -= sizeof(art_node4_v);
        statistics::interior_bytes_alloc -= sizeof(art_node4_v);
        --statistics::n4_nodes;
    }
    [[nodiscard]] uint8_t type() const override {
        return node_4;
    }
    using this_type::copy_from;
    using this_type::remove_child;
    using this_type::set_child;
    using this_type::add_child;
    using this_type::has_child;

    using this_type::num_children;
    using this_type::get_child;
    using this_type::partial_len;
    using this_type::partial;
    using this_type::keys;
    using this_type::children;
    using this_type::insert_type;
    using this_type::index;

    void remove(node_ptr& ref, unsigned pos, unsigned char ) override{

        remove_child(pos);

        // Remove nodes with only a single child
        if (num_children == 1) {
            node_ptr child = get_child(0);
            if (!child.is_leaf) {
                // Concatenate the prefixes
                unsigned prefix = partial_len;
                if (prefix < max_prefix_llength) {
                    partial[prefix] = keys[0];
                    ++prefix;
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
    void add_child_inner(unsigned char c, node_ptr child) override
    {
        unsigned idx = index(c, gt);
        // Shift to make room
        memmove(keys+idx+1, keys+idx, num_children - idx);
        memmove(children.data+idx+1, children.data+idx,
                (num_children - idx)*sizeof(IntegerPtr));
        insert_type(idx);
        // Insert element
        keys[idx] = c;
        set_child(idx, child);
        num_children++;
    }
    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override {

        if (num_children < 4) {
            this->expand_pointers(ref, {child})->add_child_inner(c, child);
        } else {
            art_node *new_node = alloc_node(node_16, {child});
            // Copy the child pointers and the key map
            new_node->set_children(0, this, 0, num_children);
            new_node->set_keys(keys, num_children);
            new_node->copy_header(this);
            ref = new_node;
            free_node(this);
            new_node->add_child(c, ref, child);
        }
    }
    [[nodiscard]] node_ptr last() const override {
        unsigned  idx = num_children - 1;
        return get_child(idx);
    }
    [[nodiscard]] unsigned last_index() const override {
        return num_children - 1;
    }

    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override{
        for (unsigned i=0 ; i < num_children; i++) {

            if (keys[i] >= c && has_child(i)){
                return {{this,get_child(i),i},keys[i] == c};
            }
        }
        return {{nullptr,nullptr,num_children},false};
    }

    [[nodiscard]] trace_element next(const trace_element& te) override{
        unsigned i = te.child_ix + 1;
        if (i < this->num_children) {
            return {this,this->get_child(i),i};
        }
        return {};
    }
    [[nodiscard]] trace_element previous(const trace_element& te) override{

        unsigned i = te.child_ix;
        if (i > 0) {
            return {this,get_child(i),i-1};
        }
        return {};
    }

};
typedef art_node4_v<int32_t> art_node4_4;
typedef art_node4_v<uintptr_t> art_node4_8;
/**
 * Node with 16 children
 */
template<typename IPtrType>
struct art_node16_v final : public
encoded_node_content<16,16, IPtrType >
{
    typedef encoded_node_content<16,16, IPtrType > Parent;
    art_node16_v(){
        statistics::node_bytes_alloc += sizeof(art_node16_v);
        statistics::interior_bytes_alloc += sizeof(art_node16_v);
        ++statistics::n16_nodes;
    }
    ~art_node16_v() override{
        statistics::node_bytes_alloc -= sizeof(art_node16_v);
        statistics::interior_bytes_alloc -= sizeof(art_node16_v);
        --statistics::n16_nodes;
    }
    [[nodiscard]] uint8_t type() const override{
        return node_16;
    }
    [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override {
        unsigned i = bits_oper16(this->keys, nuchar<16>(c), (1 << this->num_children) - 1, operbits);
        if (i) {
            i = __builtin_ctz(i);
            return i;
        }
        return this->num_children;
    }
    
    
    // unsigned pos = l - children;
    void remove(node_ptr& ref, unsigned pos, unsigned char ) override {
        this->remove_child(pos);

        if (this->num_children == 3) {
            auto *new_node = alloc_node(node_4,{});
            new_node->copy_header(this);
            new_node->set_keys(this->keys, 3);
            new_node->set_children(0, this, 0, 3);

            ref = new_node;
            free_node(this); // ???
        }
    }
    void add_child_inner(unsigned char c, node_ptr child) override
    {
        unsigned mask = (1 << this->num_children) - 1;

        unsigned bitfield = bits_oper16(nuchar<16>(c), this->get_keys(), mask, lt);

        // Check if less than any
        unsigned idx;
        if (bitfield) {
            idx = __builtin_ctz(bitfield);
            memmove(this->keys+idx+1,this->keys+idx,this->num_children-idx);
            memmove(this->children.data+idx+1,this->children.data+idx,
                    (this->num_children-idx)*sizeof(typename Parent::ChildElementType));
        } else
            idx = this->num_children;

        this->insert_type(idx);
        // Set the child
        this->keys[idx] = c;
        this->set_child(idx, child);
        ++this->num_children;
    }
    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override {

        if (this->num_children < 16)
        {
            this->expand_pointers(ref, {child})->add_child_inner(c, child);

        } else {
            art_node *new_node = alloc_node(node_48,{child});

            // Copy the child pointers and populate the key map
            new_node->set_children(0, this, 0, this->num_children);
            for (unsigned i=0;i< this->num_children;i++) {
                new_node->set_key(this->keys[i], i + 1);
            }
            new_node->copy_header(this);
            ref = new_node;
            free_node(this);
            new_node->add_child(c, ref, child);
        }

    }
    [[nodiscard]] node_ptr last() const override{
        return this->get_child(this->num_children - 1);
    }
    [[nodiscard]] unsigned last_index() const override{
        return this->num_children - 1;
    }
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override {
        unsigned mask = (1 << this->num_children) - 1;
        unsigned bf = bits_oper16(this->keys, nuchar<16>(c), mask, OPERATION_BIT::eq | OPERATION_BIT::gt); // inverse logic
        if (bf) {
            unsigned i = __builtin_ctz(bf);
            return {{this,this->get_child(i),i}, this->keys[i]==c};
        }
        return {{nullptr,nullptr,this->num_children},false};
    }
    [[nodiscard]] trace_element next(const trace_element& te) override{
        unsigned i = te.child_ix + 1;
        if (i < this->num_children) {
            return {this,this->get_child(i),i};// the keys are ordered so fine I think
        }
        return {};
    }
    [[nodiscard]] trace_element previous(const trace_element& te) override{
        unsigned i = te.child_ix;
        if (i > 0) {
            return {this,this->get_child(i),i-1};// the keys are ordered so fine I think
        }
        return {};
    }
    
};

typedef art_node16_v<int32_t> art_node16_4;
typedef art_node16_v<uintptr_t> art_node16_8;
/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
template <typename PtrEncodedType>
struct art_node48 final : public encoded_node_content<48,256,PtrEncodedType> {
    typedef encoded_node_content<48,256,PtrEncodedType> this_type;

    using this_type::copy_from;
    using this_type::remove_child;
    using this_type::set_child;
    using this_type::add_child;
    using this_type::has_child;

    using this_type::num_children;
    using this_type::get_child;
    using this_type::partial_len;
    using this_type::partial;
    using this_type::keys;
    using this_type::children;
    using this_type::types;
    using this_type::insert_type;
    using this_type::index;

    art_node48(){
        statistics::node_bytes_alloc += sizeof(art_node48);
        statistics::interior_bytes_alloc += sizeof(art_node48);
        ++statistics::n48_nodes;
    }
    ~art_node48() override{
        statistics::node_bytes_alloc -= sizeof(art_node48);
        statistics::interior_bytes_alloc -= sizeof(art_node48);
        --statistics::n48_nodes;
    }
    [[nodiscard]] uint8_t type() const override {
        return node_48;
    }
    [[nodiscard]] unsigned index(unsigned char c) const override {
        unsigned  i = keys[c];
        if (i)
            return i-1;
        return 256;
    }
    void remove(node_ptr& ref, unsigned pos, unsigned char key) override{
        if((unsigned)keys[key] -1 != pos) {
            abort();
        }
        if (keys[key] == 0){
            return;
        }
        if (children[pos].empty()) {
            abort();
        }
        keys[key] = 0;
        children[pos] = nullptr;
        types[pos] = false;
        num_children--;

        if (num_children == 12) {
            auto *new_node = alloc_node(node_16, {});
            new_node->copy_header(this);
            unsigned child = 0;
            for (unsigned i = 0; i < 256; i++) {
                pos = keys[i];
                if (pos) {
                    node_ptr nn = get_child(pos - 1);
                    if (nn.null()) {
                        abort();
                    }
                    new_node->set_key(child, i);
                    new_node->set_child(child, nn);
                    child++;
                }
            }
            ref = new_node;
            free_node(this);
        }

    }
    void add_child_inner(unsigned char c, node_ptr child) override
    {
        unsigned pos = 0;
        while (has_child(pos)) pos++;
        // not we do not need to call insert_type an empty child is found
        set_child(pos, child);
        keys[c] = pos + 1;
        num_children++;

    }
    void add_child(unsigned char c, node_ptr& ref, node_ptr child) override {
        if (num_children < 48) {
            this->expand_pointers(ref, {child})->add_child_inner(c, child);
        } else {
            auto *new_node = alloc_node(node_256,{});
            for (unsigned i = 0;i < 256; i++) {
                if (keys[i]) {
                    node_ptr nc = get_child(keys[i] - 1);
                    if(nc.null()) {
                        abort();
                    }
                    new_node->set_child(i, nc);
                }
            }
            new_node->copy_header(this);
            statistics::node256_occupants += new_node->num_children;
            ref = new_node;
            free_node(this);
            new_node->add_child(c, ref, child);
        }
    }
    [[nodiscard]] node_ptr last() const override {
        return get_child(last_index());
    }
    [[nodiscard]] unsigned last_index() const override{
        unsigned idx=255;
        while (!keys[idx]) idx--;
        return keys[idx] - 1;
    }
    [[nodiscard]] unsigned first_index() const override{
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
    [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) override{
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

    [[nodiscard]] trace_element next(const trace_element& te) override{
        unsigned uc = te.child_ix, i;

        for (; uc > 0; --uc){
            i = keys[uc];
            if(i > 0){
                return {this,get_child(i-1),i-1};
            }
        }
        return {};
    }

    [[nodiscard]] trace_element previous(const trace_element& te) override{
        unsigned uc = te.child_ix + 1, i;
        for (; uc < 256;uc++){
            i = keys[uc];
            if(i > 0){
                return {this,get_child(i-1),i-1};
            }
        }
        return {};
    }

    
} ;
typedef art_node48<int32_t> art_node48_4;
typedef art_node48<uintptr_t> art_node48_8;
/**
 * Full node with 256 children
 */
struct art_node256 final : public encoded_node_content<256,0,uintptr_t> {
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
    [[nodiscard]] trace_element next(const trace_element& te) override;
    [[nodiscard]] trace_element previous(const trace_element& te) override;

};
template <typename T> 
static T* get_node(art_node* n){
    return static_cast<T*>(n);
}
template <typename T> 
static T* get_node(const node_ptr& n){
    return static_cast<T*>(n.node);
}
template <typename T> 
static const T* get_node(const node_ptr& n){
    return static_cast<const T*>(n.node);
}
template <typename T> 
static const T* get_node(const art_node* n) {
    return static_cast<T*>(n);
}



/**
 * free a node while updating statistics 
 */

void free_node(node_ptr n);


