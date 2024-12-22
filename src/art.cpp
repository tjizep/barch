#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include <vector>
#include <atomic>
#include "art.h"
#include "valkeymodule.h"

/**
 * hopefully __ARM_NEON__ is mutually exclusive with __i386__ or __amd64__
 */
#ifdef __ARM_NEON__
    #include "sse2neon.h"
#endif

#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
    #include <emmintrin.h>
#endif
#endif

namespace statistics{
    /**
     * size stats
     */
    static std::atomic<uint64_t> n4_nodes;
    static std::atomic<uint64_t> n16_nodes;
    static std::atomic<uint64_t> n48_nodes;
    static std::atomic<uint64_t> n256_nodes;
    static std::atomic<uint64_t> leaf_nodes;
    static std::atomic<uint64_t> node_bytes_alloc;
    static std::atomic<uint64_t> interior_bytes_alloc;
    /**
     * ops stats
     */
    static std::atomic<uint64_t> delete_ops;
    static std::atomic<uint64_t> set_ops;
    static std::atomic<uint64_t> iter_ops;
    static std::atomic<uint64_t> iter_start_ops;
    static std::atomic<uint64_t> iter_range_ops;
    static std::atomic<uint64_t> range_ops;
    static std::atomic<uint64_t> get_ops;
    static std::atomic<uint64_t> lb_ops;
    static std::atomic<uint64_t> size_ops;
    static std::atomic<uint64_t> insert_ops;
    static std::atomic<uint64_t> min_ops;
    static std::atomic<uint64_t> max_ops;
    
    
}
/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) (art_node*)(((void*)((uintptr_t)x | 1)))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

struct trace_element {
    art_node* el;
    art_node* child;
    int child_ix;
    bool empty() const {
        return el == NULL && child == NULL && child_ix == 0;
    }
};

typedef std::vector<trace_element> trace_list;

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(uint8_t type) {
    // these never run the constructor
    art_node* n;
    switch (type) {
        case NODE4:
            n = (art_node*)ValkeyModule_Calloc(1, sizeof(art_node4));
            if (n) {
                statistics::node_bytes_alloc += sizeof(art_node4);
                statistics::interior_bytes_alloc += sizeof(art_node4);
                statistics::n4_nodes++;
            } 
            break;
        case NODE16:
            n = (art_node*)ValkeyModule_Calloc(1, sizeof(art_node16));
            if (n) { 
                statistics::node_bytes_alloc += sizeof(art_node16);
                statistics::interior_bytes_alloc += sizeof(art_node16);
                statistics::n16_nodes++;
            }
            break;
        case NODE48:
            n = (art_node*)ValkeyModule_Calloc(1, sizeof(art_node48));
            if (n) {
                statistics::node_bytes_alloc += sizeof(art_node48);
                statistics::interior_bytes_alloc += sizeof(art_node48);
                statistics::n48_nodes++;
            }
            break;
        case NODE256:
            n = (art_node*)ValkeyModule_Calloc(1, sizeof(art_node256));
            if (n) {
                statistics::node_bytes_alloc += sizeof(art_node256);
                statistics::interior_bytes_alloc += sizeof(art_node256);
                statistics::n256_nodes++;
            }
            break;
        default:
            abort();
    }
    n->type = type;
    return n;
}


/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
    t->root = NULL;
    t->size = 0;
    return 0;
}
static void free_node(art_leaf *n){
    if(!n) return;
    int kl = n->key_len;
    ValkeyModule_Free(LEAF_RAW(n));
    statistics::leaf_nodes--;
    statistics::node_bytes_alloc -= (sizeof(art_leaf) + kl);
}
/**
 * free a node while updating statistics 
 */
static void free_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        art_leaf * leaf = LEAF_RAW(n);
        free_node(leaf);
        return;
    }

    switch (n->type) {
        case NODE4:
            statistics::node_bytes_alloc -= sizeof(art_node4);
            statistics::interior_bytes_alloc -= sizeof(art_node4);
            statistics::n4_nodes--;
            break;

        case NODE16:
            statistics::node_bytes_alloc -= sizeof(art_node16);
            statistics::interior_bytes_alloc -= sizeof(art_node16);
            statistics::n16_nodes--;
            break;

        case NODE48:
            statistics::node_bytes_alloc -= sizeof(art_node48);
            statistics::interior_bytes_alloc -= sizeof(art_node48);
            statistics::n48_nodes--;
            break;

        case NODE256:
            statistics::node_bytes_alloc -= sizeof(art_node256);
            statistics::interior_bytes_alloc -= sizeof(art_node256);
            statistics::n256_nodes--;
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    ValkeyModule_Free(n);
}
static void free_node(art_node4 *n){
    free_node(&n->n);
}

static void free_node(art_node16 *n){
    free_node(&n->n);
}

static void free_node(art_node48 *n){
    free_node(&n->n);
}

static void free_node(art_node256 *n){
    free_node(&n->n);
}


// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        free_node(n);
        return;
    }

    // Handle each node type
    int i, idx;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p1->children[i]);
            }
            break;

        case NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p2->children[i]);
            }
            break;

        case NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<256;i++) {
                idx = ((art_node48*)n)->keys[i]; 
                if (!idx) continue; 
                destroy_node(p.p3->children[idx-1]);
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i=0;i<256;i++) {
                if (p.p4->children[i])
                    destroy_node(p.p4->children[i]);
            }
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    free_node(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * Returns the size of the ART tree.
 */


static int leaf_compare(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}
enum OPERATION_BIT {
    eq = 1,
    gt = 2,
    lt = 4
};
/**
 * compare two buffers and put the result in a bitmap
 */

static unsigned bits_oper16(const unsigned char * a, const unsigned char * b, unsigned mask, int operbits){
    unsigned bitfield = 0;
    // support non-86 architectures
    #if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
        // Compare the key to all 16 stored keys
        __m128i cmp;
        #if 0
            if (operbits == (OPERATION_BIT::eq | OPERATION_BIT::gt)) {
                // supposedly a >= b same as !(b < a)
                cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)b), _mm_loadu_si128((__m128i*)a));
                bitfield |= _mm_movemask_epi8(cmp);
                bitfield &= mask;
                return ~bitfield;
            }
        #endif

        if ((operbits & OPERATION_BIT::eq) == OPERATION_BIT::eq) {
            // _mm_set1_epi8(a)
            cmp = _mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)a),_mm_loadu_si128((__m128i*)b)); 
            bitfield |= _mm_movemask_epi8(cmp);
        }
        
        if ((operbits & OPERATION_BIT::lt) == OPERATION_BIT::lt) {
            cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
            bitfield |= _mm_movemask_epi8(cmp);
        }       
        
        if ((operbits & OPERATION_BIT::gt) == OPERATION_BIT::gt) {
            cmp = _mm_cmpgt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
            bitfield |= (_mm_movemask_epi8(cmp));
        }
    #else
        // Compare the key to all 16 stored keys
        if (operbits & OPERATION_BIT::eq) {
            for (i = 0; i < 16; ++i) {
                if (a[i] == b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & OPERATION_BIT::gt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] > b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & OPERATION_BIT::lt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] < b[i])
                    bitfield |= (1 << i);
            }
        }
    #endif
    bitfield &= mask;
    return bitfield;
}
template<int N, typename C = unsigned char>
struct nuchar {
    C data[N];
    nuchar(C c){
        memset(data, c, sizeof(data));
    }
    operator const C* () const {
        return data;
    } 
};
/**
 * find first not less than
 */
static trace_element lower_bound_child(art_node *n, const unsigned char * key, int key_len, int depth, int * is_equal) {

    int i, uc;
    unsigned char prev = 0x00, c = 0x00;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    if (depth >= key_len){
        abort();
    }
    c = key[depth];
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_children; i++) {
                if(prev > p.p1->keys[i]){
                    abort();
                }
		        if (p.p1->keys[i] >= c){
                    *is_equal = p.p1->keys[i] == c;
                    return {n,p.p1->children[i],i};
                }
                prev = p.p1->keys[i];
            }
            break;

        
        case NODE16:
            p.p2 = (art_node16*)n;
            {
                int mask = (1 << n->num_children) - 1;
                unsigned bf = bits_oper16(p.p2->keys, nuchar<16>(c), mask, OPERATION_BIT::eq | OPERATION_BIT::gt); // inverse logic
                if (bf) {
                    i = __builtin_ctz(bf);
                    return {n,p.p2->children[i],i};
                }
            }
            break;
        

        case NODE48:
            p.p3 = (art_node48*)n;
            /*
             * find first not less than
             * todo: make lb faster by adding bit map index and using __builtin_ctz as above 
             */
            uc = c;
            for (; uc < 256;uc++){
                i = p.p3->keys[uc];
                if(i > 0){
                    *is_equal = (i == c);
                    return {n,p.p3->children[i-1],i-1};
                }
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i = c; i < 256; ++i) {
                if (p.p4->children[i]) {// because nodes are ordered accordingly
                    *is_equal = (i == c);
                    return {n,p.p4->children[i],i};
                }
            }
            break;

        default:
            abort();
    }
    return {NULL, NULL, 0};
}

static art_node** find_child(art_node *n, unsigned char c) {
    int i;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_children; i++) {
		/* this cast works around a bug in gcc 5.1 when unrolling loops
		 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
		 */
                if (((unsigned char*)p.p1->keys)[i] == c)
                    return &p.p1->children[i];
            }
            break;

        {
        case NODE16:
            p.p2 = (art_node16*)n;
            i = bits_oper16(p.p2->keys, nuchar<16>(c), (1 << n->num_children) - 1, OPERATION_BIT::eq);
            if (i) {
                i = __builtin_ctz(i);
                return &p.p2->children[i];
            }
            break;
        }

        case NODE48:
            p.p3 = (art_node48*)n;
            i = p.p3->keys[c];
            if (i)
                return &p.p3->children[i-1];
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            if (p.p4->children[c])
                return &p.p4->children[c];
            break;

        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}


/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned char *key, int key_len) {
    statistics::get_ops++;
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
                return ((art_leaf*)n)->value;
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}

// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return maximum(((const art_node4*)n)->children[n->num_children-1]);
        case NODE16:
            return maximum(((const art_node16*)n)->children[n->num_children-1]);
        case NODE48:
            idx=255;
            while (!((const art_node48*)n)->keys[idx]) idx--;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return maximum(((const art_node48*)n)->children[idx]);
        case NODE256:
            idx=255;
            while (!((const art_node256*)n)->children[idx]) idx--;
            return maximum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}


// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return minimum(((const art_node4*)n)->children[0]);
        case NODE16:
            return minimum(((const art_node16*)n)->children[0]);
        case NODE48:
            idx=0;
            while (!((const art_node48*)n)->keys[idx]) idx++;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return minimum(((const art_node48*)n)->children[idx]);
        case NODE256:
            idx=0;
            while (!((const art_node256*)n)->children[idx]) idx++;
            return minimum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}

/**
 * Searches for the lower bound key
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the leaf containing the value pointer is returned.
 */
static const art_leaf* lower_bound(trace_list& trace, const art_tree *t, const unsigned char *key, int key_len) {
    art_node *n = t->root;
    int prefix_len, depth = 0, is_equal = 0;

    while (n) {
        if (IS_LEAF(n)) {
            const art_leaf * leaf = LEAF_RAW(n);
            // Check if the expanded path matches
            if (leaf_compare(leaf, key, key_len, depth) >= 0) {
                return leaf;
            }
            return NULL;
        }
        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                break;
            depth += n->partial_len;
        }

        trace_element te = lower_bound_child(n, key, key_len, depth, &is_equal);
        trace.push_back(te);
        n = te.child;
        depth++;
    }
    return maximum(n);
}
static trace_element first_child_off(art_node* n){
    int i, uc;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            return {n,p.p1->children[0],0};

        
        case NODE16:
            p.p2 = (art_node16*)n;

            return {n,p.p2->children[0],0};// the keys are ordered so fine I think
        

        case NODE48:
            p.p3 = (art_node48*)n;
            /*
             * find first not less than
             * todo: make lb faster by adding bit map index and using __builtin_ctz as above 
             */
            uc = 0; // ?
            for (; uc < 256;uc++){
                i = p.p3->keys[uc];
                if(i > 0){
                    return {n,p.p3->children[i-1],uc};
                }
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i = 0; i < 256; ++i) {
                if (p.p4->children[i]) {// because nodes are ordered accordingly
                    return {n,p.p4->children[i],i};
                }
            }
            break;

        default:
            abort();
    }
    return {NULL, NULL, 0};
}

static trace_element increment_te(trace_element &te){
    int i, uc;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    art_node * n = te.el; 
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i = te.child_ix + 1; i < n->num_children; i++) {
                return {n,p.p1->children[i],i};
            }

            break;

        
        case NODE16:
            p.p2 = (art_node16*)n;

            for (i = te.child_ix + 1; i < n->num_children; ++i) {
                return {n,p.p2->children[i],i};// the keys are ordered so fine I think
            }
            break;
        

        case NODE48:
            p.p3 = (art_node48*)n;
            /*
             * find first not less than
             * todo: make lb faster by adding bit map index and using __builtin_ctz as above 
             */
            uc = te.child_ix + 1;
            for (; uc < 256;uc++){
                i = p.p3->keys[uc];
                if(i > 0){
                    return {n,p.p3->children[i-1],i-1};
                }
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i = te.child_ix+1; i < 256; ++i) {
                if (p.p4->children[i]) {// because nodes are ordered accordingly
                    return {n,p.p4->children[i],i};
                }
            }
            break;

        default:
            abort();
    }
    return {NULL, NULL, 0};
}


static trace_element& last_el(trace_list& trace){
    if(trace.empty())
        abort();
    return *(trace.rbegin());
}
/**
 * assuming that the path to each leaf is not the same depth
 * we always have to check and extend if required
 * @return false if any non leaf node has no child 
 */
static bool extend_trace(trace_list& trace){
    if(trace.empty()) return true;
    trace_element u; 
    while(!IS_LEAF(last_el(trace).child)){
        u = first_child_off(last_el(trace).el);
        if(u.empty()) return false;
        trace.push_back(u);
    }
    return true;
}

static bool increment_trace(trace_list& trace){
    for(auto r = trace.rbegin(); r != trace.rend(); ++r){
        trace_element te = increment_te(*r);
        if(te.empty()) 
            continue; // goto the parent further back and try to increment that 
        *r = te;
        if (r != trace.rbegin()){
            auto u = r;
            // go forward
            do {
                --u;
                te = first_child_off(te.child);
                if(te.empty())
                    return false;
                *u = te;

            } while(u != trace.rbegin());
        }
        return extend_trace(trace);
    
    }
    return false;
}

void* art_lower_bound(const art_tree *t, const unsigned char *key, int key_len) {
    statistics::lb_ops++;
    const art_leaf* al;
    trace_list tl;
    al = lower_bound(tl, t, key, key_len);
    if (al) {
        return al->value;
    }
    return NULL;
}

int art_range(const art_tree *t, const unsigned char *key, int key_len, const unsigned char *key_end, int key_end_len, art_callback cb, void *data) {
    statistics::range_ops++;
    trace_list tl;
    const art_leaf* al;
    al = lower_bound(tl, t, key, key_len);
    if (al) {
        do {
            art_node * n = last_el(tl).child;
            if(IS_LEAF(n)){
                art_leaf * leaf = LEAF_RAW(n);
                if(leaf_compare(leaf, key_end, key_end_len, 0) < 0) { // upper bound is not
                    ++statistics::iter_range_ops;
                    int r = cb(data, leaf->key, leaf->key_len, leaf->value); 
                    if( r != 0) 
                        return r;
                } else {
                    return 0;
                }
            }else{
                abort();
            }
        
        } while(increment_trace(tl));
        
    }
    return 0;
}
/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
    statistics::min_ops++;
    return minimum((art_node*)t->root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    statistics::max_ops++;
    return maximum((art_node*)t->root);
}

static art_leaf* make_leaf(const unsigned char *key, int key_len, void *value) {
    art_leaf *l = (art_leaf*)ValkeyModule_Calloc(1, sizeof(art_leaf)+key_len);
    statistics::leaf_nodes++;
    statistics::node_bytes_alloc += (sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    return l;
}

static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(l1->key_len, l2->key_len) - depth;
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}

static void copy_header(art_node *dest, art_node *src) {
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {
    (void)ref;
    n->n.num_children++;
    n->children[c] = (art_node*)child;
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 48) {
        int pos = 0;
        while (n->children[pos]) pos++;
        n->children[pos] = (art_node*)child;
        n->keys[c] = pos + 1;
        n->n.num_children++;
    } else {
        art_node256 *new_node = (art_node256*)alloc_node(NODE256);
        for (int i=0;i<256;i++) {
            if (n->keys[i]) {
                new_node->children[i] = n->children[n->keys[i] - 1];
            }
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free_node(n);
        add_child256(new_node, ref, c, child);
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 16) {
        unsigned mask = (1 << n->n.num_children) - 1;
        
        unsigned bitfield = bits_oper16(nuchar<16>(c), n->keys, mask, OPERATION_BIT::lt);
        
        // Check if less than any
        unsigned idx;
        if (bitfield) {
            idx = __builtin_ctz(bitfield);
            memmove(n->keys+idx+1,n->keys+idx,n->n.num_children-idx);
            memmove(n->children+idx+1,n->children+idx,
                    (n->n.num_children-idx)*sizeof(void*));
        } else
            idx = n->n.num_children;

        // Set the child
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node48 *new_node = (art_node48*)alloc_node(NODE48);

        // Copy the child pointers and populate the key map
        memcpy(new_node->children, n->children,
                sizeof(void*)*n->n.num_children);
        for (int i=0;i<n->n.num_children;i++) {
            new_node->keys[n->keys[i]] = i + 1;
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free_node(n);
        add_child48(new_node, ref, c, child);
    }
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, art_node *child) {
    if (n->n.num_children < 4) {
        int idx;
        for (idx=0; idx < n->n.num_children; idx++) {
            if (c < n->keys[idx]) break;
        }

        // Shift to make room
        memmove(n->keys+idx+1, n->keys+idx, n->n.num_children - idx);
        memmove(n->children+idx+1, n->children+idx,
                (n->n.num_children - idx)*sizeof(void*));

        // Insert element
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node16 *new_node = (art_node16*)alloc_node(NODE16);

        // Copy the child pointers and the key map
        memcpy(new_node->children, n->children,
                sizeof(void*)*n->n.num_children);
        memcpy(new_node->keys, n->keys,
                sizeof(unsigned char)*n->n.num_children);
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free_node(n);
        add_child16(new_node, ref, c, child);
    }
}

static void add_child(art_node *n, art_node **ref, unsigned char c, art_node *child) {
    switch (n->type) {
        case NODE4:
            return add_child4((art_node4*)n, ref, c, child);
        case NODE16:
            return add_child16((art_node16*)n, ref, c, child);
        case NODE48:
            return add_child48((art_node48*)n, ref, c, child);
        case NODE256:
            return add_child256((art_node256*)n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        art_leaf *l = minimum(n);
        max_cmp = min(l->key_len, key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}

static void* recursive_insert(art_node *n, art_node **ref, const unsigned char *key, int key_len, void *value, int depth, int *old, int replace) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        *ref = (art_node*)SET_LEAF(make_leaf(key, key_len, value));
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value
        if (!leaf_matches(l, key, key_len, depth)) {
            *old = 1;
            void *old_val = l->value;
            if(replace) l->value = value;
            return old_val;
        }

        // New value, we must split the leaf into a node4
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);

        // Create a new leaf
        art_leaf *l2 = make_leaf(key, key_len, value);

        // Determine longest prefix
        int longest_prefix = longest_common_prefix(l, l2, depth);
        new_node->n.partial_len = longest_prefix;
        memcpy(new_node->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
        // Add the leafs to the new node4
        *ref = (art_node*)new_node;
        add_child4(new_node, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        add_child4(new_node, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_node;
        new_node->n.partial_len = prefix_diff;
        memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_node, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_node, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        }

        // Insert the new leaf
        art_leaf *l = make_leaf(key, key_len, value);
        add_child4(new_node, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert(*child, child, key, key_len, value, depth+1, old, replace);
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(key, key_len, value);
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
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
void* art_insert(art_tree *t, const unsigned char *key, int key_len, void *value) {
    
    int old_val = 0;
    void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val, 1);
    if (!old_val){
        t->size++;
        ++statistics::insert_ops;
    } else {
        ++statistics::set_ops;
    }
    return old;
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
void* art_insert_no_replace(art_tree *t, const unsigned char *key, int key_len, void *value) {
    ++statistics::insert_ops;

    int old_val = 0;
    void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val, 0);
    if (!old_val){
         t->size++;     
    }
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, unsigned char c) {
    n->children[c] = NULL;
    n->n.num_children--;

    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (n->n.num_children == 37) {
        art_node48 *new_node = (art_node48*)alloc_node(NODE48);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);
    
        int pos = 0;
        for (int i=0;i<256;i++) {
            if (n->children[i]) {
                new_node->children[pos] = n->children[i];
                new_node->keys[i] = pos + 1;
                pos++;
            }
        }
        free_node(n);
    }
}

static void remove_child48(art_node48 *n, art_node **ref, unsigned char c) {
    int pos = n->keys[c];
    n->keys[c] = 0;
    n->children[pos-1] = NULL;
    n->n.num_children--;

    if (n->n.num_children == 12) {
        art_node16 *new_node = (art_node16*)alloc_node(NODE16);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);
    
        int child = 0;
        for (int i=0;i<256;i++) {
            pos = n->keys[i];
            if (pos) {
                new_node->keys[child] = i;
                new_node->children[child] = n->children[pos - 1];
                child++;
            }
        }
        
        free_node(n);
    }
}

static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    if (n->n.num_children == 3) {
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);
        memcpy(new_node->keys, n->keys, 4);
        memcpy(new_node->children, n->children, 4*sizeof(void*));
        
        free_node(n);
    }
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    // Remove nodes with only a single child
    if (n->n.num_children == 1) {
        art_node *child = n->children[0];
        if (!IS_LEAF(child)) {
            // Concatenate the prefixes
            int prefix = n->n.partial_len;
            if (prefix < MAX_PREFIX_LEN) {
                n->n.partial[prefix] = n->keys[0];
                prefix++;
            }
            if (prefix < MAX_PREFIX_LEN) {
                int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
                memcpy(n->n.partial+prefix, child->partial, sub_prefix);
                prefix += sub_prefix;
            }

            // Store the prefix in the child
            memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
            child->partial_len += n->n.partial_len + 1;
        }
        *ref = child;
        free_node(n);
    }
}

static void remove_child(art_node *n, art_node **ref, unsigned char c, art_node **l) {
    switch (n->type) {
        case NODE4:
            return remove_child4((art_node4*)n, ref, l);
        case NODE16:
            return remove_child16((art_node16*)n, ref, l);
        case NODE48:
            return remove_child48((art_node48*)n, ref, c);
        case NODE256:
            return remove_child256((art_node256*)n, ref, c);
        default:
            abort();
    }
}

static art_leaf* recursive_delete(art_node *n, art_node **ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        int prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    art_node **child = find_child(n, key[depth]);
    if (!child) return NULL;

    // If the child is leaf, delete from this node
    if (IS_LEAF(*child)) {
        art_leaf *l = LEAF_RAW(*child);
        if (!leaf_matches(l, key, key_len, depth)) {
            remove_child(n, ref, key[depth], child);
            return l;
        }
        return NULL;

    // Recurse
    } else {
        return recursive_delete(*child, child, key, key_len, depth+1);
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len) {
    ++statistics::delete_ops;

    art_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
    if (l) {
        t->size--;
        void *old = l->value;
        free_node(l);
        
        return old;
    }
    return NULL;
}

// Recursively iterates over the tree
static int recursive_iter(art_node *n, art_callback cb, void *data) {
    // Handle base cases
    if (!n) return 0;
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        ++statistics::iter_ops;
        return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
    }

    int idx, res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node4*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node16*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                res = recursive_iter(((art_node48*)n)->children[idx-1], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                res = recursive_iter(((art_node256*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
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
int art_iter(art_tree *t, art_callback cb, void *data) {
    ++statistics::iter_start_ops;
    if (!t) {
        return -1;
    }
    return recursive_iter(t->root, cb, data);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const unsigned char *prefix, int prefix_len) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t)prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
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
int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void *data) {
    ++statistics::iter_start_ops;
    
    if (!t) {
        return -1;
    }
    
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                return cb(data, (const unsigned char*)l->key, l->key_len, l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            if (!leaf_prefix_matches(l, key, key_len))
               return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

            // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}
/**
 * just return the size
 */
uint64_t art_size(art_tree *t) {
    ++statistics::size_ops;

    if(t == NULL) 
        return 0;
    
    return t->size;
}

art_statistics art_get_statistics(){
    art_statistics as;
    as.leaf_nodes = statistics::leaf_nodes;
    as.node4_nodes = statistics::n4_nodes;
    as.node16_nodes = statistics::n16_nodes;
    as.node256_nodes = statistics::n256_nodes;
    as.node48_nodes = statistics::n4_nodes;
    as.bytes_allocated = statistics::node_bytes_alloc;
    return as;
}

art_ops_statistics art_get_ops_statistics(){

    art_ops_statistics os;
    os.delete_ops = statistics::delete_ops;
    os.get_ops = statistics::get_ops;
    os.insert_ops = statistics::insert_ops;
    os.iter_ops = statistics::iter_ops;
    os.iter_range_ops = statistics::iter_range_ops;
    os.lb_ops = statistics::lb_ops;
    os.max_ops = statistics::max_ops;
    os.min_ops = statistics::min_ops;
    os.range_ops = statistics::range_ops;
    os.set_ops = statistics::set_ops;
    os.size_ops = statistics::size_ops;
    return os;
}
