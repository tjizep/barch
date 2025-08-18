#pragma once
#include <algorithm>
#include <cstdint>
#include <bitset>
#include <limits>

#include "valkeymodule.h"

#include <array>
#include "logical_allocator.h"
#include "value_type.h"
#include "keyspec.h"

#include "key_options.h"
namespace art {
    template<typename I>
    int64_t i64max() {
        return std::numeric_limits<I>::max();
    }

    template<typename I>
    int64_t i64min() {
        return std::numeric_limits<I>::min();
    }

    enum key_types {
        tinteger = 1u,
        tdouble = 2u,
        tstring = 3u,
        tcomposite = 4u,
        tshort = 5u,
        tfloat = 6u,
        tlast_valid = 6u,
        tend = 255u,
        tnone = 65536
    };

    struct composite_type {
        uint8_t id{};

        composite_type(int id) : id(id) {
        }
    };

    static composite_type ts_composite{tcomposite};
    static composite_type ts_end{tend};

    enum node_kind {
        node_4 = 1u,
        node_16 = 2u,
        node_48 = 3u,
        node_256 = 4u
    };

    enum constants {
        max_prefix_llength = 10u,
        max_alloc_children = 8u,
        initial_node = node_4
    };

    enum {
        leaf_expiry_flag = 1u,
        leaf_volatile_flag = 2u,
        leaf_deleted_flag = 4u,
        leaf_large_flag = 8u,
        leaf_last_flag = (1<<(3u+1u))-1,
    };

    struct leaf;

    void free_leaf_node(art::leaf *l, logical_address logical);

    typedef logical_address logical_leaf;

    struct node_ptr_storage {
        uint8_t storage[node_pointer_storage_size];
        size_t size{};

        node_ptr_storage() {
        };

        node_ptr_storage(const node_ptr_storage &src) {
            *this = src;
        }

        template<class T, typename... Args>
        T *emplace(Args &&... args) {
            static_assert(sizeof(T) < sizeof(storage));
            new(&storage[0]) T(std::forward<Args>(args)...);
            size = sizeof(T);
            return reinterpret_cast<T *>(&storage[0]);
        }

        node_ptr_storage &operator=(const node_ptr_storage &src) {
            if (this == &src) return *this;
            size = src.size;
            memcpy(storage, src.storage, sizeof(storage));
            return *this;
        }

        template<typename T>
        void set(const T *t) {
            static_assert(sizeof(T) < sizeof(storage));
            size = sizeof(T);
            memcpy(storage, t, size);
        }

        template<typename T>
        T *ptr() {
            //if(null()) return nullptr;
            static_assert(sizeof(T) < sizeof(storage));
            return reinterpret_cast<T *>(&storage[0]);
        }

        [[nodiscard]] bool null() const {
            return size == 0;
        }

        template<typename T>
        const T *ptr() const {
            //if(null()) return nullptr;
            static_assert(sizeof(T) < sizeof(storage));
            return reinterpret_cast<const T *>(&storage[0]);
        }
    };

    template<typename node_t>
    struct node_ptr_t {
        bool is_leaf{false};
        node_ptr_t() = default;

        node_ptr_t(const node_ptr_storage &s) : storage(s) {

            logical = storage.ptr<node_t>()->get_address();
        };

        node_ptr_t(const node_ptr_t &p) = default;

        node_ptr_t(const node_t *p): logical(p->get_address()), storage(p->get_storage()) {
        }

        node_ptr_t(logical_address cl) : is_leaf(true), logical(cl) {
        }

        node_ptr_t(std::nullptr_t) : is_leaf(false) {
        }

        [[nodiscard]] bool null() const {
            if (is_leaf) return logical.null();
            return storage.null();
        }

        logical_address logical{nullptr};
        node_ptr_storage storage{};

        void set(nullptr_t) {
            storage.size = 0;
            is_leaf = false;
            logical = nullptr;
        }

        void set(const node_t *n) {
            storage = n->get_storage();
            logical = n->get_address();
            is_leaf = false;
        }

        void set(const logical_leaf &lf) {
            logical = lf;
            is_leaf = true;
        }

        bool operator ==(nullptr_t) const {
            if (is_leaf) return logical.null();
            return storage.null();
        }

        bool operator !=(const node_ptr_t &p) const {
            return !(*this == p);
        };

        bool operator ==(const node_ptr_t &p) const {
            return is_leaf == p.is_leaf && logical == p.logical;
        };

        node_ptr_t &operator =(const node_ptr_t &n) {
            if (&n == this) return *this;
            this->is_leaf = n.is_leaf;
            this->storage = n.storage;
            this->logical = n.logical;
            check();
            return *this;
        }

        node_ptr_t &operator =(const node_t *n) {
            set(n);
            return *this;
        }

        node_ptr_t &operator =(node_t *n) {
            set(n);
            return *this;
        }

        node_ptr_t &operator =(const logical_leaf &l) {
            set(l);
            return *this;
        }

        node_ptr_t &operator =(nullptr_t l) {
            set(l);
            return *this;
        }

        void check() const {
        }

        void free_from_storage() {
            if (is_leaf) {
                free_leaf_node(l(), logical);
            } else if (!logical.null() && !storage.null()) {
                modify()->free_data();
            }
        }

        leaf *l() {
            if (!is_leaf)
                abort();
            check();
            auto *l = logical.get_ap<alloc_pair>().get_leaves().modify<leaf>(logical);
            if (l == nullptr) {
                abort();
            }
            return l;
        }

        [[nodiscard]] const leaf *l() const {
            return const_leaf();
        }

        [[nodiscard]] const leaf *const_leaf() const {
            if (!is_leaf)
                abort();
            check();
            const auto *l = logical.get_ap<alloc_pair>().get_leaves().read<leaf>(logical);
            if (l == nullptr) {
                abort();
            }
            return l;
        }

        [[nodiscard]] const leaf *cl() const {
            return const_leaf();
        }

        node_t *modify() {
            if (is_leaf) {
                abort();
            }
            check();
            return storage.ptr<node_t>();
        }

        [[nodiscard]] const node_t *get_node() const {
            if (is_leaf) {
                abort();
            }

            return storage.ptr<node_t>();
        }
#if 0
        node_t* operator ->()
        {
            return get_node();
        }
#endif
        const node_t *operator ->() const {
            return get_node();
        }

        explicit operator const node_t *() const {
            return get_node();
        }

        explicit operator node_t *() {
            return modify();
        }
    };

    struct node_data {
        uint8_t type = 0;
        uint8_t pointer_size = 0;
        uint8_t partial_len = 0;
        uint8_t occupants = 0;
        uint64_t descendants = 0;
        unsigned char partial[max_prefix_llength]{};
    };

    struct node {
        typedef node_ptr_t<node> node_ptr;

        struct trace_element {
            node_ptr parent = nullptr;
            node_ptr child = nullptr;
            unsigned child_ix = 0;
            unsigned char k = 0;

            [[nodiscard]] bool empty() const {
                return parent.null() && child.null() && child_ix == 0;
            }

            bool operator!=(const trace_element &rhs) const {
                return parent != rhs.parent || child_ix != rhs.child_ix || child != rhs.child;
            }

            bool operator==(const trace_element &rhs) const {
                return parent == rhs.parent && child_ix == rhs.child_ix && child == rhs.child;
            }

            [[nodiscard]] bool valid() const {
                return !parent.null() && !child.null();
            }

            bool operator==(const std::pair<unsigned, uint8_t> &rhs) const {
                return child_ix == rhs.first && k == rhs.second;
            }

            bool operator!=(const std::pair<unsigned, uint8_t> &rhs) const {
                return !(*this == rhs);
            }
        };

        typedef std::array<node_ptr, max_alloc_children> children_t;

        struct node_proxy {
            template<typename T>
            const T *refresh_cache() const
            {
                if (!dcache || last_ticker != page_modifications::get_ticker(address.page())) {
                    dcache = address.get_ap<alloc_pair>().get_nodes().modify<T>(address);
                    last_ticker = page_modifications::get_ticker(address.page());
                }
                return (T *) dcache;
            }

            template<typename T>
            T *refresh_cache() {
                if (!dcache || last_ticker != page_modifications::get_ticker(address.page())) {
                    dcache = address.get_ap<alloc_pair>().get_nodes().modify<T>(address);
                    last_ticker = page_modifications::get_ticker(address.page());
                }
                return (T *) dcache;
            }
            mutable node_data *dcache = nullptr;
            mutable uint32_t last_ticker = page_modifications::get_ticker(0);
            mutable logical_address address{nullptr};

            node_proxy(const node_proxy &) = default;

            node_proxy() = default;

            template<typename IntPtrType, uint8_t NodeType>
            void set_lazy(logical_address in_address, node_data *data) {
                if (node_checks == 1) {
                    if (in_address.null()) {
                        abort_with("invalid lazy node address");
                    }
                    if (data->type != NodeType || data->pointer_size != sizeof(IntPtrType)) {
                        abort_with("type and pointer size does not match");
                    }
                }
                this->address = in_address;
                last_ticker = page_modifications::get_ticker(in_address.page());
                dcache = data; // it will get loaded as required
            }
        };

        node() = default;

        virtual ~node() = default;

        [[nodiscard]] virtual uint8_t type() const = 0;

        virtual node_data &data() = 0;

        [[nodiscard]] virtual const node_data &data() const = 0;

        virtual void set_leaf(unsigned at) = 0;

        virtual void set_child(unsigned at, node_ptr child) = 0;

        [[nodiscard]] virtual bool is_leaf(unsigned at) const = 0;

        [[nodiscard]] virtual bool has_child(unsigned at) const = 0;

        virtual node_ptr get_node(unsigned at) = 0;

        [[nodiscard]] virtual node_ptr get_node(unsigned at) const = 0;

        //virtual node_ptr get_child(unsigned at) = 0;
        [[nodiscard]] virtual node_ptr get_child(unsigned at) const = 0;

        [[nodiscard]] virtual unsigned index(unsigned char, unsigned operbits) const = 0;

        [[nodiscard]] virtual unsigned index(unsigned char c) const = 0;

        [[nodiscard]] virtual node_ptr find(unsigned char, unsigned operbits) const = 0;

        [[nodiscard]] virtual node_ptr find(unsigned char) const = 0;

        [[nodiscard]] virtual node_ptr last() const = 0;

        [[nodiscard]] virtual std::pair<unsigned, uint8_t> last_index() const = 0;

        virtual void remove(node_ptr &ref, unsigned pos, unsigned char key) = 0;

        virtual unsigned add_child(unsigned char c, node_ptr &ref, node_ptr child) = 0;

        virtual unsigned add_child_inner(unsigned char, node_ptr) {
            return 0;
        }

        [[nodiscard]] virtual const unsigned char &get_key(unsigned at) const = 0;

        virtual unsigned char &get_key(unsigned at) = 0;

        unsigned check_prefix(const unsigned char *, unsigned, unsigned) const;

        [[nodiscard]] virtual std::pair<unsigned, uint8_t> first_index() const = 0;

        [[nodiscard]] virtual std::pair<trace_element, bool> lower_bound_child(unsigned char c) const = 0;

        [[nodiscard]] virtual trace_element next(const trace_element &te) const = 0;

        [[nodiscard]] virtual trace_element previous(const trace_element &te) const = 0;

        virtual void set_keys(const unsigned char *other_keys, unsigned count) = 0;

        [[nodiscard]] virtual const unsigned char *get_keys() const = 0;

        virtual void set_key(unsigned at, unsigned char k) = 0;

        virtual void copy_from(node_ptr s) = 0;

        virtual void copy_header(node_ptr src) = 0;

        virtual void set_children(unsigned dpos, const node *other, unsigned spos, unsigned count) = 0;

        [[nodiscard]] virtual uint8_t child_type(unsigned at) const = 0;

        // returns true if node does not need to be rebuilt
        [[nodiscard]] virtual bool ok_child(node_ptr np) const = 0;

        [[nodiscard]] virtual bool ok_children(const children_t &child) const = 0;

        [[nodiscard]] virtual unsigned ptr_size() const = 0;

        [[nodiscard]] virtual node_ptr expand_pointers(node_ptr &ref, const children_t &child) = 0;

        [[nodiscard]] virtual node_ptr expand_pointers(const children_t &child) = 0;

        [[nodiscard]] virtual size_t alloc_size() const = 0;

        [[nodiscard]] virtual logical_address get_address() const = 0;

        [[nodiscard]] virtual node_ptr_storage get_storage() const = 0;

        [[nodiscard]] virtual logical_address create_data(alloc_pair& nodes) = 0;

        virtual void free_data() = 0;

        [[nodiscard]] virtual unsigned leaf_only_distance(unsigned start, unsigned &size) const = 0;

        [[nodiscard]] virtual bool check_data() const = 0;
    };

    typedef node::node_ptr node_ptr;
    typedef node::trace_element trace_element;
    typedef node::children_t children_t;

    node_ptr alloc_node_ptr(unsigned ptrsize, unsigned nt, const children_t &c);

    node_ptr alloc_8_node_ptr(unsigned nt); // magic 8 ball
    extern node_ptr resolve_read_node(logical_address address);

    extern node_ptr resolve_write_node(logical_address address);


    typedef heap::vector<trace_element> trace_list;

    /**
     * Represents a leaf. These are
     * of arbitrary size, as they include the key.
     */
    struct leaf {
        typedef uint8_t LeafSize;
        typedef long ExpiryType;

        leaf() = delete;

        leaf(unsigned kl, unsigned vl, int64_t expiry, bool is_volatile) {
            if (kl >= std::numeric_limits<LeafSize>::max() || vl >= std::numeric_limits<LeafSize>::max()) {
                set_is_large();
            }
            set_key_len(kl);
            set_val_len(vl);
            if (expiry && now() > expiry) {
                //std_err("key already expired:");
            }
            if (expiry > 0) set_is_expiry();
            if (is_volatile) set_volatile();
            set_expiry(expiry);
        }

        uint8_t flags{};
        LeafSize _key_len{}; // does not include null terminator (which is hidden: see make_leaf)
        LeafSize _val_len{};

        [[nodiscard]] unsigned key_len() const {

            if (large()) {
                uint32_t l;
                memcpy(&l, data, sizeof(uint32_t));
                return l;
            }
            return _key_len;
        }

        [[nodiscard]] unsigned val_len() const {
            if (bad()) {
                abort_with("invalid leaf flags");
            }
            if (large()) {
                uint32_t l;
                memcpy(&l, data+sizeof(uint32_t), sizeof(uint32_t));
                return l;
            }
            return _val_len;
        }
        void set_key_len(uint32_t l) {
            if (large()) {
                memcpy(data, &l, sizeof(uint32_t));
                return;
            }
            if ( l >= std::numeric_limits<LeafSize>::max()) {
                abort_with("key length too large");
            }
            _key_len = l;
        }
        void set_val_len(uint32_t l) {
            if (large()) {
                memcpy(data+sizeof(uint32_t), &l, sizeof(uint32_t));
                return;
            }
            if (l >= std::numeric_limits<LeafSize>::max()) {
                abort_with("value length too large");
            }
            _val_len = l;
        }

        unsigned char data[];

        void set_volatile() {
            flags |= leaf_volatile_flag;
        }

        void set_deleted() {
            flags |= leaf_deleted_flag;
        }

        void unset_volatile() {
            flags &= ~leaf_volatile_flag;
        }

        [[nodiscard]] bool is_volatile() const {
            return (flags & leaf_volatile_flag) == leaf_volatile_flag;
        }

        [[nodiscard]] bool is_expiry() const {
            return (flags & leaf_expiry_flag) == leaf_expiry_flag;
        }

        void set_is_expiry() {
            flags |= leaf_expiry_flag;
        }

        void set_is_large() {
            flags |= leaf_large_flag;
        }

        void unset_is_expiry() {
            flags &= ~leaf_expiry_flag;
        }
        static size_t make_size(unsigned kl, unsigned vl,bool expires, bool unused(is_volatile)) {
            // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
            bool large = kl >= std::numeric_limits<LeafSize>::max() || vl >= std::numeric_limits<LeafSize>::max();
            size_t esize = (expires ? sizeof(uint64_t) : 0);
            size_t lsize = (large ? sizeof(uint32_t)*2:0);
            return sizeof(leaf) + kl + 1 + vl + lsize + esize;
        }
        [[nodiscard]] size_t byte_size() const {
            // NB the + 1 is for a hidden 0 byte contained in the key not reflected by length()
            size_t esize = (is_expiry() ? sizeof(uint64_t) : 0);
            size_t lsize = (large() ? sizeof(uint32_t)*2:0);
            return key_len() + 1 + val_len() + lsize + esize + sizeof(leaf);
        }

        [[nodiscard]] bool expired() const {
            ExpiryType expiry = expiry_ms();
            if (!expiry) return false;
            auto n = now();
            return n > expiry;
        }
        [[nodiscard]] bool bad() const {
            return  flags > leaf_last_flag ;
        }
        [[nodiscard]] bool deleted() const {
            if (bad()) {
                abort_with("invalid leaf flags");
            }
            return (flags & leaf_deleted_flag) == leaf_deleted_flag;
        }

        [[nodiscard]] bool large() const {
            return  (flags & leaf_large_flag) == leaf_large_flag;
        }

        [[nodiscard]] unsigned val_start() const {
            if (bad()) {
                abort_with("invalid leaf flags");
            }
            return key_len() + 1 + (large() ? sizeof(uint32_t)*2:0);
        };

        unsigned char *val() {
            return data + val_start();
        };

        [[nodiscard]] const unsigned char *val() const {
            return data + val_start();
        };
        void check_key(const uint8_t* k)const {
            if (k){
                // a stronger check would be without the 0x00
                // but then auth doesnt work because it doesnt use type bytes
                if ( k[0] > tlast_valid && k[key_len()] != 0x00) {
                    abort_with("invalid key");
                }
                if ( k[0] == tinteger && key_len()+1 != numeric_key_size) {
                    std_err("invalid key (int) len",key_len());
                    abort_with("invalid key (int)");
                }
                if ( k[0] == tdouble && key_len()+1 != numeric_key_size) {
                    std_err("invalid key (double) len",key_len());
                    abort_with("invalid key (double)");
                }
            }
        }

        unsigned char *key() {
            if (bad()) {
                abort_with("invalid leaf flags");
            }
            auto k = data + (large() ? sizeof(uint32_t)*2:0);
            check_key(k);
            return k;
        };

        [[nodiscard]] const unsigned char *key() const {
            if (bad()) {
                abort_with("invalid leaf flags");
            }
            auto k = data + (large() ? sizeof(uint32_t)*2:0);
            check_key(k);
            return k;
        };

        [[nodiscard]] value_type get_raw_key() const {
            return {key(), key_len()};
        }

        [[nodiscard]] value_type get_key() const {
            return {key(),  key_len() + 1};
        }

        [[nodiscard]] value_type get_clean_key() const {
            return {key() + 1, (unsigned) key_len()};
        }

        void set_key(const unsigned char *k, unsigned len) {
            auto l = std::min<unsigned>(len, key_len());
            memcpy(data, k, l);
        }

        void set_key(value_type k) {
            auto l = std::min<unsigned>(k.size, key_len());
            memcpy(data, k.bytes, l);
        }

        void set_value(const void *v, unsigned len) {
            auto l = std::min<unsigned>(len, val_len());
            memcpy(val(), v, l);
        }

        void set_value(const unsigned char *v, unsigned len) {
            auto l = std::min<unsigned>(len, val_len());
            memcpy(val(), v, l);
        }

        void set_value(const char *v, unsigned len) {
            auto l = std::min<unsigned>(len, val_len());
            memcpy(val(), v, l);
        }

        [[nodiscard]] ExpiryType expiry_ms() const {
            if (!is_expiry()) return 0;
            ExpiryType r = 0;
            memcpy(&r, val() + val_len(), sizeof(ExpiryType));
            return r;
        }

        bool set_expiry(ExpiryType v) {
            if (!is_expiry()) return false;
            memcpy(val() + val_len(), &v, sizeof(ExpiryType));
            return true;
        }

        void set_value(value_type v) {
            auto l = std::min<unsigned>(v.size, val_len());
            memcpy(val(), v.bytes, l);
        }

        [[nodiscard]] const char *s() const {
            return (const char *) val();
        }

        [[nodiscard]] char *s() {
            return (char *) val();
        }

        explicit operator value_type() {
            return {s(), val_len()};
        }

        [[nodiscard]] value_type get_value() const {
            return {s(), val_len()};
        }

        /**
         * Checks if a leaf matches
         * @return 0 on success.
         */
        [[nodiscard]] int compare(value_type k) const {
            return compare(k.bytes, k.length(), 0);
        }

        [[nodiscard]] int prefix(value_type k) const {
            return prefix(k.bytes, k.length());
        }

        int compare(const unsigned char *key, unsigned key_len, unsigned unused(depth)) const {
            unsigned left_len = this->key_len();
            unsigned right_len = key_len;
            int r = memcmp(this->key(), key, std::min<unsigned>(left_len, right_len));
            if (r == 0) {
                if (left_len < right_len) return -1;
                if (left_len > right_len) return 1;
                return 0;
            }
            return r;
        }

        int prefix(const unsigned char *key, unsigned key_len) const {
            unsigned left_len = this->key_len();
            unsigned right_len = key_len;
            if (left_len < right_len) return -1;
            int r = memcmp(this->key(), key, std::min<unsigned>(left_len, right_len));
            return r;
        }

         operator const art::key_options() const {
            key_options opts;
            opts.set_expiry(this->expiry_ms());
            opts.set_volatile(is_volatile());
            return opts;
        }
    };


    template<typename T>
    static T *get_node(node *n) {
        return static_cast<T *>(n);
    }

    template<typename T>
    static T *get_node(const node_ptr &n) {
        return static_cast<T *>(n.get_node());
    }

    template<typename T>
    static const T *get_node(const node_ptr &n) {
        return static_cast<const T *>(n.get_node());
    }

    template<typename T>
    static const T *get_node(const node *n) {
        return static_cast<T *>(n);
    }

    /**
     * free a node while updating statistics
     */

    void free_node(node_ptr n);

    node_ptr make_leaf(alloc_pair& alloc, value_type key, value_type v, leaf::ExpiryType ttl = 0, bool is_volatile = false);
    node_ptr make_stable_leaf(alloc_pair& alloc, value_type key, value_type v, leaf::ExpiryType ttl = 0, bool is_volatile = false);

    node_ptr alloc_8_node_ptr(alloc_pair& alloc, unsigned nt);

    void free_leaf_node(art::node_ptr n);
}
