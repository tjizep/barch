#pragma once
#include <algorithm>
#include <cstdint>
#include <bitset>
#include <limits>
#include <vector>
#include <bits/ios_base.h>

#include "valkeymodule.h"

#include <array>
#include "compress.h"
#define unused_arg
#define unused(x)
namespace art
{
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
    enum
    {
        leaf_ttl_flag = 1,
        leaf_volatile_flag = 2,
        leaf_deleted_flag = 4
    };
    struct value_type
    {
        const unsigned char* bytes;
        unsigned size;
        value_type(const char * v, unsigned l): bytes((const unsigned char*)v), size(l) {} ;
        value_type(const unsigned char * v, unsigned l): bytes(v), size(l) {} ;
        value_type(const unsigned char * v, size_t l): bytes(v), size(l) {} ;
        [[nodiscard]] unsigned length() const
        {
            if(!size) return 0;
            return size - 1; // implied in the data is a null terminator
        }
        [[nodiscard]] const char * chars() const
        {
            return (const char*)bytes;
        }
        const unsigned char& operator[](unsigned i) const
        {
            // TODO: this is a hack fix because there's some BUG in the insert code
            // were assuming that the key has a magic 0 byte allocated after the last byte
            // however this is not so for data
            if (i < size)
            {
                return bytes[i];
            }
            throw std::out_of_range("index out of range");
        }

    };
    struct leaf;

    void free_leaf_node(art::leaf* l, compressed_address logical);
    typedef compressed_address logical_leaf;
    extern compress & get_leaf_compression();
    extern compress & get_node_compression();
    struct node_ptr_storage
    {
        uint8_t storage[64 - sizeof(size_t)]{};
        size_t size{};
        node_ptr_storage() = default;
        node_ptr_storage(const node_ptr_storage& src)
        {
            *this = src;
        }

        template<class T, typename... Args>
        T* emplace(Args&&...args)
        {
            static_assert(sizeof(T) < sizeof(storage));
            new (&storage[0]) T(std::forward<Args>(args)...);
            size = sizeof(T);
            return reinterpret_cast<T*>(&storage[0]);
        }

        node_ptr_storage& operator=(const node_ptr_storage& src)
        {
            if(this == &src) return *this;
            size = src.size;
            memcpy(storage, src.storage, sizeof(storage));
            return *this;
        }

        template<typename T>
        void set(const T* t)
        {
            static_assert(sizeof(T) < sizeof(storage));
            size = sizeof(T);
            memcpy(storage, t, size);
        }
        template<typename T>
        T* ptr() {
            //if(null()) return nullptr;
            static_assert(sizeof(T) < sizeof(storage));
            return reinterpret_cast<T*>(&storage[0]);
        }
        [[nodiscard]] bool null() const
        {
            return size == 0;
        }
        template<typename T>
        const T* ptr() const {
            //if(null()) return nullptr;
            static_assert(sizeof(T) < sizeof(storage));
            return reinterpret_cast<const T*>(&storage[0]);
        }
    };

    template<typename node_t>
    struct node_ptr_t {

        bool is_leaf{false};
        node_ptr_t() = default;
        node_ptr_t(const node_ptr_storage& s) : storage(s)
        {
            logical = storage.ptr<node_t>()->get_address();
        };
        node_ptr_t(const node_ptr_t& p)  = default;

        node_ptr_t(const node_t* p):  logical(p->get_address()), storage(p->get_storage())
        {
        }

        node_ptr_t(compressed_address cl) : is_leaf(true), resolver(&get_leaf_compression()), logical(cl)
        {
        }

        node_ptr_t(std::nullptr_t) : is_leaf(false), resolver(nullptr) {}

        [[nodiscard]] bool null() const {
            if(is_leaf) return logical.null();
            return storage.null();
        }
        compress *resolver{nullptr};
        compressed_address logical {};
        node_ptr_storage storage{};
        void set(nullptr_t) {
            storage.size = 0;
            is_leaf = false;
            logical = nullptr;
            resolver = nullptr;
        }
        void set(const node_t* n) {
            storage = n->get_storage();
            logical = n->get_address();
            is_leaf = false;
            resolver = nullptr;
        }

        void set(const logical_leaf& lf) {
            logical = lf;
            if(!resolver) resolver = &get_leaf_compression();
            is_leaf = true;
        }
        bool operator == (nullptr_t) const
        {
            if(is_leaf) return logical.null();
            return storage.null();
        }
        bool operator != (const node_ptr_t& p) const
        {
            return !(*this == p);
        };
        bool operator == (const node_ptr_t& p) const
        {
            return is_leaf == p.is_leaf && logical == p.logical;
        };
        node_ptr_t& operator = (const node_ptr_t& n){
            if(&n == this) return *this;
            this->is_leaf = n.is_leaf;
            this->storage = n.storage;
            this->logical = n.logical;
            this->resolver = n.resolver;
            check();
            return *this;
        }

        node_ptr_t& operator = (const node_t* n){
            set(n);
            return *this;
        }

        node_ptr_t& operator = (node_t* n){
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
        void check() const {}
        void free_from_storage()
        {
            if(is_leaf)
            {
                free_leaf_node(l(), logical);
            }else if (!logical.null() && !storage.null())
            {
                get_node()->free_data();
            }
        }
        leaf* l(){
            if (!is_leaf)
                abort();
            check();
            auto * l = resolver->modify<leaf>(logical);
            if(l == nullptr)
            {
                abort();
            }
            return l;
        }
        [[nodiscard]] const leaf* l() const {
            return const_leaf();
        }
        [[nodiscard]] const leaf* const_leaf() const {
            if (!is_leaf)
                abort();
            check();
            const auto * l = resolver->read<leaf>(logical);
            if(l == nullptr)
            {
                abort();
            }
            return l;
        }
        node_t* get_node()
        {
            if(is_leaf) {
                abort();
            }
            check();
            return storage.ptr<node_t>();
        }

        [[nodiscard]] const node_t* get_node() const
        {
            if(is_leaf) {
                abort();
            }

            return storage.ptr<node_t>();
        }

        node_t * operator -> () {

            return get_node();
        }
        const node_t * operator -> () const {

            return get_node();
        }

        explicit operator const node_t* () const {
            return get_node();
        }

        explicit operator node_t* () {
            return get_node();
        }
    };

    struct node_data
    {
        uint8_t type = 0;
        uint8_t pointer_size = 0;
        uint8_t partial_len = 0;
        uint8_t occupants = 0;
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
        };

        typedef std::array<node_ptr, max_alloc_children> children_t;
        struct node_proxy {

            template<typename T>
            const T* refresh_cache() const // read-only refresh
            {
                //dcache = nullptr;
                if(!dcache || last_ticker != compress::flush_ticker)
                {
                    dcache = get_node_compression().modify<T>(address);

                }
                return (T*)dcache;
            }
            template<typename T>
            T* refresh_cache()
            {
                //dcache = nullptr;
                if(!dcache || last_ticker != compress::flush_ticker)
                {
                    dcache = get_node_compression().modify<T>(address);
                }
                return (T*)dcache;
            }
            mutable node_data *dcache = nullptr;
            //mutable node_data *mcache= nullptr;
            mutable uint32_t last_ticker = compress::flush_ticker;
            compressed_address address{};
            node_proxy(const node_proxy&) = default;
            node_proxy() = default;

            template<typename T, typename IntPtrType, uint8_t NodeType>
            void set(compressed_address address)
            {
                if (address.null())
                {
                    abort();
                }
                this->address = address;
                dcache = get_node_compression().modify<T>(address);
                if (dcache->type != NodeType || dcache->pointer_size != sizeof(IntPtrType))
                {
                    abort();
                }
            }
            template<typename IntPtrType, uint8_t NodeType>
            void set_lazy(compressed_address address, node_data* data)
            {
                if (address.null())
                {
                    abort();
                }
                if (data->type != NodeType || data->pointer_size != sizeof(IntPtrType))
                {
                    abort();
                }
                this->address = address;
                dcache = data; // it will get loaded as required
                //mcache = data;

            }
        };

        node() = default;
        virtual ~node() = default;
        [[nodiscard]] virtual uint8_t type() const = 0;

        virtual node_data& data() = 0;
        [[nodiscard]] virtual const node_data& data() const = 0;

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
        [[nodiscard]] virtual trace_element next(const trace_element& te) const = 0;
        [[nodiscard]] virtual trace_element previous(const trace_element& te) const = 0;
        virtual void set_keys(const unsigned char* other_keys, unsigned count) = 0;
        [[nodiscard]] virtual const unsigned char* get_keys() const = 0;
        virtual void set_key(unsigned at, unsigned char k) = 0;
        virtual void copy_from(node_ptr s) = 0;
        virtual void copy_header(node_ptr src) = 0;
        virtual void set_children(unsigned dpos, const node* other, unsigned spos, unsigned count) = 0;
        [[nodiscard]] virtual bool child_type(unsigned at) const = 0;
        // returns true if node does not need to be rebuilt
        [[nodiscard]] virtual bool ok_child(node_ptr np) const = 0;
        [[nodiscard]] virtual bool ok_children(const children_t& child) const = 0;
        [[nodiscard]] virtual unsigned ptr_size() const = 0;

        [[nodiscard]] virtual node_ptr expand_pointers(node_ptr& ref, const children_t& child)  = 0;
        [[nodiscard]] virtual size_t alloc_size() const = 0;
        [[nodiscard]] virtual compressed_address get_address() const = 0;
        [[nodiscard]] virtual node_ptr_storage get_storage() const = 0;
        [[nodiscard]] virtual compressed_address create_data() = 0;
        virtual void free_data() = 0;
    };
    typedef node::node_ptr node_ptr;
    typedef node::trace_element trace_element;
    typedef node::children_t children_t;
    node_ptr alloc_node_ptr(unsigned nt, const children_t& c);
    node_ptr alloc_8_node_ptr(unsigned nt); // magic 8 ball
    extern node_ptr resolve_read_node(compressed_address address);
    extern node_ptr resolve_write_node(compressed_address address);


    typedef std::vector<trace_element> trace_list;

    //void free_node(art_node *n);
    //void free_node(art_leaf *n);

    /**
     * Represents a leaf. These are
     * of arbitrary size, as they include the key.
     */
    struct leaf {
        typedef uint16_t LeafSize;
        typedef long ExpiryType;
        leaf() = delete;
        leaf(unsigned kl, unsigned vl, uint64_t ttl, bool is_volatile) :
            key_len(std::min<unsigned>(kl, std::numeric_limits<LeafSize>::max()))
        ,   val_len(std::min<unsigned>(vl, std::numeric_limits<LeafSize>::max()))
        {
            if(ttl > 0) set_ttl();
            if(is_volatile) set_volatile();
            set_ttl(ttl);
        }
        uint8_t flags {};
        LeafSize key_len ; // does not include null terminator (which is hidden: see make_leaf)
        LeafSize val_len ;
        //uint64_t exp {};
        unsigned char data[];
        void set_volatile()
        {
            flags |= (uint8_t)leaf_volatile_flag;
        }
        void set_deleted()
        {
            flags |= (uint8_t)leaf_deleted_flag;
        }
        void unset_volatile()
        {
            flags &= ~(uint8_t)leaf_volatile_flag;
        }
        [[nodiscard]] bool is_volatile() const
        {
            return (flags & (uint8_t)leaf_volatile_flag) == (uint8_t)leaf_volatile_flag;
        }
        [[nodiscard]] bool is_ttl() const
        {
            return (flags & (uint8_t)leaf_ttl_flag) == (uint8_t)leaf_ttl_flag;
        }
        void set_ttl()
        {
            flags |= (uint8_t)leaf_ttl_flag;
        }
        void unset_ttl()
        {
            flags &= ~(uint8_t)leaf_ttl_flag;
        }

        [[nodiscard]] size_t byte_size() const
        {
            return key_len + 1 + val_len + ((is_ttl())? sizeof(uint64_t):0) + sizeof(leaf);
        }
        [[nodiscard]] bool expired() const
        {
            long expiry = ttl();
            if(!expiry) return false;
            auto n = std::chrono::steady_clock::now().time_since_epoch().count();
            return n > expiry;
        }
        [[nodiscard]] bool deleted() const
        {
            return (flags & (uint8_t)leaf_deleted_flag) == (uint8_t)leaf_deleted_flag;
        }

        [[nodiscard]] unsigned val_start() const
        {
            return key_len + 1;
        };
        unsigned char * val()
        {
            return data + val_start();
        };
        [[nodiscard]] const unsigned char * val() const
        {
            return data + val_start();
        };
        unsigned char * key()
        {
            return data ;
        };
        [[nodiscard]] const unsigned char * key() const
        {
            return data;
        };
        [[nodiscard]] value_type get_key() const
        {
            return {key(),(unsigned)key_len + 1};
        }

        void set_key(const unsigned char* k, unsigned len)
        {
            auto l = std::min<unsigned>(len, key_len);
            memcpy(data, k, l);
        }
        void set_key(value_type k)
        {
            auto l = std::min<unsigned>(k.size, key_len);
            memcpy(data, k.bytes, l);
        }

        void set_value(const void* v, unsigned len)
        {
            auto l = std::min<unsigned>(len, val_len);
            memcpy(val(), v, l);
        }
        void set_value(const unsigned char* v, unsigned len)
        {
            auto l = std::min<unsigned>(len, val_len);
            memcpy(val(), v, l);
        }
        void set_value(const char* v, unsigned len)
        {
            auto l = std::min<unsigned>(len, val_len);
            memcpy(val(), v, l);
        }
        [[nodiscard]] ExpiryType ttl() const
        {
            if(!is_ttl()) return 0;
            ExpiryType r = 0;
            memcpy(&r, val()+val_len, sizeof(ExpiryType));
            return r;
        }

        bool set_ttl(ExpiryType v)
        {
            if(!is_ttl()) return false;
            memcpy(val()+val_len,&v , sizeof(ExpiryType));
            return true;
        }

        void set_value(value_type v)
        {
            auto l = std::min<unsigned>(v.size, val_len);
            memcpy(val(), v.bytes, l);
        }
        [[nodiscard]] void* value() const
        {
            void * v;
            memcpy(&v, val(), std::min<unsigned>(sizeof(void*),val_len));
            return v;
        }
        [[nodiscard]] const char * s() const
        {
            return (const char *)val();
        }

        [[nodiscard]] char * s()
        {
            return (char *)val();
        }

        explicit operator value_type()
        {
            return {s(), val_len};
        }

        [[nodiscard]] value_type get_value() const
        {
            return {s(), val_len};
        }
        /**
         * Checks if a leaf matches
         * @return 0 on success.
         */
        [[nodiscard]] int compare(value_type k) const
        {
            return compare(k.bytes, k.length(), 0);
        }
        int compare(const unsigned char *key, unsigned key_len, unsigned unused(depth)) const {
            // TODO: compare is broken will fail some edge cases - see heap::buffer::compare for correct impl
            // Fail if the key lengths are different
            if (this->key_len != (LeafSize)key_len) return 1;

            // Compare the keys starting at the depth
            return memcmp(this->data, key, key_len);
        }
    } ;



    template <typename T>
    static T* get_node(node* n){
        return static_cast<T*>(n);
    }
    template <typename T>
    static T* get_node(const node_ptr& n){
        return static_cast<T*>(n.get_node());
    }
    template <typename T>
    static const T* get_node(const node_ptr& n){
        return static_cast<const T*>(n.get_node());
    }
    template <typename T>
    static const T* get_node(const node* n) {
        return static_cast<T*>(n);
    }

    /**
     * free a node while updating statistics
     */

    void free_node(node_ptr n);
    node_ptr make_leaf(value_type key, value_type v, leaf::ExpiryType ttl = 0, bool is_volatile = false);


    void free_leaf_node(art::node_ptr n);
}

