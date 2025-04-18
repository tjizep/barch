//
// Created by linuxlite on 3/12/25.
//

#ifndef NODE_ABSTRACT_H
#define NODE_ABSTRACT_H
#include "nodes.h"
#include "statistics.h"
#include "simd.h"

namespace art
{
    template <typename EncodingType>
    bool ok(const node* n, uintptr_t base)
    {
        if (n == nullptr) return true;
        if (sizeof(EncodingType) == sizeof(uintptr_t)) return true;

        auto uval = n->get_address().address();

        int64_t ival = uval - base;
        int64_t imax = i64max<EncodingType>();
        int64_t imin = i64min<EncodingType>();
        return (ival > imin && ival < imax);
    }

    template <typename EncodingType, bool IsLeaf>
    struct encoded_element
    {
    private:
        EncodingType& value;
        uintptr_t base;

    public:
        encoded_element(EncodingType& value, uintptr_t base) : value(value), base(base)
        {
        }

        encoded_element& operator=(nullptr_t)
        {
            value = 0;
            return *this;
        }

        encoded_element& operator=(const node_ptr& t)
        {
            if (t.is_leaf != IsLeaf)
            {
                abort();
            }
            if (t.is_leaf)
            {
                set_leaf(t.logical);
            }
            else
            {
                set_node(t.get_node());
            }
            return *this;
        }

        void set_leaf(const logical_leaf& ptr)
        {
            if (!IsLeaf)
            {
                abort();
            }
            value = ptr.null() ? 0 : ptr.address();
        }

        [[nodiscard]] logical_leaf get_leaf() const
        {
            if (!IsLeaf)
            {
                abort();
            }
            return logical_leaf(value);
        }

        void set_node(const node* ptr)
        {
            if (IsLeaf)
            {
                abort();
            }
            if (ptr == nullptr)
            {
                value = 0;
                return;
            }
            if (sizeof(EncodingType) == sizeof(uintptr_t))
            {
                value = ptr->get_address().address();
                return;
            }
            value = (ptr->get_address().address() - base);
        }

        [[nodiscard]] node_ptr modify()
        {
            if (IsLeaf)
            {
                abort();
            }
            if (value == 0)
            {
                return nullptr;
            }
            return resolve_write_node(compressed_address((int64_t)value + base));
        }

        [[nodiscard]] node_ptr get_node() const
        {
            if (IsLeaf)
            {
                abort();
            }
            if (value == 0)
            {
                return nullptr;
            }
            return resolve_read_node(compressed_address((int64_t)value + base));
        }

        [[nodiscard]] node_ptr cget() const
        {
            if (IsLeaf)
            {
                abort();
            }
            if (value == 0)
            {
                return nullptr;
            }
            return resolve_read_node(compressed_address((int64_t)value + base));
        }

        encoded_element& operator=(const node* ptr)
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
            if (IsLeaf)
            {
                return node_ptr(get_leaf());
            }
            return node_ptr(get_node());
        }

        explicit operator node_ptr() const
        {
            if (IsLeaf)
            {
                return node_ptr(get_leaf());
            }
            return cget(); // well have to just cast it
        }


        [[nodiscard]] bool exists() const
        {
            return value != 0;
        }

        [[nodiscard]] bool empty() const
        {
            return value == 0;
        }
    };

    template <typename EncodedType, bool IsLeaf, int SIZE>
    struct node_array
    {
        typedef encoded_element<EncodedType, IsLeaf> ProxyType;
        EncodedType data[SIZE]{};

        [[nodiscard]] uintptr_t get_offset() const
        {
            return 0;
        }

        [[nodiscard]] bool ok(const node_ptr&) const
        {
            return true;
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

    /**
     * node content to do common things related to keys and pointers on each node
     */
    template <unsigned SIZE, unsigned KEYS, uint8_t node_type, typename i_ptr_t>
    struct encoded_node_content : public node, private node::node_proxy
    {
        // test somewhere that sizeof ChildElementType == sizeof LeafElementType
        typedef i_ptr_t IntPtrType;
        typedef i_ptr_t ChildElementType;
        typedef i_ptr_t LeafElementType;
        typedef node_array<i_ptr_t, false, SIZE> ChildArrayType;
        typedef node_array<i_ptr_t, true, SIZE> LeafArrayType;

        node_data& data() override
        {
            return *refresh_cache<encoded_data>();
        }

        [[nodiscard]] const node_data& data() const override
        {
            return *refresh_cache<encoded_data>();
        }

        struct encoded_data : node_data
        {
            unsigned char keys[KEYS]{};

            uint8_t types[SIZE]{};

            union
            {
                LeafArrayType leaves{};
                ChildArrayType children;
            };
        };

        encoded_data& nd()
        {
            return *refresh_cache<encoded_data>();
        }

        const encoded_data& nd() const
        {
            return *refresh_cache<encoded_data>();
        }

        compressed_address create_data() final
        {
            address = get_node_compression().new_address(alloc_size());
            encoded_data* r = get_node_compression().modify<encoded_data>(address);
            r->type = node_type;
            r->pointer_size = sizeof(IntPtrType);
            dcache = r;
            switch (node_type)
            {
            case node_4:
                ++statistics::n4_nodes;
                break;
            case node_16:
                ++statistics::n16_nodes;
                break;
            case node_48:
                ++statistics::n48_nodes;
                break;
            case node_256:
                ++statistics::n256_nodes;
                break;
            default:
                abort();
            }

            return address;
        }

        void free_data() final
        {
            switch (node_type)
            {
            case node_4:
                --statistics::n4_nodes;
                break;
            case node_16:
                --statistics::n16_nodes;
                break;
            case node_48:
                --statistics::n48_nodes;
                break;
            case node_256:
                --statistics::n256_nodes;
                break;
            default:
                abort();
            }
            //if (address.is_null_base())
            //    abort();
            get_node_compression().free(address, alloc_size());
        }

        encoded_node_content() = default;

        encoded_node_content& create()
        {
            create_data();
            return *this;
        }

        void from(compressed_address address, node_data* data)
        {
            set_lazy<IntPtrType, node_type>(address, data);
        };

        ~encoded_node_content() override = default;

        void check_object() const
        {
        }

        encoded_node_content(const encoded_node_content& content) = default;
        encoded_node_content& operator=(const encoded_node_content&) = delete;

        [[nodiscard]] compressed_address get_address() const final
        {
            return address;
        }

        [[nodiscard]] size_t alloc_size() const final
        {
            return sizeof(encoded_data);
        }

        void set_leaf(unsigned at) final
        {
            check_object();
            if (SIZE <= at)
                abort();
            nd().types[at] = leaf_type;
        }

        void set_child(unsigned at, node_ptr node) final
        {
            check_object();
            if (SIZE <= at)
                abort();
            auto& dat = nd();
            dat.types[at] = node.is_leaf ? leaf_type : non_leaf_type;
            if (node.is_leaf)
            {
                dat.leaves[at] = node.logical;
            }
            else
            {
                dat.children[at] = node;
            }
        }

        [[nodiscard]] bool is_leaf(unsigned at) const final
        {
            check_object();
            if (SIZE <= at)
                abort();
            bool is = nd().types[at] == leaf_type;
            return is;
        }

        [[nodiscard]] bool has_child(unsigned at) const final
        {
            check_object();
            if (SIZE <= at)
                abort();
            return nd().children[at].exists();
        }

        [[nodiscard]] node_ptr get_node(unsigned at) const final
        {
            check_object();
            if (at < SIZE)
            {
                auto& dat = nd();
                return dat.types[at] == leaf_type ? node_ptr(dat.leaves[at]) : dat.children[at].get_node();
            }
            return nullptr;
        }

        node_ptr get_node(unsigned at) final
        {
            check_object();
            if (at < SIZE)
                return nd().types[at] == leaf_type ? node_ptr(nd().leaves[at]) : node_ptr(nd().children[at]);

            return nullptr;
        }
#if 0
        node_ptr get_child(unsigned at) final
        {
            check_object();
            return get_node(at);
        }
#endif
        [[nodiscard]] node_ptr get_child(unsigned at) const final
        {
            check_object();
            return get_node(at);
        }

        [[nodiscard]] bool ok_child(node_ptr np) const override
        {
            check_object();
            if (np.is_leaf)
                return nd().leaves.ok(np);
            return nd().children.ok(np);
        }

        [[nodiscard]] bool ok_children(const children_t& child) const final
        {
            check_object();
            for (auto np : child)
            {
                // TODO: compress leaf addresses again
                if (np.is_leaf)
                {
                    return nd().leaves.ok(np);
                }
                else
                {
                    return nd().children.ok(np);
                }
            }
            return true;
        }


        // TODO: NB check where this function is used
        [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override
        {
            check_object();
            unsigned i;
            if (KEYS < data().occupants)
            {
                return data().occupants;
            }
            if (operbits & (eq & gt))
            {
                for (i = 0; i < data().occupants; ++i)
                {
                    if (nd().keys[i] >= c)
                        return i;
                }
                if (operbits == (eq & gt)) return data().occupants;
            }
            if (operbits & (eq & lt))
            {
                for (i = 0; i < data().occupants; ++i)
                {
                    if (nd().keys[i] <= c)
                        return i;
                }
                if (operbits == (eq & lt)) return data().occupants;
            }
            if (operbits & eq)
            {
                for (i = 0; i < data().occupants; ++i)
                {
                    if (KEYS > 0 && nd().keys[i] == c)
                        return i;
                }
            }
            if (operbits & gt)
            {
                for (i = 0; i < data().occupants; ++i)
                {
                    if (KEYS > 0 && nd().keys[i] > c)
                        return i;
                }
            }
            if (operbits & lt)
            {
                for (i = 0; i < data().occupants; ++i)
                {
                    if (KEYS > 0 && nd().keys[i] < c)
                        return i;
                }
            }
            return data().occupants;
        }

        [[nodiscard]] unsigned index(unsigned char c) const override
        {
            check_object();
            return index(c, eq);
        }

        [[nodiscard]] node_ptr find(unsigned char c) const override
        {
            check_object();
            return get_child(index(c));
        }

        [[nodiscard]] node_ptr find(unsigned char c, unsigned operbits) const override
        {
            check_object();
            return get_child(index(c, operbits));
        }

        [[nodiscard]] unsigned first_index() const override
        {
            check_object();
            return 0;
        };

        [[nodiscard]] const unsigned char* get_keys() const override
        {
            check_object();
            return nd().keys;
        }

        [[nodiscard]] const unsigned char& get_key(unsigned at) const final
        {
            check_object();
            if (at < KEYS)
                return nd().keys[at];
            abort();
        }

        void set_key(unsigned at, unsigned char k) final
        {
            check_object();

            auto max_keys = KEYS;
            if (at < max_keys)
            {
                nd().keys[at] = k;
                return;
            }
            abort();
        }

        unsigned char& get_key(unsigned at) final
        {
            check_object();
            if (at < KEYS)
                return nd().keys[at];
            abort();
        }

        bool has_any(unsigned pos)
        {
            check_object();
            if (SIZE <= pos)
                abort();

            return nd().children[pos].exists();
        }

        void set_keys(const unsigned char* other_keys, unsigned count) final
        {
            check_object();
            if (KEYS < count)
                abort();
            memcpy(nd().keys, other_keys, count);
        }

        void insert_type(unsigned pos)
        {
            check_object();
            if (SIZE <= pos)
                abort();
            unsigned count = data().occupants;
            auto& n = nd();
            for (unsigned p = count; p > pos; --p)
            {
                n.types[p] = n.types[p - 1];
            }
        }

        void remove_type(unsigned pos)
        {
            check_object();
            if (SIZE < pos)
                abort();
            unsigned count = data().occupants;
            auto& n = nd();
            for (unsigned p = pos; p < count - 1; ++p)
            {
                n.types[p] = n.types[p + 1];
            }
        }

        [[nodiscard]] bool child_type(unsigned at) const override
        {
            check_object();
            return nd().types[at] == leaf_type;
        }

        void set_children(unsigned dpos, const node* other, unsigned spos, unsigned count) override
        {
            check_object();
            if (dpos < SIZE && count <= SIZE)
            {
                auto& dat = nd();

                for (unsigned d = dpos; d < count; ++d)
                {
                    auto n = other->get_child(d + spos);
                    if (n.is_leaf)
                    {
                        dat.leaves[d] = n.logical;
                    }
                    else
                        dat.children[d] = n;
                }
                for (unsigned t = 0; t < count; ++t)
                {
                    dat.types[t + dpos] = other->child_type(t + spos);
                }
            }
            else
            {
                abort();
            }
        }

        template <typename S>
        void set_children(unsigned pos, const S* other, unsigned count)
        {
            check_object();
            set_children(pos, other, 0, count);
        }

        void remove_child(unsigned pos)
        {
            check_object();
            if (pos < KEYS && KEYS == SIZE)
            {
                auto& dat = nd();
                memmove(dat.keys + pos, dat.keys + pos + 1, dat.occupants - 1 - pos);
                memmove(dat.children.data + pos, dat.children.data + pos + 1,
                        (dat.occupants - 1 - pos) * sizeof(ChildElementType));
                remove_type(pos);
                dat.keys[dat.occupants - 1] = 0;
                dat.children[dat.occupants - 1] = nullptr;
                --dat.occupants;
            }
            else
            {
                abort();
            }
        }

        void copy_header(node_ptr src) override
        {
            check_object();
            auto& dat = nd();
            auto& sd = src->data();
            if (sd.occupants > SIZE)
            {
                abort();
            }
            dat.occupants = sd.occupants;
            dat.partial_len = sd.partial_len;
            memcpy(dat.partial, sd.partial, std::min<unsigned>(max_prefix_llength, sd.partial_len));
        }

        void copy_from(node_ptr s) override
        {
            check_object();
            if (s->data().occupants > SIZE)
            {
                abort();
            }
            this->copy_header(s);
            set_keys(s->get_keys(), s->data().occupants);
            set_children(0, (node*)s, 0, s->data().occupants);
            if (data().occupants != s->data().occupants)
            {
                abort();
            }
        }

        [[nodiscard]] node_ptr expand_pointers(node_ptr& ref, const children_t& children) override
        {
            check_object();
            if (ok_children(children)) return this;
            node_ptr n = alloc_8_node_ptr(type());
            n.modify()->copy_from(this);
            free_node(this);
            ref = n;
            return n;
        }

        [[nodiscard]] unsigned ptr_size() const override
        {
            check_object();
            return sizeof(ChildElementType);
        };

        [[nodiscard]] virtual unsigned leaf_only_distance(unsigned , unsigned& size ) const
        {
            size = 0;
            return 256;
        }
    protected:
    };
}
#endif //NODE_ABSTRACT_H
