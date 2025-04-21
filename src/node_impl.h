//
// Created by linuxlite on 3/12/25.
//

#ifndef NODE_IMPL_H
#define NODE_IMPL_H
#include "node_content.h"
/**
*  these are virtualized variable pointer nodes for external storage and or memory reduction using
*  pointer offsets or compression
*/
namespace art
{
    /**
     * variable pointer size node with 4 children
     */
    template <typename IntegerPtr>
    struct node4_v final : public encoded_node_content<4, 4, node_4, IntegerPtr>
    {
        typedef encoded_node_content<4, 4, node_4, IntegerPtr> this_type;

        [[nodiscard]] uint8_t type() const override
        {
            return node_4;
        }

        node4_v() = default;

        explicit node4_v(compressed_address address)
        {
            node4_v::from(address);
        }

        explicit node4_v(compressed_address address, node_data* data)
        {
            node4_v::from(address, data);
        }

        [[nodiscard]] node_ptr_storage get_storage() const final
        {
            node_ptr_storage storage;
            storage.emplace<node4_v>(*this);
            return storage;
        }

        using this_type::copy_from;
        using this_type::remove_child;
        using this_type::set_child;
        using this_type::add_child;
        using this_type::has_child;

        using this_type::data;
        using this_type::get_child;
        using this_type::nd;
        using this_type::insert_type;
        using this_type::index;

        void remove(node_ptr& ref, unsigned pos, unsigned char) override
        {
            remove_child(pos);
            auto& dat = nd();
            // Remove nodes with only a single child
            if (dat.occupants == 1)
            {
                node_ptr child = get_child(0);
                if (!child.is_leaf)
                {
                    // Concatenate the prefixes
                    unsigned prefix = data().partial_len;
                    if (prefix < max_prefix_llength)
                    {
                        dat.partial[prefix] = dat.keys[0];
                        ++prefix;
                    }
                    if (prefix < max_prefix_llength)
                    {
                        unsigned sub_prefix = std::min<
                            unsigned>(child->data().partial_len, max_prefix_llength - prefix);
                        memcpy(dat.partial + prefix, child->data().partial, sub_prefix);
                        prefix += sub_prefix;
                    }

                    // Store the prefix in the child
                    memcpy(child.modify()->data().partial, dat.partial, std::min<unsigned>(prefix, max_prefix_llength));
                    child.modify()->data().partial_len += dat.partial_len + 1;
                }
                ref = child;
                free_node(this);
            }
        }

        void add_child_inner(unsigned char c, node_ptr child) override
        {
            unsigned idx = index(c, gt);
            auto& dat = nd();
            // Shift to make room
            memmove(dat.keys + idx + 1, dat.keys + idx, data().occupants - idx);
            memmove(dat.children.data + idx + 1, dat.children.data + idx,
                    (data().occupants - idx) * sizeof(IntegerPtr));
            insert_type(idx);
            // Insert element
            dat.keys[idx] = c;
            set_child(idx, child);
            ++dat.occupants;
        }

        void add_child(unsigned char c, node_ptr& ref, node_ptr child) override
        {
            if (data().occupants < 4)
            {
                this->expand_pointers(ref, {child}).modify()->add_child_inner(c, child);
            }
            else
            {
                auto new_node = alloc_node_ptr(node_16, {child});
                // Copy the child pointers and the key map
                new_node.modify()->set_children(0, this, 0, data().occupants);
                new_node.modify()->set_keys(nd().keys, data().occupants);
                new_node.modify()->copy_header(this);
                ref = new_node;
                free_node(this);
                new_node.modify()->add_child(c, ref, child);
            }
        }

        [[nodiscard]] node_ptr last() const override
        {
            unsigned idx = data().occupants - 1;
            return get_child(idx);
        }

        [[nodiscard]] unsigned last_index() const override
        {
            return data().occupants - 1;
        }

        [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) const override
        {
            auto &d = nd();

            for (unsigned i = 0; i < d.occupants; i++)
            {
                if (d.keys[i] >= c && d.children[i].exists())
                {
                    return {{this, get_child(i), i, d.keys[i]}, d.keys[i] == c};
                }
            }
            return {{nullptr, nullptr, d.occupants}, false};
        }

        [[nodiscard]] trace_element next(const trace_element& te) const override
        {
            unsigned i = te.child_ix + 1;
            if (i < this->data().occupants)
            {
                return {this, this->get_child(i), i, nd().keys[i]};
            }
            return {};
        }

        [[nodiscard]] trace_element previous(const trace_element& te) const override
        {
            unsigned i = te.child_ix;
            if (i > 0)
            {
                return {this, get_child(i - 1), i - 1,nd().keys[i - 1]};
            }
            return {};
        }

    };

    typedef node4_v<int32_t> node4_4;
    typedef node4_v<uintptr_t> node4_8;
    /**
     * Variable pointer node with 16 children
     */
    template <typename IPtrType>
    struct node16_v final : public
        encoded_node_content<16, 16, node_16, IPtrType>
    {
        typedef encoded_node_content<16, 16, node_16, IPtrType> Parent;
        typedef encoded_node_content<16, 16, node_16, IPtrType> this_type;

        [[nodiscard]] uint8_t type() const override
        {
            return node_16;
        }

        node16_v() = default;

        explicit node16_v(compressed_address address)
        {
            node16_v::from(address);
        }

        explicit node16_v(compressed_address address, node_data* data)
        {
            node16_v::from(address, data);
        }

        [[nodiscard]] node_ptr_storage get_storage() const final
        {
            node_ptr_storage storage;
            storage.emplace<node16_v>(*this);
            return storage;
        }

        [[nodiscard]] unsigned index(unsigned char c, unsigned operbits) const override
        {
            unsigned i = simd::bits_oper16(this->nd().keys, simd::nuchar<16>(c), (1 << this->data().occupants) - 1, operbits);
            if (i)
            {
                i = __builtin_ctz(i);
                return i;
            }
            return this->data().occupants;
        }


        // unsigned pos = l - children;
        void remove(node_ptr& ref, unsigned pos, unsigned char) override
        {
            this->remove_child(pos);

            if (this->data().occupants == 3)
            {
                auto new_node = alloc_node_ptr(node_4, {});
                new_node.modify()->copy_header(this);
                new_node.modify()->set_keys(this->nd().keys, 3);
                new_node.modify()->set_children(0, this, 0, 3);

                ref = new_node;
                free_node(this); // ???
            }
        }

        void add_child_inner(unsigned char c, node_ptr child) override
        {
            auto &dat = this->nd();
            unsigned mask = (1 << dat.occupants) - 1;

            unsigned bitfield = simd::bits_oper16(simd::nuchar<16>(c), this->get_keys(), mask, lt);

            // Check if less than any
            unsigned idx;
            if (bitfield)
            {
                idx = __builtin_ctz(bitfield);
                memmove(dat.keys + idx + 1, dat.keys + idx, dat.occupants - idx);
                memmove(dat.children.data + idx + 1, dat.children.data + idx,
                        (dat.occupants - idx) * sizeof(typename Parent::ChildElementType));
            }
            else
                idx = dat.occupants;

            this->insert_type(idx);
            // Set the child
            dat.keys[idx] = c;
            this->set_child(idx, child);
            ++dat.occupants;
        }

        void add_child(unsigned char c, node_ptr& ref, node_ptr child) override
        {
            if (this->data().occupants < 16)
            {
                this->expand_pointers(ref, {child}).modify()->add_child_inner(c, child);
            }
            else
            {
                auto new_node = alloc_node_ptr(node_48, {child});

                // Copy the child pointers and populate the key map
                new_node.modify()->set_children(0, this, 0, this->data().occupants);
                for (unsigned i = 0; i < this->data().occupants; i++)
                {
                    new_node.modify()->set_key(this->nd().keys[i], i + 1);
                }
                new_node.modify()->copy_header(this);
                ref = new_node;
                free_node(this);
                new_node.modify()->add_child(c, ref, child);
            }
        }

        [[nodiscard]] node_ptr last() const override
        {
            return this->get_child(this->data().occupants - 1);
        }

        [[nodiscard]] unsigned last_index() const override
        {
            return this->data().occupants - 1;
        }

        [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) const override
        {
            unsigned mask = (1 << this->data().occupants) - 1;
            unsigned bf = bits_oper16(this->nd().keys, simd::nuchar<16>(c), mask, OPERATION_BIT::eq | OPERATION_BIT::gt);
            // inverse logic
            if (bf)
            {
                unsigned i = __builtin_ctz(bf);
                return {{this, this->get_child(i), i}, this->nd().keys[i] == c};
            }
            return {{nullptr, nullptr, this->data().occupants}, false};
        }

        [[nodiscard]] trace_element next(const trace_element& te) const override
        {
            unsigned i = te.child_ix + 1;
            auto &dat = this->nd();
            if (i < dat.occupants)
            {
                return {this, this->get_child(i), i, dat.keys[i]}; // the keys are ordered so fine I think
            }
            return {};
        }

        [[nodiscard]] trace_element previous(const trace_element& te) const override
        {
            unsigned i = te.child_ix;
            if (i > 0)
            {   auto &dat = this->nd();
                return {this, this->get_child(i), i - 1, dat.keys[i - 1]}; // the keys are ordered so fine I think
            }
            return {};
        }
    };

    typedef node16_v<int32_t> node16_4;
    typedef node16_v<uintptr_t> node16_8;
    /**
     * Node with 48 children, but
     * a full 256 byte field.
     */
    template <typename PtrEncodedType>
    struct node48 final : public encoded_node_content<48, 256, node_48, PtrEncodedType>
    {
        typedef encoded_node_content<48, 256, node_48, PtrEncodedType> this_type;

        using this_type::copy_from;
        using this_type::remove_child;
        using this_type::set_child;
        using this_type::add_child;
        using this_type::has_child;

        using this_type::data;
        using this_type::get_child;
        using this_type::nd;
        using this_type::insert_type;
        using this_type::index;
        node48() = default;

        explicit node48(compressed_address address)
        {
            node48::from(address);
        }

        explicit node48(compressed_address address, node_data* data)
        {
            node48::from(address, data);
        }

        [[nodiscard]] node_ptr_storage get_storage() const final
        {
            node_ptr_storage storage;
            storage.emplace<node48>(*this);
            return storage;
        }

        [[nodiscard]] uint8_t type() const override
        {
            return node_48;
        }

        [[nodiscard]] unsigned index(unsigned char c) const override
        {
            unsigned i = nd().keys[c];
            if (i)
                return i - 1;
            return 256;
        }

        void remove(node_ptr& ref, unsigned pos, unsigned char key) override
        {
            if ((unsigned)nd().keys[key] - 1 != pos)
            {
                abort();
            }
            if (nd().keys[key] == 0)
            {
                return;
            }
            if (nd().children[pos].empty())
            {
                abort();
            }
            nd().keys[key] = 0;
            nd().children[pos] = nullptr;
            nd().types[pos] = false;
            --nd().occupants;

            if (data().occupants == 12)
            {
                auto new_node = alloc_node_ptr(node_16, {});
                new_node.modify()->copy_header(this);
                unsigned child = 0;
                for (unsigned i = 0; i < 256; i++)
                {
                    pos = nd().keys[i];
                    if (pos)
                    {
                        node_ptr nn = get_child(pos - 1);
                        if (nn.null())
                        {
                            abort();
                        }
                        new_node.modify()->set_key(child, i);
                        new_node.modify()->set_child(child, nn);
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
            auto &dat = this->nd();
            // The simd optimization below seems to provide benefits
            // I leave the original here for reference or if
            // some bug emerges
            //while (dat.children[pos].exists()) pos++;
            pos = simd::first_byte_eq(dat.types, node48::KEY_COUNT,0);

            // not we do not need to call insert_type an empty child is found
            set_child(pos, child);
            dat.keys[c] = pos + 1;
            dat.occupants++;
        }

        void add_child(unsigned char c, node_ptr& ref, node_ptr child) override
        {
            auto & dat = nd();
            if (dat.occupants < 48)
            {
                this->expand_pointers(ref, {child}).modify()->add_child_inner(c, child);
            }
            else
            {
                auto new_node = alloc_node_ptr(node_256, {});
                for (unsigned i = 0; i < 256; i++)
                {
                    if (dat.keys[i])
                    {
                        node_ptr nc = get_child(dat.keys[i] - 1);
                        if (nc.null())
                        {
                            abort();
                        }
                        new_node.modify()->set_child(i, nc);
                    }
                }
                new_node.modify()->copy_header(this);
                statistics::node256_occupants += new_node->data().occupants;
                ref = new_node;
                free_node(this);
                new_node.modify()->add_child(c, ref, child);
            }
        }

        [[nodiscard]] node_ptr last() const override
        {
            return get_child(last_index());
        }

        [[nodiscard]] unsigned last_index() const override
        {
            unsigned idx = 255;
            while (!nd().keys[idx]) idx--;
            return nd().keys[idx] - 1;
        }

        [[nodiscard]] unsigned first_index() const override
        {
            unsigned uc = 0; // ?
            unsigned i;
            auto& dat = nd();
            for (; uc < 256; uc++)
            {
                i = dat.keys[uc];
                if (i > 0)
                {
                    return i - 1;
                }
            }
            return uc;
        }

        [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) const override
        {
            /*
            * find first not less than
            * todo: make lb faster by adding bit map index and using __builtin_ctz as above
            */
            unsigned uc = c;
            unsigned i = 0;
            auto &dat = this->nd();
            int test = simd::first_byte_gt(dat.keys+uc, 256-uc,0) + uc;
            if (test < 256)
            {
                i = dat.keys[test];
                trace_element te = {this, get_child(i - 1), i - 1,(uint8_t)uc};
                return {te, (i == c)};
            }
#if 0
            for (; uc < 256; uc++)
            {
                i = dat.keys[uc];
                if (i > 0)
                {
                    if (test!=i)
                    {
                        std_log("failed test?");
                    }
                    trace_element te = {this, get_child(i - 1), i - 1,(uint8_t)uc};
                    return {te, (i == c)};
                }
            }
#endif
            return {{nullptr, nullptr, 256}, false};
        }

        [[nodiscard]] trace_element previous(const trace_element& te) const override
        {
            unsigned uc = te.k, i;
            auto &dat = this->nd();
            for (; uc > 0; --uc)
            {
                i = dat.keys[uc];
                if (i > 0)
                {
                    return {this, get_child(i - 1), i - 1, (uint8_t)uc};
                }
            }
            return {};
        }

        [[nodiscard]] trace_element next(const trace_element& te) const override
        {
            unsigned uc = te.k + 1, i;
            auto &dat = this->nd();
            for (; uc < 256; uc++)
            {
                i = dat.keys[uc];
                if (i > 0)
                {
                    return {this, get_child(i - 1), i - 1, (uint8_t)uc};
                }
            }
            return {};
        }
        [[nodiscard]] virtual unsigned leaf_only_distance(unsigned start, unsigned& size ) const
        {
            size = 0;
            return 0;
            unsigned r = start;
            auto& dat = nd();
            unsigned last_leaf = 256;

            for (; r < 256; ++r)
            {
                if (dat.types[r] != 0)
                {

                    if (dat.types[r] == non_leaf_type)
                        return last_leaf < 256 ? dat.keys[last_leaf] - 1 : 256;
                    ++size;
                    last_leaf = r;
                }
            }
            if (last_leaf < 256)
                return dat.keys[last_leaf] - 1;
            return 256;

        }
    };

    typedef node48<int32_t> node48_4;
    typedef node48<uintptr_t> node48_8;
    /**
     * variable pointer size node with 256 children
     */
    template <typename intptr_t>
    struct node256 final : public encoded_node_content<256, 0, node_256, intptr_t>
    {
        typedef encoded_node_content<256, 0, node_256, intptr_t> this_type;

        using this_type::set_child;
        using this_type::has_child;
        using this_type::data;
        using this_type::get_child;
        using this_type::nd;
        using this_type::has_any;

        node256() = default;

        explicit node256(compressed_address address)
        {
            node256::from(address);
        }

        explicit node256(compressed_address address, node_data* data)
        {
            node256::from(address, data);
        }

        [[nodiscard]] uint8_t type() const override
        {
            return node_256;
        }

        [[nodiscard]] node_ptr_storage get_storage() const
        {
            node_ptr_storage storage;
            storage.emplace<node256>(*this);
            return storage;
        }

        [[nodiscard]] unsigned index(unsigned char c) const override
        {
            if (nd().children[c].exists())
                return c;
            return 256;
        }

        void remove(node_ptr& ref, unsigned pos, unsigned char key) override
        {
            if (key != pos)
            {
                abort();
            }
            auto& dat = nd();
            dat.children[key] = nullptr;
            dat.types[key] = 0;
            --dat.occupants;
            --statistics::node256_occupants;
            // Resize to a node48 on underflow, not immediately to prevent
            // trashing if we sit on the 48/49 boundary
            if (dat.occupants == 37)
            {
                auto new_node = alloc_node_ptr(node_48, {});
                ref = new_node;
                new_node.modify()->copy_header(this);

                pos = 0;
                for (unsigned i = 0; i < 256; i++)
                {
                    if (has_any(i))
                    {
                        new_node.modify()->set_child(pos, get_child(i)); //[pos] = n->nd().children[i];
                        new_node.modify()->set_key(i, pos + 1);
                        pos++;
                    }
                }

                free_node(this);
            }
        }

        void add_child(unsigned char c, node_ptr&, node_ptr child) override
        {
            if (!has_child(c))
            {
                ++statistics::node256_occupants;
                ++data().occupants; // just to keep stats ok
            }
            set_child(c, child);
        }

        [[nodiscard]] node_ptr last() const override
        {
            return get_child(last_index());
        }

        [[nodiscard]] unsigned last_index() const override
        {
            auto& dat = nd();
            unsigned idx = 255;
            while (dat.children[idx].empty()) idx--;
            return idx;
        }

        [[nodiscard]] unsigned first_index() const override
        {
            auto& dat = nd();
            unsigned uc = 0; // ?
            for (; uc < 256; uc++)
            {
                if (dat.types[uc] > 0)
                {
                    return uc;
                }
            }
            return uc;
        }

        [[nodiscard]] std::pair<trace_element, bool> lower_bound_child(unsigned char c) const override
        {
            auto& dat = nd();
            unsigned i = simd::first_byte_gt(dat.types+c,256-c,0)+c;
            if (i < 256)
            {
                return {{this, get_child(i), i, (uint8_t)i}, (i == c)};
            }

#if 0
            for (unsigned i = c; i < 256; ++i)
            {
                if (has_child(i))
                {
                    if (si != i)
                    {
                        abort();
                    }
                    // because nodes are ordered accordingly
                    return {{this, get_child(i), i, (uint8_t)i}, (i == c)};
                }
            }

#endif
            return {{nullptr, nullptr, 256}, false};
        }

        [[nodiscard]] trace_element next(const trace_element& te) const override
        {
            if (te.child_ix > 255) return {};

            auto& dat = nd();
            auto r = simd::first_byte_gt(dat.keys+te.child_ix+1,256-te.child_ix-1,0);
            unsigned i = te.child_ix + r + 1;
            if (i < 256)
                return {this, get_child(i), i, (uint8_t)i};
            return {};
        }

        [[nodiscard]] trace_element previous(const trace_element& te) const override
        {
            if (!te.child_ix) return {};
            for (unsigned i = te.child_ix - 1; i > 0; --i)
            {
                // these aren't sparse so shouldn't take long
                if (has_child(i))
                {
                    // because nodes are ordered accordingly
                    return {this, get_child(i), i, (uint8_t)i};
                }
            }
            return {};
        }
        [[nodiscard]] virtual unsigned leaf_only_distance(unsigned start, unsigned& size) const
        {
            unsigned r = start;
            auto& dat = nd();
            unsigned last_leaf = 256;
            size = 0;
            for (; r < 256; ++r)
            {
                if (dat.types[r] != 0)
                {

                    if (dat.types[r] == non_leaf_type)
                        return last_leaf;
                    ++size;
                    last_leaf = r;
                }
            }
            return last_leaf;
            //r = simd::count_chars(dat.types+start, size,leaf_type);
            //if ( simd::count_chars(dat.types+start,size,non_leaf_type)>0)
            //    return 0;
            //return r;
        };
    };

    typedef node256<int32_t> node256_4;
    typedef node256<uintptr_t> node256_8;
}
#endif //NODE_IMPL_H
