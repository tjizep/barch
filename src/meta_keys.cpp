#include "meta_keys.h"

#include "conversion.h"
#include "keys.h"
#include "sharded_store.h"

namespace barch::meta {
    art::value_type key(composite& q, const std::string& name) {
        // one component, the scope byte and the name, the way a function's key is its
        // name. noint, so a name made of digits stays a name
        const std::string scoped = std::string(1, scope_data) + name;
        return q.create(art::ts_meta, {conversion::convert(art::value_type{scoped.data(), scoped.size()}, true)});
    }

    bool name_of(art::value_type k, std::string& name) {
        if (!is_meta(k) || k.size <= composite_key_size)
            return false;
        const std::string scoped = encoded_key_as_string(k.sub(composite_key_size));
        if (scoped.empty() || scoped[0] != scope_data)
            return false;
        name = scoped.substr(1);
        return true;
    }

    bool get(const key_space_ptr& space, const std::string& name, std::string& out) {
        if (!space)
            return false;
        composite q;
        auto k = key(q, name);
        barch::sharded_store store(space);
        return store.search(k, [&](const art::node_ptr& n) {
            auto v = n.const_leaf()->get_value();
            out.assign(v.chars(), v.size);
        });
    }

    bool set(const key_space_ptr& space, const std::string& name, const std::string& value,
             std::string& err) {
        if (!space) {
            err = "no key space";
            return false;
        }
        composite q;
        auto k = key(q, name);
        auto fc = [](const art::node_ptr&) -> void {};
        try {
            barch::sharded_store store(space);
            store.with_key_write(k, [&](const barch::shard_ptr& t) {
                art::key_options opts;
                opts.set_hashed(!t->opt_ordered_keys);
                t->opt_insert(opts, k, art::value_type{value.data(), value.size()}, true, fc);
            });
        } catch (const std::exception& e) {
            err = e.what();
            return false;
        }
        return true;
    }

    bool remove(const key_space_ptr& space, const std::string& name) {
        if (!space)
            return false;
        composite q;
        auto k = key(q, name);
        barch::sharded_store store(space);
        auto fc = [](art::node_ptr) -> void {};
        return store.remove(k, fc);
    }
}
