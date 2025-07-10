//
// Created by teejip on 7/7/25.
//

#include "list_api.h"
#include "value_type.h"
#include "valkeymodule.h"
#include "art.h"
#include "caller.h"
#include "composite.h"
#include "module.h"
#include "keys.h"
#include "vk_caller.h"
template<typename T>
art::value_type vt(const T& t) {
    return {(const uint8_t*)&t,sizeof(t)};
}
struct list_key {

    conversion::byte_comparable<int64_t> parent = conversion::make_int64_bytes(0ll);
    conversion::byte_comparable<int64_t> item = conversion::make_int64_bytes(0ll);

    [[nodiscard]] art::value_type as_value() const {
        return {(const uint8_t*)&parent,sizeof(list_key)};
    }

};

static char id_key_prefix_[7] = "_@id__";

static art::value_type id_key_prefix = {(const uint8_t*)id_key_prefix_,sizeof(id_key_prefix_)};


struct list_header {

    conversion::byte_comparable<int64_t> id = conversion::make_int64_bytes(0ll);
    conversion::byte_comparable<int64_t> start = conversion::make_int64_bytes(0ll);
    conversion::byte_comparable<int64_t> end = conversion::make_int64_bytes(0ll);
    list_header() = default;
    explicit list_header(art::value_type v) {
        *this = v;
    }

    [[nodiscard]] uint64_t size() const {
        return conversion::dec_bytes_to_int(end) - conversion::dec_bytes_to_int(start);
    }

    [[nodiscard]] art::value_type as_value() const {
        return {(const uint8_t*)&id,sizeof(list_header)};
    }

    list_header& operator=(art::value_type vt) {
        if (vt.size != sizeof(list_header)) {
            abort_with("invalid header size");
        }
        memcpy(&id,vt.bytes,sizeof(list_header));
        return *this;
    }
};

extern "C"{
    int LPUSH(caller& cc, const arg_t& args) {
        int64_t updated = 0;
        list_header header;
        auto fc = [&](const art::node_ptr &) -> void {
            ++updated;
        };
        if (args.size() < 3) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.null();
        }
        auto t = get_art(args[1]);
        storage_release release(t->latch);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = t->query.create({container});
        li.create({container});
        auto added = t->insert(key, header.as_value(), false, fc);
        if (added) {
        } else {
            const art::leaf *dl = t->last_leaf_added.const_leaf();
            header = dl->get_value();
        }
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);
        for (size_t n = 2; n < args.size(); n += 1) {
            li.push(conversion::comparable_key(end));
            header.end = conversion::make_int64_bytes(++end);
            if (conversion::dec_bytes_to_int(header.end) != end) {
                abort_with("end not updated");
            }
            t->insert(li.create(), args[n], true, fc);
            t->insert(key, header.as_value(), true);
            li.pop_back();

        }
        // todo: we can set the header directly but that change would not be replicated - currently
        t->insert(key, header.as_value(), true);
        list_header h;
        const art::leaf *dl = t->last_leaf_added.const_leaf();
        h = dl->get_value();
        if (conversion::dec_bytes_to_int(h.end) != end) {
            abort_with("header not updated");
        }
        return cc.long_long(end - start);
    }

    int LPOP(caller& cc, const arg_t& args) {
        if (args.size() < 3) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.null();
        }
        auto count = conversion::to_int64(conversion::as_variable(args[2]));
        auto t = get_art(args[1]);
        storage_release release(t->latch);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = t->query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.null();
        }
        li.create({container});

        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);
        if (start == end) {
            return cc.null();
        }
        int64_t actual = 0;

        for (auto i = 0; i < count; ++i) {
            li.push(conversion::comparable_key(--end));
            t->remove(li.create());
            li.pop_back();
            header.end = conversion::make_int64_bytes(end);
            if (start == end) {
                t->remove(key);
                return cc.long_long(end - start);
            }
            ++actual;
        }
        // todo: we can set the header directly but that change would not be replicated
        t->insert(key, header.as_value(), true);

        return cc.long_long(end - start);
    }

    int LLEN(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.null();
        }
        auto t = get_art(args[1]);
        storage_release release(t->latch);
        auto container = conversion::convert(args[1]);
        auto key = t->query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.long_long(0);

        }
        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);

        return cc.long_long(end - start);

    }
}
