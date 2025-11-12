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
thread_local composite query;
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
    int bpop(caller& cc, const arg_t& args, bool tail, bool blocking = true) {
        if (args.size() < 3) {
            return cc.wrong_arity();
        }
        if (blocking && cc.has_blocks()) {
            return cc.push_error("block already set");
        }
        caller::keys_t blocks;

        auto spc = cc.kspace();
        uint64_t time_out = blocking ? conversion::to_double(conversion::as_variable(args.back()))*1000ull : 0;
        cc.start_array();
        size_t popped = 0;
        for (size_t ki = 1; ki < args.size() - 1; ++ki) {
            if (key_ok(args[ki]) != 0) {
                return cc.push_error("invalid key");
            }
            auto t = spc->get(args[ki]);

            storage_release release(t);
            composite li;
            auto container = conversion::convert(args[ki]);
            auto key = query.create({container});
            auto value = t->search(key);
            if (value.null()) {
                if (blocking) blocks.emplace_back(args[ki].to_string(),t->get_shard_number());
                // the key does not exist at all and we must add a block here
                continue;
            }
            li.create({container});

            list_header header {value.const_leaf()->get_value()};
            int64_t start = conversion::dec_bytes_to_int(header.start);
            int64_t end = conversion::dec_bytes_to_int(header.end);
            if (start == end) {
                if (blocking) blocks.emplace_back(args[ki].to_string(),t->get_shard_number());
                continue; // this condition is somewhat strange but a key is already registered for blocking
                // we do not send a notification in this case
            }
            cc.push_encoded_key(value.cl()->get_key());
            if (tail) {
                li.push(conversion::comparable_key(--end));
                header.end = conversion::make_int64_bytes(end);
            } else {
                li.push(conversion::comparable_key(start++));
                header.start = conversion::make_int64_bytes(start);
            }
            ++popped;
            t->remove(li.create(),[&](const art::node_ptr& old) {
                if (!old.null())
                    cc.push_vt(old.const_leaf()->get_value());
            }); // remove the key
            li.pop_back();

            if (start == end) {
                // usually the entire key must go (but were blocking so ++blocks)
                if (blocking) blocks.emplace_back(args[ki].to_string(),t->get_shard_number());
            }
            // todo: we can set the header directly but that change would not be replicated
            t->insert(key, header.as_value(), true);
        }
        cc.end_array(0);
        if (!blocks.empty() && popped == 0) {
            cc.add_block(blocks, time_out,[tail](caller& call, const caller::keys_t& keys) {
                // this gets called as soon as the key gets pushed
                // it happens on the same thread as the caller
                if (keys.empty()) {
                    // theres a timeout
                    call.push_null();
                    return;
                }
                for (auto& k: keys) {
                    bpop(call, {"bpop", k.key, "0"},tail,false);
                }
            });
        }
        return 0;
    }
    int BLPOP(caller& cc, const arg_t& args) {
        return bpop(cc, args, true);
    }
    int BRPOP(caller& cc, const arg_t& args) {
        return bpop(cc, args, false);
    }
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
            return cc.push_null();
        }
        auto t = cc.kspace()->get(args[1]);
        storage_release release(t);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = query.create({container});
        li.create({container});
        auto added = t->insert(key, header.as_value(), false, fc);
        if (added) {
        } else {
            const art::leaf *dl = t->get_last_leaf_added().const_leaf();
            header = dl->get_value();
        }
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);
        if (start == end) {
            t->call_unblock(args[1].to_string());
        }
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
        const art::leaf *dl = t->get_last_leaf_added().const_leaf();
        h = dl->get_value();
        if (conversion::dec_bytes_to_int(h.end) != end) {
            abort_with("header not updated");
        }
        return cc.push_ll(end - start);
    }

    int LPOP(caller& cc, const arg_t& args) {
        if (args.size() < 3) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        auto count = conversion::to_int64(conversion::as_variable(args[2]));
        auto t = cc.kspace()->get(args[1]);
        storage_release release(t);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();
        }
        li.create({container});

        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);
        if (start == end) {
            return cc.push_null();
        }
        int64_t actual = 0;

        for (auto i = 0; i < count; ++i) {
            li.push(conversion::comparable_key(--end));
            t->remove(li.create());
            li.pop_back();
            header.end = conversion::make_int64_bytes(end);
            if (start == end) {
                t->remove(key);
                return cc.push_ll(end - start);
            }
            ++actual;
        }
        // todo: we can set the header directly but that change would not be replicated
        t->insert(key, header.as_value(), true);

        return cc.push_ll(end - start);
    }

    int LLEN(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        auto t = cc.kspace()->get(args[1]);
        storage_release release(t);
        auto container = conversion::convert(args[1]);
        auto key = query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_ll(0);

        }
        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);

        return cc.push_ll(end - start);

    }
    int LBACK(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        auto t = cc.kspace()->get(args[1]);
        storage_release release(t);
        auto container = conversion::convert(args[1]);
        auto key = query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();

        }
        list_header header {value.const_leaf()->get_value()};
        int64_t end = conversion::dec_bytes_to_int(header.end);
        composite li;
        li.create({container,conversion::comparable_key(--end)});
        auto back = t->search(li.create());
        if (back.null()) {
            return cc.push_null();
        }
        return cc.push_vt(back.const_leaf()->get_value());
    }

    int LFRONT(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        auto t = cc.kspace()->get(args[1]);
        storage_release release(t);
        auto container = conversion::convert(args[1]);
        auto key = query.create({container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();
        }
        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        composite li;
        li.create({container,conversion::comparable_key(start)});
        auto front = t->search(li.create());
        if (front.null()) {
            return cc.push_null();
        }
        return cc.push_vt(front.const_leaf()->get_value());
    }
}
