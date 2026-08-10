//
// Created by teejip on 7/7/25.
//

#include "list_api.h"
#include "sharded_store.h"
#include <limits>
#include <cstdlib>
#include <cmath>
#include <cerrno>
#include "key_type.h"
#include "value_type.h"
#include "../external/include/valkeymodule.h"
#include "art/art.h"
#include "caller.h"
#include "composite.h"
#include "module.h"
#include "keys.h"


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
        // The timeout is read and judged before anything is locked. It used to be taken
        // after, and taken on trust: `BLPOP k 0x7FFFFFFFFFFFFF` was accepted, the wait
        // never ended, and because the locks were already held the whole store went with
        // it - every later command answered nil until the server was restarted. redis
        // refuses the same timeout outright.
        uint64_t time_out = 0;
        if (blocking) {
            double secs = 0;
            // strtod rather than the strict reader: redis parses the timeout with the C
            // library, which takes hex and exponent forms, and then judges the value. A
            // caller writing 0x7FFFFFFFFFFFFF should be told the number is too big, not
            // that it is not a number
            std::string t(args.back().chars(), args.back().size);
            char *tail = nullptr;
            errno = 0;
            secs = std::strtod(t.c_str(), &tail);
            if (t.empty() || tail != t.c_str() + t.size() || std::isnan(secs)) {
                return cc.push_error("timeout is not a float or out of range");
            }
            if (secs < 0) {
                return cc.push_error("timeout is negative");
            }
            // the wait is kept in milliseconds, so a timeout that cannot be one is not a
            // very long wait, it is an unreadable number
            if (errno == ERANGE || std::isinf(secs)
                || !(secs * 1000.0 < (double) std::numeric_limits<int64_t>::max())) {
                return cc.push_error("timeout is out of range");
            }
            time_out = (uint64_t) (secs * 1000.0);
        }

        {
            // a name holding a string is a wrong type, and saying so matters more here
            // than elsewhere: without it `BLPOP notalist 0` waits for a list that can
            // never arrive, and 0 means forever
            barch::sharded_store wt(cc.kspace());
            for (size_t ki = 1; ki + 1 < args.size(); ++ki) {
                if (barch::kind_of(wt, args[ki]) == barch::key_kind::string) {
                    return cc.push_error(barch::wrong_type_message());
                }
            }
        }

        caller::keys_t blocks;
        auto spc = cc.kspace();
        barch::sharded_store store(spc);
        // a multi key pop has to hold every shard, a single key one only its own
        auto locks = args.size() > 3 ? store.lock_space_write()
                                     : store.lock_key_write(args[1]);
        // the array is not opened until there is something to put in it. A blocking pop
        // that finds nothing has no reply to give yet - the block callback answers later,
        // or the timeout does - and opening one here would have to be taken back, which
        // only the RESP builder can do. A reply builder that streams as it goes, as the
        // valkey module one does, cannot rewind a postponed length array it has already
        // begun, so it would send an empty one. Not starting it suits both.
        size_t popped = 0;
        auto open_reply = [&]() {
            if (popped == 0) cc.start_array();
        };
        for (size_t ki = 1; ki < args.size() - 1; ++ki) {
            if (key_ok(args[ki]) != 0) {
                return cc.push_error("invalid key");
            }
            auto t = spc->get(args[ki]);

            composite li;
            auto container = conversion::convert(args[ki]);
            auto key = query.create(art::ts_list, {container});
            auto value = t->search(key);
            if (value.null()) {
                if (blocking) blocks.emplace_back(args[ki].to_string(),t->get_shard_number());
                // the key does not exist at all and we must add a block here
                continue;
            }
            li.create(art::ts_list, {container});

            list_header header {value.const_leaf()->get_value()};
            int64_t start = conversion::dec_bytes_to_int(header.start);
            int64_t end = conversion::dec_bytes_to_int(header.end);
            if (start == end) {
                if (blocking) blocks.emplace_back(args[ki].to_string(),t->get_shard_number());
                continue; // this condition is somewhat strange but a key is already registered for blocking
                // we do not send a notification in this case
            }
            open_reply(); // first thing to say, so the array starts here
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
        if (popped > 0) {
            cc.end_array();
        }
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
        return bpop(cc, args, false);
    }
    int BRPOP(caller& cc, const arg_t& args) {
        return bpop(cc, args, true);
    }
    // `at_tail` means the high index end, which is what LBACK reads. It was called
    // `left`, which read as the head and is the opposite of what it selects
    int push(caller& cc, const arg_t& args, bool at_tail) {
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
        barch::sharded_store store(cc.kspace());
        // a name already holding a plain value is not a list to be pushed onto, and
        // neither is one already holding a hash or an ordered set. Checked before the
        // lock: this routes to shards of its own - see key_type.h
        if (!barch::container_writable(store, args[1], barch::container_kind::list)) {
            return cc.push_error(barch::wrong_type_message());
        }
        auto t = store.write_locked(args[1]);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        li.create(art::ts_list, {container});
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
        // every element has to fit in a leaf with its index. Checked before any of them is
        // written, so a push either happens or does not: it used to insert what fit, fail
        // the rest silently, and still advance the header - the list then reported a
        // length it did not have and LLEN agreed with it
        for (size_t n = 2; n < args.size(); n += 1) {
            if (!fits_in_leaf(key.size + numeric_key_size, args[n].size)) {
                return cc.push_error(too_large_message());
            }
        }
        for (size_t n = 2; n < args.size(); n += 1) {
            if (at_tail) {
                li.push(conversion::comparable_key(end));
                header.end = conversion::make_int64_bytes(++end);
                if (conversion::dec_bytes_to_int(header.end) != end) {
                    abort_with("end not updated");
                }
            }else {
                li.push(conversion::comparable_key(--start));
                header.start = conversion::make_int64_bytes(start);
                if (conversion::dec_bytes_to_int(header.start) != start) {
                    abort_with("end not updated");
                }
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
        if (at_tail) {
            if (conversion::dec_bytes_to_int(h.end) != end) {
                abort_with("header not updated");
            }
        }else {
            if (conversion::dec_bytes_to_int(h.start) != start) {
                abort_with("header not updated");
            }
        }
        return cc.push_ll(end - start);
    }
    // L* work on the head, which is the low index end, and R* on the tail. These were
    // the other way round, so LPUSH appended and RPUSH prepended - the reverse of redis,
    // and the reason LFRONT appeared to answer with the most recently LPUSHed value.
    // The stored layout has not changed: a list saved before this reads back in the same
    // order, but the command that built it is now the other one. See TODO 38
    int LPUSH(caller& cc, const arg_t& args) {
        return push(cc, args, false);
    }
    int RPUSH(caller& cc, const arg_t& args) {
        return push(cc, args, true);
    }
    /**
     * LPUSHX and RPUSHX push only onto a list that is already there.
     *
     * A missing list is not created and nothing is written; the reply is 0, which is the
     * length a caller would have got had it existed and been empty - redis makes the same
     * choice, and it is what lets a caller append to a list without ever creating one.
     */
    static int pushx(caller& cc, const arg_t& args, bool at_tail) {
        if (args.size() < 3) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        barch::sharded_store store(cc.kspace());
        if (barch::kind_of(store, args[1]) == barch::key_kind::string) {
            return cc.push_error(barch::wrong_type_message());
        }
        {
            // exists is asked before the push so that a missing list is left missing
            auto t = store.read_locked(args[1]);
            composite q;
            auto key = q.create(art::ts_list, {conversion::convert(args[1])});
            if (t->search(key).null()) {
                return cc.push_ll(0);
            }
        }
        return push(cc, args, at_tail);
    }
    int LPUSHX(caller& cc, const arg_t& args) {
        return pushx(cc, args, false);
    }
    int RPUSHX(caller& cc, const arg_t& args) {
        return pushx(cc, args, true);
    }
    /**
     * LPOP key [count] / RPOP key [count]
     *
     * The count is optional, as in redis, and the reply is what was removed rather than
     * the length left behind. It used to require the count and answer with the remaining
     * length, so a caller who wanted the values had to read them first and hope nothing
     * else popped in between. See TODO 38.
     *
     * Without a count the reply is one bulk string, or nil when there was nothing to
     * take. With a count it is an array of up to that many, which may be shorter than
     * asked for and may be empty.
     */
    int pop(caller& cc, const arg_t& args, bool at_tail) {
        if (args.size() < 2 || args.size() > 3) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        {
            // popping from a name that holds a string is a wrong type, not an empty list
            barch::sharded_store wt(cc.kspace());
            if (barch::kind_of(wt, args[1]) == barch::key_kind::string) {
                return cc.push_error(barch::wrong_type_message());
            }
        }
        const bool has_count = args.size() == 3;
        int64_t count = 1;
        if (has_count) {
            count = conversion::to_i64(conversion::as_variable(args[2]));
            if (count < 0) {
                return cc.push_error("value is out of range, must be positive");
            }
        }
        barch::sharded_store store(cc.kspace());
        auto t = store.write_locked(args[1]);
        composite li;
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();
        }
        li.create(art::ts_list, {container});

        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);

        // the bytes are copied out before the entry is removed, because the leaf they
        // point at goes away with it
        heap::std_vector<std::string> popped;
        for (int64_t i = 0; i < count; ++i) {
            if (start == end) break;
            if (at_tail) {
                li.push(conversion::comparable_key(--end));
            } else {
                li.push(conversion::comparable_key(start++));
            }
            auto entry = li.create();
            auto held = t->search(entry);
            if (!held.null()) {
                auto vt = held.const_leaf()->get_value();
                popped.emplace_back(vt.chars(), vt.size);
            }
            t->remove(entry);
            li.pop_back();
            if (at_tail) {
                header.end = conversion::make_int64_bytes(end);
            } else {
                header.start = conversion::make_int64_bytes(start);
            }
        }
        if (start == end) {
            t->remove(key);
        } else {
            // todo: we can set the header directly but that change would not be replicated
            t->insert(key, header.as_value(), true);
        }

        if (!has_count) {
            if (popped.empty()) {
                return cc.push_null();
            }
            return cc.push_vt(art::value_type{popped.front()});
        }
        cc.start_array();
        for (auto& v : popped) {
            cc.push_vt(art::value_type{v});
        }
        cc.end_array();
        return cc.ok();
    }
    int LPOP(caller& cc, const arg_t& args) {
        return pop(cc, args, false);
    }
    int RPOP(caller& cc, const arg_t& args) {
        return pop(cc, args, true);
    }
    int LLEN(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        {
            // a name holding a string is not an empty list
            barch::sharded_store wt(cc.kspace());
            if (barch::kind_of(wt, args[1]) == barch::key_kind::string) {
                return cc.push_error(barch::wrong_type_message());
            }
        }
        barch::sharded_store store(cc.kspace());
        // read only: a shared lock is enough, as DONE 19 did for SIZE
        auto t = store.read_locked(args[1]);
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_ll(0);

        }
        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        int64_t end = conversion::dec_bytes_to_int(header.end);

        return cc.push_ll(end - start);

    }
    /**
     * LRANGE key start stop - the elements between two positions, both ends inclusive.
     *
     * Positions are zero based and a negative counts back from the end, so 0 -1 is the
     * whole list. Out of range positions are clamped rather than refused and a start past
     * the end answers an empty array, which is what redis does and what ZRANGE was taught
     * to do in DONE 38.
     *
     * The elements are stored at consecutive indices between the header's start and end,
     * so this is a walk of exactly the span asked for rather than of the list.
     */
    int LRANGE(caller& cc, const arg_t& args) {
        if (args.size() != 4) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        long long from = 0, to = 0;
        if (!conversion::to_ll(args[2], from) || !conversion::to_ll(args[3], to)) {
            return cc.push_error("value is not an integer or out of range");
        }
        barch::sharded_store store(cc.kspace());
        if (barch::kind_of(store, args[1]) == barch::key_kind::string) {
            return cc.push_error(barch::wrong_type_message());
        }
        auto t = store.read_locked(args[1]);
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        auto value = t->search(key);
        if (value.null()) {
            cc.start_array();
            cc.end_array();
            return cc.ok();
        }
        list_header header {value.const_leaf()->get_value()};
        int64_t first = conversion::dec_bytes_to_int(header.start);
        int64_t last = conversion::dec_bytes_to_int(header.end);
        int64_t len = last - first;

        if (from < 0) from += len;
        if (to < 0) to += len;
        if (from < 0) from = 0;
        if (to >= len) to = len - 1;

        cc.start_array();
        for (int64_t i = from; i <= to && i < len; ++i) {
            composite li;
            li.create(art::ts_list, {container, conversion::comparable_key(first + i)});
            auto e = t->search(li.create());
            if (e.null()) continue;
            cc.push_vt(e.const_leaf()->get_value());
        }
        cc.end_array();
        return cc.ok();
    }

    int LBACK(caller& cc, const arg_t& args) {
        if (args.size() < 2) {
            return cc.wrong_arity();
        }
        if (key_ok(args[1]) != 0) {
            return cc.push_null();
        }
        barch::sharded_store store(cc.kspace());
        // read only: a shared lock is enough, as DONE 19 did for SIZE
        auto t = store.read_locked(args[1]);
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();

        }
        list_header header {value.const_leaf()->get_value()};
        int64_t end = conversion::dec_bytes_to_int(header.end);
        composite li;
        li.create(art::ts_list, {container,conversion::comparable_key(--end)});
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
        barch::sharded_store store(cc.kspace());
        // read only: a shared lock is enough, as DONE 19 did for SIZE
        auto t = store.read_locked(args[1]);
        auto container = conversion::convert(args[1]);
        auto key = query.create(art::ts_list, {container});
        auto value = t->search(key);
        if (value.null()) {
            return cc.push_null();
        }
        list_header header {value.const_leaf()->get_value()};
        int64_t start = conversion::dec_bytes_to_int(header.start);
        composite li;
        li.create(art::ts_list, {container,conversion::comparable_key(start)});
        auto front = t->search(li.create());
        if (front.null()) {
            return cc.push_null();
        }
        return cc.push_vt(front.const_leaf()->get_value());
    }
}

/* the list commands as a RESP client sees them */
void register_list_api(function_map& r) {
    r["LBACK"] = {::LBACK,{"read","list","data"}};
    r["LFRONT"] = {::LFRONT,{"read","list","data"}};
    r["LPUSH"] = {::LPUSH,{"write","list","data"}};
    r["LPUSHX"] = {::LPUSHX,{"write","list","data"}};
    r["RPUSHX"] = {::RPUSHX,{"write","list","data"}};
    r["RPUSH"] = {::RPUSH,{"write","list","data"}};
    r["RPOP"] = {::RPOP,{"write","list","data"}};
    r["LPOP"] = {::LPOP,{"write","list","data"}};
    r["BLPOP"] = {::BLPOP,{"write","list","data"}};
    r["BRPOP"] = {::BRPOP,{"write","list","data"}};
    r["LLEN"] = {::LLEN,{"read","list","data"}};
    r["LRANGE"] = {::LRANGE,{"read","list","data"}};
}
