//
// Created by teejip on 4/9/25.
//

#include "hash_api.h"
#include "value_type.h"
#include "../external/include/valkeymodule.h"
#include "art/art.h"
#include "caller.h"
#include "composite.h"
#include "module.h"
#include "keys.h"
#include "vk_caller.h"
#include "sharded_store.h"
#include "key_type.h"
#include "art/iterator.h"
static thread_local composite query;
extern "C"{
int HSET(caller& cc, const arg_t& args) {
    // `added` counts fields that were not there before, which is what redis replies
    // with. `updated` counts the ones that were, and is only kept because insert wants
    // a callback
    int64_t added = 0;
    int64_t updated = 0;

    auto fc = [&](const art::node_ptr&) -> void {
        ++updated;
    };
    if (args.size() < 4 || (args.size() % 2) != 0) {
        return cc.wrong_arity();
    }
    if (key_ok(args[1]) != 0) {
        return cc.push_null();
    }
    barch::sharded_store store(cc.kspace());
    // a name already holding a plain value is not a hash to add fields to. Checked before
    // the lock, because kind_of routes to shards of its own - see key_type.h
    if (barch::kind_of(store, args[1]) == barch::key_kind::string) {
        return cc.push_error(barch::wrong_type_message());
    }
    store.with_container_write(args[1], [&](const barch::shard_ptr& t) {
        auto container = conversion::convert(args[1]);

        query.create({container});
        for (size_t n = 2; n < args.size(); n += 2) {

            if (key_ok(args[n]) != 0) {
                continue;
            }

            auto field = conversion::convert(args[n]);
            query.push(field);
            art::value_type key = query.create();
            art::value_type val = args[n+1];

            if (t->insert(key, val, true, fc)) {
                ++added;
            }

            query.pop_back();
        }
    });
    return cc.push_ll(added);
}
}
int cmd_HSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HSET);
}

int cmd_HMSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HSET(ctx, argv, argc);
}


/**
 * Add a number to one hash field, creating the field when it is not there.
 *
 * HUPDATEEX cannot do this on its own: its updater is only reached through
 * shard::update, which does nothing when the key is absent, so a missing field was
 * left alone and the reply was the zero the accumulator started at. This mirrors what
 * BarchModifyInteger does for plain keys - the miss and the insert that follows it run
 * under one container write lock, so two callers cannot both decide the field is
 * missing and both create it.
 */
template<typename NumT>
static int HNUMERIC(caller& call, const arg_t& argv, NumT by, bool as_double) {
    if (argv.size() != 4)
        return call.wrong_arity();
    auto n = argv[1];
    auto f = argv[2];
    if (key_ok(n) != 0 || key_ok(f) != 0) {
        return call.push_null();
    }
    NumT l = NumT();
    bool ok = false;
    // as in keys_api: a field that is present but not numeric must not be taken for an
    // absent one and overwritten by the increment
    bool present = false;
    numeric_status why = numeric_status::updated;
    barch::sharded_store store(call.kspace());
    store.with_container_write(n, [&](const barch::shard_ptr& t) {
        query.create({conversion::convert(n)});
        query.push(conversion::convert(f));
        art::value_type key = query.create();
        auto updater = [&](const art::node_ptr &old) -> art::node_ptr {
            if (old.null()) {
                return nullptr;
            }
            present = true;
            auto v = leaf_numeric_update(l, old, by, why);
            if (!v.null()) ok = true;
            return v;
        };
        if (!t->update(key, updater)) {
            if (!present) {
                // field absent: start it at the increment, as redis does
                l = by;
                std::string held = numeric_to_text(l);
                auto fc = [&](const art::node_ptr &) -> void {};
                t->insert(key, art::value_type{held}, true, fc);
                ok = true;
            }
        }
        query.pop_back();
    });
    if (!ok) {
        if (present && why == numeric_status::overflowed) {
            return call.push_error("increment or decrement would overflow");
        }
        return call.push_error("hash value is not an integer");
    }
    if (as_double) {
        return call.push_double((double) l);
    }
    return call.push_ll((int64_t) l);
}

int HUPDATEEX(caller& call, const arg_t&argv, int fields_start,
              bool replies,
              const std::function<art::node_ptr(const art::node_ptr &old)> &modify) {
    if (argv.size() < 3)
        return call.wrong_arity();
    int responses = 0;
    int r = 0;
    art::key_spec spec(argv);

    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    }
    barch::sharded_store store(call.kspace());
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        query.create({conversion::convert(n)});
        if (replies)
            call.start_array();
        for (size_t n = fields_start; n < argv.size(); ++n) {

            auto k = argv[n];

            if (key_ok(k) != 0) {
                if (replies)
                    r |= call.push_null();
                ++responses;
                continue;
            }


            auto updater = [&](const art::node_ptr &leaf) -> art::node_ptr {
                if (leaf.null()) {
                    if (replies)
                        r |= call.push_ll(-2);
                } else {
                    return modify(leaf);
                }
                return nullptr;
            };
            auto converted = conversion::convert(k);
            query.push(converted);
            art::value_type key = query.create();
            t->update(key, updater);
            query.pop_back();
            ++responses;
        }
        if (replies)
            call.end_array();
    });
    return call.ok();
}


int HUPDATE(caller& call,const arg_t& argv, int fields_start,
            const std::function<art::node_ptr(const art::node_ptr &old)> &modify) {
    return HUPDATEEX(call, argv, fields_start, true, modify);
}

int INNER_HEXPIRE(caller& call, const arg_t& argv, const std::function<int64_t(int64_t)> &calc) {
    if (argv.size() < 4)
        return call.wrong_arity();
    art::hexpire_spec ex_spec(argv);
    if (ex_spec.parse_options() != VALKEYMODULE_OK) {
        return call.syntax_error();
    }
    // HUPDATE below takes the container lock; this only needs the allocator of the
    // shard the container lives on, to build the replacement leaf
    barch::sharded_store store(call.kspace());
    auto t = store.shard_for(argv[1]);
    int r = 0;
    auto updater = [&](const art::node_ptr &leaf) -> art::node_ptr {
        auto l = leaf.const_leaf();
        auto ttl = calc(ex_spec.seconds);
        bool do_set = false;
        if (ex_spec.NX) {
            do_set = !l->is_expiry();
        }
        if (ex_spec.XX) {
            do_set = l->is_expiry();
        }
        if (ex_spec.GT) {
            do_set = (l->expiry_ms() > 0 && l->expiry_ms() < ttl);
        }
        if (ex_spec.LT) {
            do_set = (l->expiry_ms() > 0 && l->expiry_ms() >= ttl);
        }
        if (do_set) {
            r |= call.push_ll(1);
            return art::make_leaf(t->get_ap(), l->get_key(), l->get_value(), ttl, l->is_volatile(), l->is_compressed());
        } else {
            r |= call.push_ll( 0);
        }
        return nullptr;
    };
    return r | HUPDATE(call, argv, ex_spec.fields_start, updater);
}

extern "C"
int HEXPIRE(caller& call, const arg_t& args) {
    return INNER_HEXPIRE(call, args, [](int64_t nr) -> int64_t {
            return art::now() + 1000 * nr;
        });
}

int cmd_HEXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;

    return call.vk_call(ctx, argv,argc, [](caller& call, const arg_t& args) {
        return INNER_HEXPIRE(call, args, [](int64_t nr) -> int64_t {
            return art::now() + 1000 * nr;
        });
    });

}
extern "C"
int HEXPIREAT(caller& call, const arg_t& args) {
    return INNER_HEXPIRE(call, args, [](int64_t nr) -> int64_t {
        return 1000 * nr;
    });
}

int cmd_HEXPIREAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEXPIREAT);

}
extern "C"
int HGETEX(caller& call, const arg_t &argv) {
    art::hgetex_spec spec(argv);
    int r = 0;
    if (spec.parse_options() != VALKEYMODULE_OK) {
        return call.syntax_error();
    }
    long responses = 0;
    // HUPDATEEX takes the container lock; this only needs the allocator of the shard
    // the container lives on, to build the replacement leaf
    barch::sharded_store store(call.kspace());
    call.start_array();
    r = r | HUPDATEEX(call, argv, spec.fields_start, false,
                      [&](const art::node_ptr &leaf) -> art::node_ptr {
                          auto l = leaf.const_leaf();
                          int64_t ttl = 0;
                          bool do_set = false;

                          if (spec.EX) {
                              do_set = true;
                              ttl = art::now() + spec.time_val * 1000;
                          }
                          if (spec.PX) {
                              do_set = true;
                              ttl = art::now() + spec.time_val;
                          }
                          if (spec.EXAT) {
                              do_set = true;
                              ttl = 1000 * spec.time_val;
                          }
                          if (spec.PXAT) {
                              do_set = true;
                              ttl = spec.time_val;
                          }
                          if (spec.PERSIST) {
                              do_set = true;
                          }

                          r |= call.push_vt(l->get_value());
                          ++responses;
                          if (do_set) {
                              return art::make_leaf(store.shard_for(argv[1])->get_ap(), l->get_key(), l->get_value(), ttl, l->is_volatile());
                          }
                          return nullptr;
                      });
    call.end_array();
    return r;
}
int cmd_HGETEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx,argv,argc, HGETEX);
}
extern "C"
int HINCRBY(caller& call, const arg_t &argv) {
    long long by = 0;
    if (argv.size() != 4)
        return call.wrong_arity();
    if (!conversion::to_ll(argv[3], by)) {
        return call.push_error("value is not an integer or out of range");
    }
    return HNUMERIC<long long>(call, argv, by, false);
}

int cmd_HINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HINCRBY);
}
extern "C"
int HINCRBYFLOAT(caller& call, const arg_t &argv) {
    double by = 0;
    if (argv.size() != 4)
        return call.wrong_arity();
    if (!conversion::to_double(argv[3], by)) {
        return call.push_error("value is not a valid float");
    }
    return HNUMERIC<double>(call, argv, by, true);
}

int cmd_HINCRBYFLOAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HINCRBYFLOAT);
}
extern "C"
int HDEL(caller& call, const arg_t &argv) {

    if (argv.size() < 3)
        return call.wrong_arity();

    int responses = 0;
    art::key_spec spec;
    auto k = argv[1];
    if (key_ok(k) != 0) {
        return call.push_null();
    }

    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    // this used to re-route per member and take no lock at all, so the removes ran
    // unsynchronised against readers. one route, one write lock, as HSET already did
    barch::sharded_store store(call.kspace());
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        query.create({conversion::convert(k)});
        for (size_t n = 2; n < argv.size(); ++n) {
            size_t klen = 0;
            auto k = argv[n];

            if (key_ok(k) != 0) {
                continue;
            }

            auto converted = conversion::convert(k, klen);
            query.push(converted);

            art::value_type key = query.create();
            t->remove(key, del_report);
            query.pop_back();
        }
    });
    call.push_ll(responses);
    return call.ok();
}
int cmd_HDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HDEL);
}
extern "C"
int HGETDEL(caller& call, const arg_t &argv) {

    if (argv.size() < 4)
        return call.wrong_arity();
    int responses = 0;
    art::key_spec spec;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    }
    if (argv[2] != "FIELDS") {
        return call.wrong_arity();
    }
    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    // as HDEL: was re-routing per member with no lock held over the removes
    barch::sharded_store store(call.kspace());
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        query.create({conversion::convert(n)});
        for (size_t n = 3; n < argv.size(); ++n) {
            auto k = argv[n];

            if (key_ok(k) != 0) {
                continue;
            }

            auto converted = conversion::convert(k);
            query.push(converted);

            art::value_type key = query.create();
            t->remove(key, del_report);
            query.pop_back();
        }
    });
    call.push_ll(responses);
    return call.ok();
}

int cmd_HGETDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGETDEL);
}

/**
 * Shared field lookup for the H* readers.
 *
 * `as_array` is what separates the multi field readers from the single field ones.
 * HMGET and HTTL answer for a list of fields and so wrap their replies in an array;
 * HGET and HEXISTS answer for exactly one thing and must not, because a redis client
 * reading HGET expects a bulk string and HEXISTS expects an integer. Wrapping those
 * was what produced the one element array HGET used to return.
 */
int HQUERY(caller& call,const arg_t& argv, bool fancy,
           const std::function<void(art::node_ptr leaf)> &reporter, const std::function<void()> &nullreporter,
           bool as_array) {

    if (argv.size() < 3)
        return call.wrong_arity();
    int fields_start = 2;
    if (fancy) {
        art::hgetex_spec spec(argv);

        if (spec.parse_options() != VALKEYMODULE_OK) {
            return call.syntax_error();
        }

        if (spec.EX||spec.PX||spec.EXAT||spec.PXAT||spec.PERSIST) {
            return call.syntax_error();
        }
        fields_start = spec.fields_start;
    }

    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    }
    barch::sharded_store store(call.kspace());
    bool missing = false;
    store.with_container_write(n, [&](const barch::shard_ptr& t) {
    art::value_type any_key = query.create({conversion::convert(n)});
    art::node_ptr lb = t->lower_bound(any_key);
    if (lb.null()) {
        missing = true;
        return;
    }
    if (lb.is_leaf) {
        // Check if the expanded path matches
        if (lb.const_leaf()->prefix(any_key) != 0) {
            missing = true;
            return;
        }
    }
    if (as_array) call.start_array();
    for (size_t arg = fields_start; arg < argv.size(); ++arg) {
        auto k = argv[arg];
        if (key_ok(k) != 0) {
            call.push_null();
        } else {
            auto converted = conversion::convert(k);
            query.push(converted);
            art::value_type search_key = query.create();
            art::node_ptr r = t->search(search_key);
            if (r.null()) {
                nullreporter();
            } else {
                reporter(r);
            }
            query.pop_back();
            ++responses;
        }
    }
    if (as_array) call.end_array();
    });
    return missing ? call.push_null() : call.ok();
}

int HGET_(caller& call, const arg_t& argv,
         const std::function<void(art::node_ptr leaf)> &reporter, bool as_array) {
    return HQUERY(call, argv, false, reporter, [&]()-> void {
        call.push_null();
    }, as_array);
}
extern "C"
int HTTL(caller& call,const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        long long ttl = l->expiry_ms();
        if (ttl == 0) {
            call.push_ll(-1);
        } else {
            call.push_ll((ttl - art::now()) / 1000);
        }
    };
    auto nullreport = [&]() -> void {
        call.push_ll(-2);
    };
    int r = HQUERY(call, argv, true, reporter, nullreport, true);
    return r;
}
int cmd_HTTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HTTL);
}
extern "C"
int HGET(caller& call, const arg_t& argv) {
    // one key and one field, and the value comes back as a bulk string. It used to be
    // wrapped in a one element array because it shared HMGET's reply path
    if (argv.size() != 3)
        return call.wrong_arity();
    auto reporter = [&](art::node_ptr r) -> void {
        auto vt = r.const_leaf()->get_value();
        call.push_vt(vt);
    };
    return HGET_(call, argv, reporter, false);
}

extern "C"
int HMGET(caller& call, const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto vt = r.const_leaf()->get_value();
        call.push_vt(vt);
    };
    return HGET_(call, argv, reporter, true);
}

int cmd_HGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGET);
}
extern "C"
int HLEN(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    int responses = 0;
    size_t nlen = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    }

    barch::sharded_store store(call.kspace());
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        query.create({conversion::convert(n, nlen), art::ts_end});
        auto search_end = query.end();
        auto search_start = query.prefix(2);
        auto table_key = query.prefix(2);
        auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
            if (!key.starts_with(table_key.pref(1))) {
                return -1;
            }
            ++responses;

            return 0;
        };
        t->range(search_start, search_end, table_iter, nullptr);
    });
    return call.push_ll(responses);
}
int cmd_HLEN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HLEN);
}

int cmd_HMGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HGET(ctx, argv, argc);
}
extern "C"
int HEXPIRETIME(caller& call, const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        call.push_ll(l->expiry_ms() / 1000);
    };
    // a FIELDS form reader, so it keeps the array
    return HQUERY(call, argv, true, reporter, [&]()-> void {call.push_null();}, true);
}

int cmd_HEXPIRETIME(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEXPIRETIME);
}
extern "C"
int HGETALL(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    }
    barch::sharded_store store(call.kspace());
    bool missing = false;
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        art::value_type search_start = query.create({conversion::convert(n)},false);

        art::value_type table_key = search_start;
        //bool exists = false;
        art::iterator ai(t, search_start);
        if (!ai.ok()) {
            missing = true;
            return;
        }
        call.start_array();

        while (ai.ok()) {
            auto ik = ai.key();
            if (!ik.starts_with(table_key)) break;
            auto key = ai.key();
            auto value = ai.value();
            if (!key.starts_with(table_key)) {
                break;
            }
            call.push_encoded_key(art::value_type{key.bytes + table_key.size, key.size - table_key.size});
            call.push_vt(value);
            responses += 2;
            ai.next();
        }
        call.end_array();
    });
    return missing ? call.push_null() : call.ok();
}
int cmd_HGETALL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGETALL);
}
extern "C"
int HKEYS(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();

    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.push_null();
    };
    barch::sharded_store store(call.kspace());
    store.with_container_write(argv[1], [&](const barch::shard_ptr& t) {
        art::value_type search_end = query.create({conversion::convert(n), art::ts_end});
        art::value_type search_start = query.prefix(2);
        art::value_type table_key = search_start;
        call.start_array();
        auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
            if (!key.starts_with(search_start.pref(1))) {
                return -1;
            }
            call.push_encoded_key(art::value_type{key.bytes + table_key.size, key.size - table_key.size});
            responses += 1;

            return 0;
        };
        t->range(search_start, search_end, table_iter, nullptr);

        call.end_array();
    });
    return call.ok();
}
int cmd_HKEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HKEYS);
}
extern "C"
int HEXISTS(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    int cnt = 0;
    auto reporter = [&](art::node_ptr unused(r)) -> void {
        ++cnt;
    };
    // no array: the reply is the flag on its own, as a redis client expects
    int r = HQUERY(call, argv, false, reporter, [&]()-> void {

    }, false);
    if (r == call.ok()) {
        return call.push_bool(cnt>0);
    }
    return call.push_null();
}

int cmd_HEXISTS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEXISTS);
}

int add_hash_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(HSET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HGETDEL), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HGETEX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HMSET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HEXPIRE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HDEL), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HINCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HINCRBYFLOAT), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HGET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HTTL), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HLEN), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HEXPIRETIME), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HMGET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HGETALL), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HKEYS), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HEXISTS), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

/* the hash commands as a RESP client sees them.
 *
 * HGETEX and HQUERY are deliberately not registered, not an oversight. Both are
 * implemented and reachable from the valkey module side, but neither has been settled
 * over RESP - HGETEX in particular shares HUPDATEEX's option parsing, which is not a
 * barch_function at all but a helper taking extra arguments, so it cannot go in this
 * table as it stands. */
void register_hash_api(function_map& r) {
    r["HSET"] = {::HSET,{"write","hash","data"}};
    r["HEXPIREAT"] = {::HEXPIREAT,{"write","hash","data"}};
    r["HEXPIRE"] = {::HEXPIRE,{"write","hash","data"}};
    //r["HGETEX"] = ::HGETEX;
    r["HMGET"] = {::HMGET,{"read","hash","data"}};
    r["HINCRBY"] = {::HINCRBY,{"write","hash","data"}};
    r["HINCRBYFLOAT"] = {::HINCRBYFLOAT,{"write","hash","data"}};
    r["HDEL"] = {::HDEL,{"write","hash","data"}};
    r["HGETDEL"] = {::HGETDEL,{"write","hash","data"}};
    r["HTTL"] = {::HTTL,{"read","hash","data"}};
    r["HGET"] = {::HGET,{"read","hash","data"}};
    r["HLEN"] = {::HLEN,{"read","hash","data"}};
    r["HEXPIRETIME"] = {::HEXPIRETIME,{"read","hash","data"}};
    r["HGETALL"] = {::HGETALL,{"read","hash","data"}};
    r["HKEYS"] = {::HKEYS,{"read","hash","data"}};
    r["HEXISTS"] = {::HEXISTS,{"read","hash","data"}};
}
