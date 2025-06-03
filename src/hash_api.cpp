//
// Created by teejip on 4/9/25.
//

#include "hash_api.h"
#include "value_type.h"
#include "valkeymodule.h"
#include "art.h"
#include "caller.h"
#include "composite.h"
#include "module.h"
#include "keys.h"
#include "vk_caller.h"

int api_hset(caller& cc, const arg_t& args) {
    int responses = 0;
    int r = 0;
    int64_t updated = 0;

    auto fc = [&](art::node_ptr) -> void {
        ++updated;
    };
    if (key_ok(args[1]) != 0) {
        return cc.null();
    }
    auto t = get_art(args[1]);
    storage_release release(t->latch);
    auto container = conversion::convert(args[1]);
    t->query.create({container});
    for (size_t n = 2; n < args.size(); n += 2) {

        if (key_ok(args[n]) != 0) {
            r |= cc.null();
            ++responses;
            continue;
        }

        auto field = conversion::convert(args[n]);
        t->query.push(field);
        art::value_type key = t->query.create();
        art::value_type val = args[n+1];

        t->insert(key, val, true, fc);
        t->query.pop_back();
        ++responses;
    }
    return cc.boolean(updated);
}
int cmd_HSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, api_hset);
}

int cmd_HMSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HSET(ctx, argv, argc);
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
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);

    t->query.create({conversion::convert(n)});
    if (replies)
        call.start_array();
    for (size_t n = fields_start; n < argv.size(); ++n) {

        auto k = argv[n];

        if (key_ok(k) != 0) {
            if (replies)
                r |= call.null();
            ++responses;
            continue;
        }


        auto updater = [&](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) {
                if (replies)
                    r |= call.long_long(-2);
            } else {
                return modify(leaf);
            }
            return nullptr;
        };
        auto converted = conversion::convert(k);
        t->query.push(converted);
        art::value_type key = t->query.create();
        art::update(t, key, updater);
        t->query.pop_back();
        ++responses;
    }
    if (replies)
        call.end_array(responses);
    return call.ok();
}


int HUPDATE(caller& call,const arg_t& argv, int fields_start,
            const std::function<art::node_ptr(const art::node_ptr &old)> &modify) {
    return HUPDATEEX(call, argv, fields_start, true, modify);
}

int HEXPIRE(caller& call, const arg_t& argv, const std::function<int64_t(int64_t)> &calc) {
    if (argv.size() < 4)
        return call.wrong_arity();
    art::hexpire_spec ex_spec(argv);
    if (ex_spec.parse_options() != VALKEYMODULE_OK) {
        return call.syntax_error();
    }
    int r = 0;
    auto updater = [&](const art::node_ptr &leaf) -> art::node_ptr {
        auto l = leaf.const_leaf();
        auto ttl = calc(ex_spec.seconds);
        bool do_set = false;
        if (ex_spec.NX) {
            do_set = !l->is_ttl();
        }
        if (ex_spec.XX) {
            do_set = l->is_ttl();
        }
        if (ex_spec.GT) {
            do_set = (l->ttl() > 0 && l->ttl() < ttl);
        }
        if (ex_spec.LT) {
            do_set = (l->ttl() > 0 && l->ttl() >= ttl);
        }
        if (do_set) {
            r |= call.long_long(1);
            return art::make_leaf(*get_art(argv[1]), l->get_key(), l->get_value(), ttl, l->is_volatile());
        } else {
            r |= call.long_long( 0);
        }
        return nullptr;
    };
    return r | HUPDATE(call, argv, ex_spec.fields_start, updater);
}

int cmd_HEXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;

    return call.vk_call(ctx, argv,argc, [](caller& call, const arg_t& args) {
        return HEXPIRE(call, args, [](int64_t nr) -> int64_t {
            return art::now() + 1000 * nr;
        });
    });

}

int HEXPIREAT(caller& call, const arg_t& args) {
    return HEXPIRE(call, args, [](int64_t nr) -> int64_t {
        return 1000 * nr;
    });
}

int cmd_HEXPIREAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEXPIREAT);

}

int HGETEX(caller& call, const arg_t &argv) {
    art::hgetex_spec spec(argv);
    int r = 0;
    if (spec.parse_options() != VALKEYMODULE_OK) {
        return call.syntax_error();
    }
    long responses = 0;
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

                          r |= call.vt(l->get_value());
                          ++responses;
                          if (do_set) {
                              return art::make_leaf(*get_art(argv[1]), l->get_key(), l->get_value(), ttl, l->is_volatile());
                          }
                          return nullptr;
                      });
    call.end_array(responses);
    return r;
}
int cmd_HGETEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx,argv,argc, HGETEX);
}

int HINCRBY(caller& call, const arg_t &argv) {
    if (argv.size() != 4)
        return call.wrong_arity();

    long long by = 0;

    if (!conversion::to_ll(argv[3], by)) {
        return call.wrong_arity();
    }
    long long l = 0;
    auto vmin = argv;
    vmin.pop_back();
    int r = HUPDATEEX(call, vmin, 2, false,
                      [&](const art::node_ptr &old) -> art::node_ptr {
                          return leaf_numeric_update(l, old, by);
                      });
    if (r == VALKEYMODULE_OK) {
        return call.long_long(l);
    }
    return call.null();
}

int cmd_HINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HINCRBY);
}

int HINCRBYFLOAT(caller& call, const arg_t &argv) {
    if (argv.size() != 4)
        return call.wrong_arity();
    double by = 0;
    if (!conversion::to_double(argv[3], by)) {
        return call.wrong_arity();
    }
    int r = 0;
    double l = 0;
    auto arg2 = argv;
    arg2.pop_back();
    r = HUPDATEEX(call, arg2, 2, false,
                  [&l,by](const art::node_ptr &old) -> art::node_ptr {
                      return leaf_numeric_update(l, old, by);
                  });
    if (r == VALKEYMODULE_OK) {
        return call.double_(l);
    }
    return call.null();
}

int cmd_HINCRBYFLOAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HINCRBYFLOAT);
}

int HDEL(caller& call, const arg_t &argv) {

    if (argv.size() < 4)
        return call.wrong_arity();

    int responses = 0;
    art::key_spec spec;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    t->query.create({conversion::convert(n)});
    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    for (size_t n = 2; n < argv.size(); ++n) {
        size_t klen = 0;
        auto k = argv[n];

        if (key_ok(k) != 0) {
            continue;
        }

        auto converted = conversion::convert(k, klen);
        t->query.push(converted);

        art::value_type key = t->query.create();
        get_art(argv[1])->remove(key, del_report);
        t->query.pop_back();
    }
    call.long_long(responses);
    return call.ok();
}
int cmd_HDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HDEL);
}

int HGETDEL(caller& call, const arg_t &argv) {

    if (argv.size() < 4)
        return call.wrong_arity();
    int responses = 0;
    art::key_spec spec;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);

    if (argv[2] != "FIELDS") {
        return call.wrong_arity();
    }
    t->query.create({conversion::convert(n)});
    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    for (size_t n = 3; n < argv.size(); ++n) {
        auto k = argv[n];

        if (key_ok(k) != 0) {
            continue;
        }

        auto converted = conversion::convert(k);
        t->query.push(converted);

        art::value_type key = t->query.create();
        get_art(argv[1])->remove(key, del_report);
        t->query.pop_back();
    }
    call.long_long(responses);
    return call.ok();
}

int cmd_HGETDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGETDEL);
}

int HGETEX(caller& call,const arg_t& argv,
           const std::function<void(art::node_ptr leaf)> &reporter, const std::function<void()> &nullreporter) {
    if (argv.size() < 3)
        return call.wrong_arity();
    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(n);
    storage_release release(t->latch);

    art::value_type any_key = t->query.create({conversion::convert(n)});
    art::node_ptr lb = lower_bound(t, any_key);
    if (lb.null()) {
        return call.null();
    }
    if (lb.is_leaf) {
        // Check if the expanded path matches
        if (lb.const_leaf()->prefix(any_key) != 0) {
            return call.null();
        }
    }
    call.start_array();
    for (size_t arg = 2; arg < argv.size(); ++arg) {
        auto k = argv[arg];
        if (key_ok(k) != 0) {
            call.null();
        } else {
            auto converted = conversion::convert(k);
            t->query.push(converted);
            art::value_type search_key = t->query.create();
            art::node_ptr r = art_search(t, search_key);
            if (r.null()) {
                nullreporter();
            } else {
                reporter(r);
            }
            t->query.pop_back();
            ++responses;
        }
    }
    call.end_array(responses);
    return call.ok();
}

int HGET_(caller& call, const arg_t& argv,
         const std::function<void(art::node_ptr leaf)> &reporter) {
    return HGETEX(call, argv, reporter, [&]()-> void {
        call.null();
    });
}

int HTTL(caller& call,const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        long long ttl = l->ttl();
        if (ttl == 0) {
            call.long_long(-1);
        } else {
            call.long_long((ttl - art::now()) / 1000);
        }
    };
    auto nullreport = [&]() -> void {
        call.long_long(-2);
    };
    int r = HGETEX(call, argv, reporter, nullreport);
    return r;
}
int cmd_HTTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HTTL);
}
int HGET(caller& call, const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto vt = r.const_leaf()->get_value();
        call.vt(vt);
    };
    return HGET_(call, argv, reporter);
}
int cmd_HGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGET);
}

int HLEN(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    int responses = 0;
    size_t nlen = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    t->query.create({conversion::convert(n, nlen), art::ts_end});
    auto search_end = t->query.end();
    auto search_start = t->query.prefix(2);
    auto table_key = t->query.prefix(2);
    auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
        if (!key.starts_with(table_key)) {
            return -1;
        }
        ++responses;

        return 0;
    };
    art::range(t, search_start, search_end, table_iter, nullptr);

    return call.long_long(responses);
}
int cmd_HLEN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HLEN);
}

int cmd_HMGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HGET(ctx, argv, argc);
}

int HEXPIRETIME(caller& call, const arg_t& argv) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        call.long_long(l->ttl() / 1000);
    };
    return HGET_(call, argv, reporter);
}

int cmd_HEXPIRETIME(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEXPIRETIME);
}

int HGETALL(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    t->query.create({conversion::convert(n), art::ts_end});

    art::value_type search_end = t->query.end();
    art::value_type search_start = t->query.prefix(2);
    art::value_type table_key = search_start;
    bool exists = false;
    auto table_counter = [&](art::node_ptr leaf)-> int {
        auto l = leaf.const_leaf();
        if (!l->get_key().starts_with(table_key)) {
            return -1;
        }
        exists = true;
        return -1;
    };
    art::range(t, search_start, search_end, table_counter);
    if (!exists) {
        return call.null();
    }
    call.start_array();
    auto table_iter = [&](const art::node_ptr &leaf)-> int {
        auto l = leaf.const_leaf();
        auto key = l->get_key();
        auto value = l->get_value();
        if (!key.starts_with(table_key)) {
            return -1;
        }
        call.reply_encoded_key(art::value_type{key.bytes + table_key.size, key.size - table_key.size});
        call.vt(value);
        responses += 2;

        return 0;
    };
    art::range(t, search_start, search_end, table_iter);

    call.end_array(responses);
    return call.ok();
}
int cmd_HGETALL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HGETALL);
}
int HKEYS(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();

    int responses = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    };
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::value_type search_end = t->query.create({conversion::convert(n), art::ts_end});
    art::value_type search_start = t->query.prefix(2);
    art::value_type table_key = search_start;
    call.start_array();
    auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
        if (!key.starts_with(search_start)) {
            return -1;
        }
        call.reply_encoded_key(art::value_type{key.bytes + table_key.size, key.size - table_key.size});
        responses += 1;

        return 0;
    };
    art::range(t, search_start, search_end, table_iter, nullptr);

    call.end_array(responses);
    return call.ok();
}
int cmd_HKEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HKEYS);
}
int HEXISTS(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    int responses = 0;
    size_t nlen = 0;
    auto n = argv[1];
    if (key_ok(n) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);

    art::value_type search_end = t->query.create({conversion::convert(n, nlen), art::ts_end});
    art::value_type search_start = t->query.prefix(2);
    art::value_type table_key = t->query.prefix(2);
    auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
        if (!key.starts_with(table_key)) {
            return -1;
        }
        ++responses;
        return -1;
    };
    art::range(t, search_start, search_end, table_iter, nullptr);

    return call.boolean(responses > 0 ? 1 : 0);
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
