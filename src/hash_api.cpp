//
// Created by teejip on 4/9/25.
//

#include "hash_api.h"
#include "value_type.h"
#include "valkeymodule.h"
#include "art.h"
#include "composite.h"
#include "module.h"
#include "keys.h"
int cmd_HSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc < 4)
        return ValkeyModule_WrongArity(ctx);

    int responses = 0;
    int r = VALKEYMODULE_OK;
    size_t nlen;
    //thread_local composite query;
    //art::key_spec spec;
    int64_t updated = 0;

    auto fc = [&](art::node_ptr) -> void {
        ++updated;
    };
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);
    art::value_type shard_key{n, nlen};
    auto container = conversion::convert(n, nlen);
    t->query.create({container});
    for (int n = 2; n < argc; n += 2) {
        size_t klen, vlen;
        const char *k = ValkeyModule_StringPtrLen(argv[n], &klen);
        const char *v = ValkeyModule_StringPtrLen(argv[n + 1], &vlen);

        if (key_ok(k, klen) != 0) {
            r |= ValkeyModule_ReplyWithNull(ctx);
            ++responses;
            continue;
        }

        auto field = conversion::convert(k, klen);
        t->query.push(field);
        art::value_type key = t->query.create();
        art::value_type val = {v, (unsigned) vlen};

        art_insert(t, {}, key, val, fc);
        t->query.pop_back();
        ++responses;
    }
    ValkeyModule_ReplyWithBool(ctx, updated > 0 ? 0 : 1);
    return 0;
}

int cmd_HMSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HSET(ctx, argv, argc);
}

int HUPDATEEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc, int fields_start,
              bool replies,
              const std::function<art::node_ptr(const art::node_ptr &old)> &modify) {
    ValkeyModule_AutoMemory(ctx);
    if (argc < 3)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    int r = VALKEYMODULE_OK;
    size_t nlen;
    art::key_spec spec;

    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);

    t->query.create({conversion::convert(n, nlen)});
    if (replies)
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    for (int n = fields_start; n < argc; ++n) {
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[n], &klen);

        if (key_ok(k, klen) != 0) {
            if (replies)
                r |= ValkeyModule_ReplyWithNull(ctx);
            ++responses;
            continue;
        }


        auto updater = [&](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) {
                if (replies)
                    r |= ValkeyModule_ReplyWithLongLong(ctx, -2);
            } else {
                return modify(leaf);
            }
            return nullptr;
        };
        auto converted = conversion::convert(k, klen);
        t->query.push(converted);
        art::value_type key = t->query.create();
        art::update(t, key, updater);
        t->query.pop_back();
        ++responses;
    }
    if (replies)
        ValkeyModule_ReplySetArrayLength(ctx, responses);
    return 0;
}

int HUPDATE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc, int fields_start,
            const std::function<art::node_ptr(const art::node_ptr &old)> &modify) {
    return HUPDATEEX(ctx, argv, argc, fields_start, true, modify);
}

int HEXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc, const std::function<int64_t(int64_t)> &calc) {
    if (argc < 4)
        return ValkeyModule_WrongArity(ctx);
    art::hexpire_spec ex_spec(argv, argc);
    if (ex_spec.parse_options() != VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithSimpleString(ctx, "ERR");
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
            r |= ValkeyModule_ReplyWithLongLong(ctx, 1);
            return art::make_leaf(*get_art(argv), l->get_key(), l->get_value(), ttl, l->is_volatile());
        } else {
            r |= ValkeyModule_ReplyWithLongLong(ctx, 0);
        }
        return nullptr;
    };
    return r | HUPDATE(ctx, argv, argc, ex_spec.fields_start, updater);
}

int cmd_HEXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return HEXPIRE(ctx, argv, argc, [](int64_t nr) -> int64_t {
        return art::now() + 1000 * nr;
    });
}

int cmd_HEXPIREAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return HEXPIRE(ctx, argv, argc, [](int64_t nr) -> int64_t {
        return 1000 * nr;
    });
}

int cmd_HGETEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    art::hgetex_spec spec(argv, argc);
    int r = 0;
    if (spec.parse_options() != VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithSimpleString(ctx, "ERR");
    }
    long responses = 0;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    r = r | HUPDATEEX(ctx, argv, argc, spec.fields_start, false,
                      [argv,&spec,&r,ctx,&responses](const art::node_ptr &leaf) -> art::node_ptr {
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

                          r |= ValkeyModule_ReplyWithStringBuffer(ctx, l->get_value().chars(), l->get_value().size);
                          ++responses;
                          if (do_set) {
                              return art::make_leaf(*get_art(argv), l->get_key(), l->get_value(), ttl, l->is_volatile());
                          }
                          return nullptr;
                      });
    ValkeyModule_ReplySetArrayLength(ctx, responses);
    return r;
}

int cmd_HINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    long long by = 0;

    if (ValkeyModule_StringToLongLong(argv[3], &by) != VALKEYMODULE_OK) {
        return ValkeyModule_WrongArity(ctx);
    }
    long long l = 0;
    int r = HUPDATEEX(ctx, argv, argc - 1, 2, false,
                      [by,&l](const art::node_ptr &old) -> art::node_ptr {
                          return leaf_numeric_update(l, old, by);
                      });
    if (r == VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithLongLong(ctx, l);
    }
    return ValkeyModule_ReplyWithNull(ctx);
}

int cmd_HINCRBYFLOAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    double by = 0;
    if (ValkeyModule_StringToDouble(argv[3], &by) != VALKEYMODULE_OK) {
        return ValkeyModule_WrongArity(ctx);
    }
    int r = 0;
    double l = 0;
    r = HUPDATEEX(ctx, argv, argc - 1, 2, false,
                  [&l,by](const art::node_ptr &old) -> art::node_ptr {
                      return leaf_numeric_update(l, old, by);
                  });
    if (r == VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithDouble(ctx, l);
    }
    return ValkeyModule_ReplyWithNull(ctx);
}


int cmd_HDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    if (argc < 4)
        return ValkeyModule_WrongArity(ctx);

    int responses = 0;
    size_t nlen;
    art::key_spec spec;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);
    t->query.create({conversion::convert(n, nlen)});
    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    for (int n = 2; n < argc; ++n) {
        size_t klen = 0;
        const char *k = ValkeyModule_StringPtrLen(argv[n], &klen);

        if (key_ok(k, klen) != 0) {
            continue;
        }

        auto converted = conversion::convert(k, klen);
        t->query.push(converted);

        art::value_type key = t->query.create();
        art_delete(get_art(argv), key, del_report);
        t->query.pop_back();
    }
    ValkeyModule_ReplyWithLongLong(ctx, responses);
    return 0;
}

int cmd_HGETDEL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    if (argc < 4)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    size_t nlen;
    art::key_spec spec;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);

    if (ValkeyModule_StringCompare(argv[2], Constants.FIELDS) != 0) {
        return ValkeyModule_WrongArity(ctx);
    }
    t->query.create({conversion::convert(n, nlen)});
    auto del_report = [&](art::node_ptr) -> void {
        ++responses;
    };
    for (int n = 3; n < argc; ++n) {
        size_t klen = 0;
        const char *k = ValkeyModule_StringPtrLen(argv[n], &klen);

        if (key_ok(k, klen) != 0) {
            continue;
        }

        auto converted = conversion::convert(k, klen);
        t->query.push(converted);

        art::value_type key = t->query.create();
        art_delete(get_art(argv), key, del_report);
        t->query.pop_back();
    }
    ValkeyModule_ReplyWithLongLong(ctx, responses);
    return 0;
}

int HGETEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
           const std::function<void(art::node_ptr leaf)> &reporter, const std::function<void()> &nullreporter) {
    ValkeyModule_AutoMemory(ctx);
    if (argc < 3)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);

    art::value_type any_key = t->query.create({conversion::convert(n, nlen)});
    art::node_ptr lb = lower_bound(get_art(argv), any_key);
    if (lb.null()) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    if (lb.is_leaf) {
        // Check if the expanded path matches
        if (lb.const_leaf()->prefix(any_key) != 0) {
            return ValkeyModule_ReplyWithNull(ctx);
        }
    }
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    for (int arg = 2; arg < argc; ++arg) {
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[arg], &klen);
        if (key_ok(k, klen) != 0) {
            ValkeyModule_ReplyWithNull(ctx);
        } else {
            auto converted = conversion::convert(k, klen);
            t->query.push(converted);
            art::value_type search_key = t->query.create();
            art::node_ptr r = art_search(get_art(argv), search_key);
            if (r.null()) {
                nullreporter();
            } else {
                reporter(r);
            }
            t->query.pop_back();
            ++responses;
        }
    }
    ValkeyModule_ReplySetArrayLength(ctx, responses);
    return VALKEYMODULE_OK;
}

int HGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
         const std::function<void(art::node_ptr leaf)> &reporter) {
    return HGETEX(ctx, argv, argc, reporter, [&]()-> void {
        ValkeyModule_ReplyWithNull(ctx);
    });
}

int cmd_HTTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        long long ttl = l->ttl();
        if (ttl == 0) {
            ValkeyModule_ReplyWithLongLong(ctx, -1);
        } else {
            ValkeyModule_ReplyWithLongLong(ctx, (ttl - art::now()) / 1000);
        }
    };
    auto nullreport = [&]() -> void {
        ValkeyModule_ReplyWithLongLong(ctx, -2);
    };
    int r = HGETEX(ctx, argv, argc, reporter, nullreport);
    return r;
}

int cmd_HGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto vt = r.const_leaf()->get_value();

        auto *val = ValkeyModule_CreateString(ctx, vt.chars(), vt.size);
        ValkeyModule_ReplyWithString(ctx, val);
    };
    return HGET(ctx, argv, argc, reporter);
}

int cmd_HLEN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
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
    art::range(get_art(argv), search_start, search_end, table_iter, nullptr);

    return ValkeyModule_ReplyWithLongLong(ctx, responses);
}

int cmd_HMGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    return cmd_HGET(ctx, argv, argc);
}

int cmd_HEXPIRETIME(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    auto reporter = [&](art::node_ptr r) -> void {
        auto l = r.const_leaf();
        ValkeyModule_ReplyWithLongLong(ctx, l->ttl() / 1000);
    };
    return HGET(ctx, argv, argc, reporter);
}

int cmd_HGETALL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
    storage_release release(t->latch);
    t->query.create({conversion::convert(n, nlen), art::ts_end});

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
    art::range(get_art(argv), search_start, search_end, table_counter);
    if (!exists) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    auto table_iter = [&](const art::node_ptr &leaf)-> int {
        auto l = leaf.const_leaf();
        auto key = l->get_key();
        auto value = l->get_value();
        if (!key.starts_with(table_key)) {
            return -1;
        }
        reply_encoded_key(ctx, art::value_type{key.bytes + table_key.size, key.size - table_key.size});
        auto *val = ValkeyModule_CreateString(ctx, value.chars(), value.size);
        ValkeyModule_ReplyWithString(ctx, val);
        responses += 2;

        return 0;
    };
    art::range(get_art(argv), search_start, search_end, table_iter);

    ValkeyModule_ReplySetArrayLength(ctx, responses);
    return VALKEYMODULE_OK;
}

int cmd_HKEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);

    int responses = 0;
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    };
    auto t = get_art(argv);
    storage_release release(t->latch);
    art::value_type search_end = t->query.create({conversion::convert(n, nlen), art::ts_end});
    art::value_type search_start = t->query.prefix(2);
    art::value_type table_key = search_start;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    auto table_iter = [&](void *, art::value_type key, art::value_type unused(value))-> int {
        if (!key.starts_with(search_start)) {
            return -1;
        }
        reply_encoded_key(ctx, art::value_type{key.bytes + table_key.size, key.size - table_key.size});
        responses += 1;

        return 0;
    };
    art::range(get_art(argv), search_start, search_end, table_iter, nullptr);

    ValkeyModule_ReplySetArrayLength(ctx, responses);
    return VALKEYMODULE_OK;
}

int cmd_HEXISTS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    size_t nlen = 0;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    if (key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    auto t = get_art(argv);
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
    art::range(get_art(argv), search_start, search_end, table_iter, nullptr);

    return ValkeyModule_ReplyWithBool(ctx, responses > 0 ? 1 : 0);
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
