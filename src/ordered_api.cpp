//
// Created by teejip on 4/9/25.
//

#include "ordered_api.h"
#include "conversion.h"
#include "art.h"
#include "composite.h"
#include "keys.h"
#include "module.h"
#include "vk_caller.h"
// TODO: one day this counters gonna wrap
static std::atomic<int64_t> counter = art::now() * 1000000;
#define IX_MEMBER ""
art::tree *get_art_s(const std::string& key) {
    return get_art(get_shard(key));
}

struct query_pool {
    composite query[max_queries_per_call]{};
    heap::set<size_t> available{};

    query_pool() {
        for (size_t i = 0; i < max_queries_per_call; i++) {
            available.insert(i);
        }
    }

    size_t create() {
        if (!available.empty()) {
            size_t r = *available.begin();
            available.erase(r);
            return r;
        }
        abort();
    }

    composite &operator[](size_t i) {
        if (i >= max_queries_per_call) {
            abort();
        }
        return query[i];
    }

    void release(size_t id) {
        if (available.contains(id)) {
            abort();
        }
        available.insert(id);
    }
};

thread_local query_pool queries;

struct query {
    size_t id = queries.create();
    composite *cache = &queries[id];

    composite *operator->() const {
        return cache; //&queries[id];
    }

    ~query() {
        queries.release(id);
    }
};

struct ordered_keys {
    ordered_keys(const ordered_keys &) = default;

    ordered_keys(ordered_keys &&) = default;

    ordered_keys(
        const composite &score_key,
        const composite &member_key,
        art::value_type value) : score_key(score_key), member_key(member_key), value(value) {
    }

    composite score_key;
    composite member_key;
    art::value_type value;
};

void insert_ordered(composite &score_key, composite &member_key, art::value_type value, bool update = false) {
    auto sk = score_key.create();
    auto mk = member_key.create();
    if (score_key.comp.size() < 2) {
        abort_with("invalid key buffer size");
    }
    art::value_type shk = score_key.comp[1].get_value();
    if (shk.size < 3) {
        abort_with("invalid key size");
    }
    shk = shk.sub(1,shk.size - 2);
    auto t = get_art(get_shard(shk));
    storage_release release(t->latch);
    t->insert(sk, value, update);
    t->insert(mk, sk, update);
}

void remove_ordered(composite &score_key, composite &member_key) {
    auto sk = score_key.create();
    auto mk = member_key.create();
    if (score_key.comp.size() < 2) {
        abort_with("invalid key buffer size");
    }
    art::value_type shk = score_key.comp[1].get_value();
    if (shk.size < 3) {
        abort_with("invalid key size");
    }
    shk = shk.sub(1,shk.size - 2);
    auto t = get_art(get_shard(shk));
    storage_release release(t->latch);
    t->remove(sk);
    t->remove(mk);
}

void insert_ordered(ordered_keys &thing, bool update = false) {
    insert_ordered(thing.score_key, thing.member_key, thing.value, update);
}

void remove_ordered(ordered_keys &thing) {
    remove_ordered(thing.score_key, thing.member_key);
}



int ZADD(caller& call, const arg_t &argv) {

    if (argv.size() < 4)
        return call.wrong_arity();
    int responses = 0;
    int r = call.ok();
    art::zadd_spec zspec(argv);
    if (zspec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    auto key = argv[1];
    if (key_ok(key) != 0) {
        return call.null();
    }

    auto t = get_art(key);
    storage_release release(t->latch);

    zspec.LFI = true;
    int64_t updated = 0;
    int64_t fkadded = 0;
    auto fc = [&](const art::node_ptr &) -> void {
        ++updated;
    };
    auto fcfk = [&](const art::node_ptr &val) -> void {
        if (val.is_leaf) {
            t->remove(val.const_leaf()->get_value());
        }
        --fkadded;
    };

    auto before = t->size;
    auto container = conversion::convert(key);
    for (size_t n = zspec.fields_start; n < argv.size(); n += 2) {
        auto k = argv[n];
        if (n + 1 >= argv.size()) {
            return call.syntax_error();
        }
        auto v =argv[n + 1];

        if (key_ok(k) != 0 || key_ok(v) != 0) {
            r |= call.null();
            ++responses;
            continue;
        }

        auto score = conversion::convert(k, true);
        auto member = conversion::convert(v);
        conversion::comparable_key id{++counter};
        if (score.ctype() != art::tfloat && score.ctype() != art::tdouble) {
            r |= call.null();
            ++responses;
            continue;
        }
        art::value_type qkey = t->cmd_ZADD_q1.create({container, score, member});
        if (zspec.XX) {
            art::update(t, qkey, [&](const art::node_ptr &old) -> art::node_ptr {
                if (old.null()) return nullptr;

                auto l = old.const_leaf();
                return art::make_leaf(*t, qkey, {}, l->ttl(), l->is_volatile());
            });
        } else {
            if (zspec.LFI) {
                auto member_key = t->cmd_ZADD_qindex.create({IX_MEMBER, container, member}); //, score
                art_insert(t, {}, member_key, qkey, true, fcfk);
                ++fkadded;
            }

            art_insert(t, {}, qkey, {}, !zspec.NX, fc);
        }
        ++responses;
    }
    auto current = t->size;
    if (zspec.CH) {
        call.long_long(current - before + updated - fkadded);
    } else {
        call.long_long(current - before - fkadded);
    }

    return call.ok();
}
int cmd_ZADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZADD);
}

int ZREM(caller& call, const arg_t& argv) {

    if (argv.size() < 3)
        return call.wrong_arity();
    int responses = 0;
    int r = call.ok();
    int64_t removed = 0;
    auto key = argv[1];
    if (key_ok(key) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto container = conversion::convert(key);
    query q1, qmember;
    q1->create({container});
    auto member_prefix = qmember->create({IX_MEMBER, container});
    for (size_t n = 2; n < argv.size(); ++n) {
        auto mem = argv[n];

        if (key_ok(mem) != 0) {
            r |= call.null();
            ++responses;
            continue;
        }

        auto member = conversion::convert(mem);
        conversion::comparable_key id{++counter};
        qmember->push(member);
        art::iterator byscore(t, qmember->create());
        if (byscore.ok()) {
            auto kscore = byscore.key();
            if (!kscore.starts_with(member_prefix)) break;
            auto fkmember = byscore.value();
            t->remove(fkmember);
            if (byscore.remove()) {
                ++removed;
            }
        }
        qmember->pop(1);
        ++responses;
    }

    return call.long_long(removed);
}
int cmd_ZREM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREM);
}

int ZINCRBY(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    int responses = 0;
    int64_t updated = 0;
    auto fc = [&](art::node_ptr) -> void {
        ++updated;
    };
    auto key = argv[1];
    if (key_ok(key) != 0) {
        return call.null();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);

    double incr = 0.0f;
    if (!conversion::to_double(argv[2], incr)) {
        return call.error("invalid argument");
    }
    auto v = argv[3];

    if (key_ok(v) != 0) {
        return call.error("invalid argument");
    }
    auto target = conversion::convert(v, true);
    auto target_member = target.get_value();
    auto container = conversion::convert(key);
    query q1, q2;
    auto prefix = q1->create({container});
    art::iterator scores(t, prefix);
    // we'll just add a bucket index to make scanning these faster without adding
    // too much data
    while (scores.ok()) {
        auto k = scores.key();
        auto val = scores.value();
        if (!k.starts_with(prefix)) break;
        auto encoded_number = k.sub(prefix.size, numeric_key_size);
        auto member = k.sub(prefix.size + numeric_key_size);
        if (target_member == member) {
            double number = conversion::enc_bytes_to_dbl(encoded_number);
            number += incr;
            conversion::comparable_key id{++counter};
            q1->push(conversion::comparable_key(number));
            q1->push(member);
            art::value_type qkey = q1->create();
            art_insert(t, {}, qkey, val, true, fc);

            q1->pop(2);
            if (!scores.remove()) // remove the current one
            {
                return call.error("internal error");
            };

            ++responses;
            return call.double_(number);
        }

        scores.next();
    }
    if (responses == 0) {
        auto score = conversion::comparable_key(incr);
        auto member = conversion::convert(v);
        q1->push(score);
        q1->push(member);
        art::value_type qkey = q1->create();
        art::value_type qv = v ;
        art_insert(t, {}, qkey, qv, true, fc);
        q1->pop(2);
        return call.double_(incr);
    }

    return 0;
}
int cmd_ZINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINCRBY);
}
int cmd_ZCOUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);
    if (argc < 4)
        return ValkeyModule_WrongArity(ctx);
    auto t = get_art(argv);
    storage_release release(t->latch);
    size_t nlen, minlen, maxlen;
    const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
    const char *smin = ValkeyModule_StringPtrLen(argv[2], &minlen);
    const char *smax = ValkeyModule_StringPtrLen(argv[3], &maxlen);

    if (key_ok(smin, minlen) != 0 || key_ok(smax, maxlen) != 0 || key_ok(n, nlen) != 0) {
        return ValkeyModule_ReplyWithNull(ctx);
    }

    auto container = conversion::convert(n, nlen);
    auto mn = conversion::convert(smin, minlen, true);
    auto mx = conversion::convert(smax, maxlen, true);
    query lq, uq, pq;
    auto lower = lq->create({container, mn});
    auto prefix = pq->create({container});
    auto upper = uq->create({container, mx});
    long long count = 0;
    art::iterator ai(t, lower);
    while (ai.ok()) {
        auto ik = ai.key();
        if (!ik.starts_with(prefix)) break;
        if (ik.sub(0, prefix.size + numeric_key_size) <= upper) {
            ++count;
        } else {
            break;
        }
        ai.next();
    }
    return ValkeyModule_ReplyWithLongLong(ctx, count);
}

static int zrange(caller& call, art::tree* t, const art::zrange_spec &spec) {

    auto container = conversion::convert(spec.key);
    auto mn = conversion::convert(spec.start, true);
    auto mx = conversion::convert(spec.stop, true);
    query lq, uq, pq, tq;
    art::value_type lower;
    art::value_type prefix;
    art::value_type nprefix;
    art::value_type upper;
    if (spec.BYLEX) {
        // it is implied that mn and mx are non-numeric strings
        lower = lq->create({IX_MEMBER, container, mn});
        prefix = pq->create({IX_MEMBER, container});
        nprefix = tq->create({container});
        upper = uq->create({IX_MEMBER, container, mx});
    } else {
        lower = lq->create({container, mn});
        prefix = pq->create({container});
        nprefix = prefix;
        upper = uq->create({container, mx});
    }
    long long count = 0;
    long long replies = 0;
    heap::std_vector<std::pair<art::value_type, art::value_type> > bylex;
    heap::std_vector<art::value_type> rev;
    heap::vector<ordered_keys> removals;
    if (!spec.REMOVE)
        call.start_array();
    art::iterator ai(t,lower);
    while (ai.ok()) {
        auto v = ai.key();
        if (!v.starts_with(prefix)) {
            break;
        }
        art::value_type current_comp;
        if (spec.BYLEX) {
            current_comp = v.sub(0, prefix.size + mx.get_size() - 1);
            v = ai.value(); // in case of bylex the value is a fk to by-score
        } else {
            current_comp = v.sub(0, prefix.size + numeric_key_size);
        }

        if (current_comp <= upper) {
            bool doprint = !spec.count;

            if (spec.count && count >= spec.offset && (count - spec.offset < spec.count)) {
                doprint = true;
            }
            if (doprint) {
                auto encoded_number = v.sub(nprefix.size, numeric_key_size);
                auto member = v.sub(nprefix.size + numeric_key_size);
                bool pushed = false;

                if (spec.REV && !spec.REMOVE) {
                    if (spec.BYLEX) {
                        bylex.push_back({member, encoded_number});
                        pushed = true;
                    } else {
                        rev.push_back(v.sub(nprefix.size, numeric_key_size * 2));
                        pushed = true;
                    }
                }
                if (!pushed && spec.REMOVE) // scheduled for removal
                {
                    composite score_key, member_key;
                    score_key.create({container, encoded_number, member});
                    // fyi: member key means lex key
                    member_key.create({IX_MEMBER, container, member});
                    removals.push_back({score_key, member_key, art::value_type()});
                }
                if (!pushed && !spec.REMOVE) // bylex should be in correct order
                {
                    call.reply_encoded_key(member);
                    ++replies;
                    if (spec.has_withscores) {
                        call.reply_encoded_key(encoded_number);
                        ++replies;
                    }
                }
            }
            ++count;
        } else {
            break;
        }
        ai.next();
    }
    if (spec.BYLEX && !spec.REMOVE) {
        if (spec.REV) {
            std::sort(bylex.begin(), bylex.end(), [](auto &a, auto &b) {
                return b < a;
            });
        }
        for (auto &rec: bylex) {
            /// TODO: min max filter
            call.reply_encoded_key(rec.first);
            ++replies;
            if (spec.has_withscores) {
                call.reply_encoded_key(rec.second);
                ++replies;
            }
        }
    } else if (spec.REV && !spec.REMOVE) {
        std::sort(rev.begin(), rev.end(), [](auto &a, auto &b) {
            return b < a;
        });
        for (auto &rec: rev) {
            call.reply_encoded_key(rec.sub(numeric_key_size, numeric_key_size));
            ++replies;
            if (spec.has_withscores) {
                call.reply_encoded_key(rec.sub(0, numeric_key_size));
                ++replies;
            }
        }
    };
    if (!spec.REMOVE) {
        call.end_array(replies);
    } else {
        for (auto &r: removals) {
            remove_ordered(r.score_key, r.member_key);
        }
        return call.long_long(removals.size());
    }

    return 0;
}

int ZRANGE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }

    return zrange(call, t, spec);
}

int cmd_ZRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx,argv,argc,ZRANGE);
}

int ZCARD(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto n = argv[1];

    if (key_ok(n) != 0) {
        return call.null();
    }

    auto container = conversion::convert(n);
    query lq, uq;
    auto lower = lq->create({container});
    auto upper = uq->create({container, art::ts_end});
    long long count = 0;
    art::iterator ai(t, lower);
    while (ai.ok()) {
        if (!ai.key().starts_with(lower)) break;
        if (ai.key() <= upper) {
            ++count;
        } else {
            break;
        }
        ai.next();
    }
    return call.long_long(count);
}

int cmd_ZCARD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx,argv,argc,ZCARD);
}

enum ops {
    difference = 0,
    intersect = 1,
    onion = 2
};
#if 0
static double rnd(float f) {
	return std::round((double)f * 100000.0) / 100000.0;
}
#endif
static double rnd(double f) {
    return f;
}

static int ZOPER(
    caller& call,
    const arg_t& argv,
    ops operate,
    art::value_type store = {},
    bool card = false,
    bool removal = false) {

    if (argv.size() < 4)
        return call.wrong_arity();
    art::zops_spec spec(argv);

    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    if (spec.aggr == art::zops_spec::agg_none && store.empty())
        call.start_array();
    long long replies = 0;
    double aggr = 0.0f;
    size_t count = 0;
    size_t results_added = 0;
    heap::vector<ordered_keys> new_keys;
    heap::vector<ordered_keys> removed_keys;

    size_t ks = spec.keys.size();
    auto fk = spec.keys.begin();
    switch (spec.aggr) {
        case art::zops_spec::agg_none:
        case art::zops_spec::avg:
        case art::zops_spec::sum:
            break;
        case art::zops_spec::min:
            aggr = std::numeric_limits<double>::max();
            break;
        case art::zops_spec::max:
            aggr = std::numeric_limits<double>::min();
            break;
    }

    if (spec.aggr == art::zops_spec::min) {
        aggr = std::numeric_limits<double>::max();
    }
    if (fk != spec.keys.end()) {
        auto t = get_art_s(*fk);
        storage_release release(t->latch);

        query lq, uq;
        auto container = conversion::convert(*fk);
        auto lower = lq->create({container});

        art::iterator i(t, lower);

        for (; i.ok();) {
            auto v = i.key();
            if (!v.starts_with(lower)) break;
            auto encoded_number = v.sub(lower.size, numeric_key_size);
            auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
            auto number = conversion::enc_bytes_to_dbl(encoded_number);

            size_t found_count = 0;
            auto ok = spec.keys.begin();
            ++ok;

            for (; ok != spec.keys.end(); ++ok) {
                query tainerq, checkq;
                auto check_set = conversion::convert(*ok);
                auto check_tainer = tainerq->create({check_set});
                auto check = checkq->create({check_set, conversion::comparable_key(number)});
                art::iterator j(get_art_s(*ok), check);
                bool found = false;
                if (j.ok()) {
                    auto kf = j.key();
                    if (!kf.starts_with(check)) break;

                    auto efn = kf.sub(check_tainer.size, numeric_key_size);
                    auto fn = conversion::enc_bytes_to_dbl(efn);
                    found = fn == number;
                }
                if (found) found_count++;
            }

            if (count < spec.weight_values.size()) {
                number *= spec.weight_values[count];
            }
            bool add_result = false;
            switch (operate) {
                case intersect:
                    add_result = (found_count == ks - 1);
                    break;
                case difference:
                    add_result = (found_count == 0);
                    break;
                case onion: // union lol, does not work yet

                    break;
            }
            if (add_result) {
                ++results_added;
                switch (spec.aggr) {
                    case art::zops_spec::agg_none:
                        if (store.empty()) {
                            call.reply_encoded_key(member);
                            ++replies;
                            if (spec.has_withscores) {
                                call.reply_encoded_key(encoded_number);
                                ++replies;
                            }
                        } else {
                            // this is possible because the art tree is guaranteed not to reallocate
                            // anything during the compressed_release scope
                            if (!card) {
                                composite score_key, member_key;
                                if (removal) {
                                    score_key.create({container, encoded_number, member});
                                    member_key.create({IX_MEMBER, container, member});
                                    removed_keys.emplace_back(score_key, member_key, i.value());
                                } else {
                                    score_key.create({conversion::convert(store), encoded_number, member});
                                    member_key.create({IX_MEMBER, conversion::convert(store), member});
                                    new_keys.emplace_back(score_key, member_key, i.value());
                                }
                            }
                            replies++;
                        }

                        break;
                    case art::zops_spec::avg:
                    case art::zops_spec::sum:
                        aggr += rnd(number);
                        break;
                    case art::zops_spec::min:
                        aggr = std::min(rnd(number), aggr);
                        break;
                    case art::zops_spec::max:
                        aggr = std::max(rnd(number), aggr);
                        break;
                }
            }
            ++count;
            i.next();
        }
    }

    for (auto &ordered_keys: new_keys) {
        insert_ordered(ordered_keys);
    }
    for (auto &ordered_keys: removed_keys) {
        remove_ordered(ordered_keys);
    }
    if (replies == 0 && spec.aggr != art::zops_spec::agg_none) {
        return call.double_(results_added > 0 ? aggr : 0.0f);
    }
    if (store.empty()) {
        call.end_array(replies);
    } else {
        return call.long_long(replies);
    }

    return call.ok();
}

int ZDIFF(caller& call, const arg_t& argv) {
    try {
        return ZOPER(call, argv, difference);
    } catch (std::exception &e) {
        art::log(e,__FILE__,__LINE__);
    }
    return call.error("internal error");
}

int cmd_ZDIFF(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZDIFF);
}

int ZDIFFSTORE(caller& call, const arg_t& argv) {
    auto member = argv[1];
    if (member.empty())
        return call.error("syntax error");
    arg_t narg;
    std::copy(++argv.begin(), argv.end(), std::back_inserter(narg));
    return ZOPER(call, narg, difference, member);
}

int cmd_ZDIFFSTORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZDIFFSTORE);
}

int ZINTERSTORE(caller& call, const arg_t& argv) {
    auto member = argv[1];
    if (member.empty())
        return call.error("syntax error");
    arg_t narg;
    std::copy(++argv.begin(), argv.end(), std::back_inserter(narg));
    return ZOPER(call, narg, intersect, member);
}

int cmd_ZINTERSTORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTERSTORE);
}

int ZINTERCARD(caller& call, const arg_t& argv) {
    return ZOPER(call, argv, intersect, {"#",1}, true);
}

int cmd_ZINTERCARD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTERCARD);
}

int ZINTER(caller& call, const arg_t& argv) {
    try {
        return ZOPER(call, argv, intersect);
    } catch (std::exception &e) {
        art::log(e,__FILE__,__LINE__);
    }
    return call.error("internal error");
}

int cmd_ZINTER(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTER);
}

int ZPOPMIN(caller& call, const arg_t& argv) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    long long count = 1;
    long long replies = 0;
    auto k = argv[1];
    if (argv.size() == 3) {
        if (call.ok() != conversion::to_ll(argv[2], count)) {
            return call.error("invalid count");
        }
    }

    if (key_ok(k) != 0) {
        return call.null();
    }
    auto container = conversion::convert(k);
    query l, u;
    auto lower = l->create({container});
    call.start_array();

    for (long long c = 0; c < count; ++c) {
        art::iterator i(t, lower);
        if (!i.ok()) {
            break;
        }
        auto v = i.key();
        if (!v.starts_with(lower)) break;
        auto encoded_number = v.sub(lower.size, numeric_key_size);
        auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
        call.reply_encoded_key(encoded_number);
        call.reply_encoded_key(member);
        replies += 2;
        if (!i.remove()) {
            break;
        };
    }
    call.end_array(replies);
    return call.ok();
}
int cmd_ZPOPMIN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZPOPMIN);
}

int ZPOPMAX(caller& call, const arg_t& argv) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    long long count = 1;
    long long replies = 0;
    auto k = argv[1];
    if (argv.size() == 3) {
        if (!conversion::to_ll(argv[2], count)) {
            return call.error("invalid count");
        }
    }

    if (key_ok(k) != 0) {
        return call.null();
    }

    auto container = conversion::convert(k);
    query l, u;
    auto lower = l->create({container});
    auto upper = u->create({container, art::ts_end});
    call.start_array();

    for (long long c = 0; c < count; ++c) {
        art::iterator i(t, upper);
        if (!i.ok()) {
            break;
        }
        auto v = i.key();
        if (!v.starts_with(lower)) {
            i.previous();
            if (!i.ok()) {
                break;
            }
            v = i.key();
        };
        if (!v.starts_with(lower)) break;

        auto encoded_number = v.sub(lower.size, numeric_key_size);
        auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
        call.reply_encoded_key(encoded_number);
        call.reply_encoded_key(member);
        replies += 2;

        if (!i.remove()) {
            art::std_log("Could not remove key");
        };
    }
    call.end_array(replies);
    return call.ok();
}

int cmd_ZPOPMAX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZPOPMAX);
}

int ZREVRANGE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = false;
    return zrange(call, t, spec);
}

int cmd_ZREVRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGE);
}

int ZRANGEBYSCORE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = false;
    spec.BYLEX = false;
    return zrange(call, t, spec);
}

int cmd_ZRANGEBYSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANGEBYSCORE);
}

int ZREVRANGEBYSCORE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = false;
    return zrange(call, t, spec);
}
int cmd_ZREVRANGEBYSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGEBYSCORE);
}
int ZREMRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = false;
    spec.BYLEX = true;
    spec.REMOVE = true;
    return zrange(call, t, spec);
}

int cmd_ZREMRANGEBYLEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREMRANGEBYLEX);
}

int ZRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = false;
    spec.BYLEX = true;
    return zrange(call, t, spec);
}

int cmd_ZRANGEBYLEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANGEBYLEX);
}

int ZREVRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = true;
    return zrange(call, t, spec);
}

int cmd_ZREVRANGEBYLEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGEBYLEX);
}

int ZRANK(caller& call, const arg_t& argv) {
    if (argv.size() != 4) {
        return call.wrong_arity();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto c = argv[1];
    if (c.empty()) {
        return call.wrong_arity();
    }
    auto a = argv[2];
    if (a.empty()) {
        return call.wrong_arity();
    }
    auto b = argv[3];
    if (b.empty()) {
        return call.wrong_arity();
    }

    composite qlower, qupper;
    auto container = conversion::convert(c);
    auto lower = conversion::convert(a, true);
    auto upper = conversion::convert(b, true);
    auto min_key = qlower.create({container, lower});
    auto max_key = qupper.create({container, upper});
    if (max_key < min_key) {
        return call.long_long(0);
    }
    art::iterator first(t, min_key);

    int64_t rank = 0;
    if (first.ok()) {
        rank = first.distance(max_key);
    }

    return call.long_long(rank);
}
int cmd_ZRANK(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANK);
}
int ZFASTRANK(caller& call, const arg_t& argv) {
    if (argv.size() != 4) {
        return call.wrong_arity();
    }
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto c = argv[1];
    if (c.empty()) {
        return call.wrong_arity();
    }
    auto a = argv[2];
    if (a.empty()) {
        return call.wrong_arity();
    }
    auto b = argv[3];
    if (b.empty()) {
        return call.wrong_arity();
    }

    composite qlower, qupper;
    auto container = conversion::convert(c);
    auto lower = conversion::convert(a, true);
    auto upper = conversion::convert(b, true);
    auto min_key = qlower.create({container, lower});
    auto max_key = qupper.create({container, upper});
    if (max_key < min_key) {
        return call.long_long(0);
    }

    art::iterator first(t, min_key);
    art::iterator last(t, max_key);

    int64_t rank = 0;
    if (first.ok() && last.ok()) {
        rank += last.key() == max_key ? 1 : 0;
        rank += first.fast_distance(last);
    }

    return call.long_long(rank);
}
int cmd_ZFASTRANK(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZFASTRANK);
}
int add_ordered_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMIN), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZPOPMAX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZREM), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZCOUNT), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZCARD), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZDIFF), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZDIFFSTORE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZINTERSTORE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZINCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZREMRANGEBYLEX), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZINTERCARD), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZINTER), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGEBYSCORE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGEBYSCORE), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZREVRANGEBYLEX), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZRANGEBYLEX), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZRANK), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ZFASTRANK), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
