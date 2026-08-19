//
// Created by teejip on 4/9/25.
//

#include "ordered_api.h"
#include <cstdlib>
#include <cctype>
#include <optional>
#include "sharded_store.h"
#include <map>
#include <cmath>
#include "key_type.h"
#include "conversion.h"
#include "art/art.h"
#include "composite.h"
#include "art/iterator.h"
#include "keys.h"
#include "module.h"
#include "vk_caller.h"
// TODO: one day this counters gonna wrap
static std::atomic<int64_t> counter = art::now() * 1000000;
// The marker that puts a member index key before the score keys of the same set.
//
// It was the bare literal "", which does not build an empty component: the const char*
// form leaves out the separator, so the component after it merges in and the key came out
// byte for byte identical to that of a set whose name really began with an 0x03. Two
// different things with one encoding - see DONE 62.
#define IX_MEMBER conversion::empty_component()

static thread_local composite cmd_ZADD_q1;
static thread_local composite cmd_ZADD_qindex;
struct query_pool {
    composite query[max_queries_per_call]{};
    heap::unordered_set<size_t> available{};

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

static void insert_ordered(caller& call, composite &score_key, composite &member_key, art::value_type value, bool update = false) {
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
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.shard_for(shk);
    //write_lock release(t->latch); // the shard should be latched
    // try_lock, not a guard: the caller is expected to hold this shard already, and
    // this only takes it when nobody else has
    bool locked = t->get_latch().try_lock();
    try {
        t->insert(sk, value, update);
        t->insert(mk, sk, update);
    }catch (const std::exception& e) {
        barch::err({e.what()});
    }
    if (locked) {
        t->get_latch().unlock();
    }
}

static void remove_ordered(caller& call, composite &score_key, composite &member_key) {
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
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.shard_for(shk);
    // as add_ordered: the caller is expected to hold this shard already
    bool locked = t->get_latch().try_lock();
    //write_lock release(t->get_latch()); // the shard should be latched
    try {
        t->remove(sk);
        t->remove(mk);
    }catch (const std::exception& e) {
        barch::err({e.what()});
    }
    if (locked) {
        t->get_latch().unlock();
    }
}

void insert_ordered(caller& call, ordered_keys &thing, bool update = false) {
    insert_ordered(call, thing.score_key, thing.member_key, thing.value, update);
}

void remove_ordered(caller& call, ordered_keys &thing) {
    remove_ordered(call, thing.score_key, thing.member_key);
}


/**
 * A score as redis reads one: the C library's parse, so inf and exponent forms are
 * numbers, and only nan and a word that is not a number at all are refused.
 *
 * conversion::to_double is stricter than that and rejects "inf", which made ZINCRBY refuse
 * an increment redis stores happily.
 */
static bool read_score(art::value_type v, double& out) {
    std::string t(v.chars(), v.size);
    if (t.empty()) return false;
    char *tail = nullptr;
    out = std::strtod(t.c_str(), &tail);
    if (tail != t.c_str() + t.size()) return false;
    return !std::isnan(out);
}

extern "C"

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
        return call.push_null();
    }

    barch::sharded_store kstore(call.kspace());
    // a name already holding a plain value, a list or a hash is not an ordered set to add
    // members to. Before the lock, because this routes to shards of its own - key_type.h
    if (!barch::container_writable(kstore, key, barch::container_kind::ordered_map)) {
        return call.push_error(barch::wrong_type_message());
    }
    auto t = kstore.write_locked(key);

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

    auto before = t->get_size();
    auto container = conversion::convert(key);
    for (size_t n = zspec.fields_start; n < argv.size(); n += 2) {
        auto k = argv[n];
        if (n + 1 >= argv.size()) {
            return call.syntax_error();
        }
        auto v =argv[n + 1];

        // the score is judged before the key check, because an empty or unreadable score
        // is a bad score rather than a bad key - `ZADD z '' m` used to answer as though it
        // had worked, and so did `ZADD z nan m`
        double sc = 0;
        if (!read_score(k, sc)) {
            return call.push_error("value is not a valid float");
        }
        if (key_ok(k) != 0 || key_ok(v) != 0) {
            r |= call.push_null();
            ++responses;
            continue;
        }

        // the score is encoded from the number already parsed rather than from the text
        // again. The text reader does not accept "inf", so a positive infinity was being
        // stored as the string "inf" - a component of a different shape - and every read
        // of that member afterwards missed it. -inf happened to survive, which is why the
        // two behaved differently
        auto score = conversion::comparable_key(sc);
        auto member = conversion::convert(v);
        if (score.ctype() != art::tfloat && score.ctype() != art::tdouble) {
            r |= call.push_null();
            ++responses;
            continue;
        }

        art::value_type qkey = cmd_ZADD_q1.create(art::ts_ordered_map, {container, score, member});
        if (zspec.XX) {
            t->update(qkey, [&](const art::node_ptr &old) -> art::node_ptr {
                if (old.null()) return nullptr;

                auto l = old.const_leaf();
                return art::make_leaf(t->get_ap(), qkey, {}, l->expiry_ms(), l->is_volatile());
            });
        } else {
            if (zspec.LFI) {
                auto member_key = cmd_ZADD_qindex.create(art::ts_ordered_map, {IX_MEMBER, container, member}); //, score
                t->insert({}, member_key, qkey, true, fcfk);
                ++fkadded;
            }

            t->insert({}, qkey, {}, !zspec.NX, fc);
        }
        ++responses;
    }
    // a waiter on BZMPOP needs to know the set now has a member. Cheap when nobody is
    // waiting, and the only way a blocking pop on a zset ever wakes
    t->call_unblock(key.to_string());
    auto current = t->get_tree_size();
    if (zspec.CH) {
        call.push_ll(current - before + updated - fkadded);
    } else {
        call.push_ll(current - before - fkadded);
    }

    return call.ok();
}
int cmd_ZADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZADD);
}
extern "C"
int ZREM(caller& call, const arg_t& argv) {

    if (argv.size() < 3)
        return call.wrong_arity();
    int responses = 0;
    int r = call.ok();
    int64_t removed = 0;
    auto key = argv[1];
    if (key_ok(key) != 0) {
        return call.push_null();
    }
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    auto container = conversion::convert(key);
    query q1, qmember;
    q1->create(art::ts_ordered_map, {container});
    qmember->create(art::ts_ordered_map, {IX_MEMBER, container});
    for (size_t n = 2; n < argv.size(); ++n) {
        auto mem = argv[n];

        if (key_ok(mem) != 0) {
            r |= call.push_null();
            ++responses;
            continue;
        }

        auto member = conversion::convert(mem);
        qmember->push(member);
        art::value_type wanted = qmember->create();
        art::iterator byscore(t, wanted);
        if (byscore.ok()) {
            auto kscore = byscore.key();
            // the iterator is a lower bound, so a member that is not there lands on
            // whichever member comes after it. Two things went wrong with that: the walk
            // broke out of the whole loop rather than skipping the one member, so
            // `ZREM k missing present` removed nothing and answered 0; and the key it
            // landed on was never checked against the one asked for, so the member that
            // happened to be next could be removed in place of the one that was not there
            if (kscore == wanted) {
                auto fkmember = byscore.value();
                t->remove(fkmember);
                if (byscore.remove()) {
                    ++removed;
                }
            }
        }
        qmember->pop(1);
        ++responses;
    }

    return call.push_ll(removed);
}
int cmd_ZREM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREM);
}

extern "C"
int ZINCRBY(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    int responses = 0;
    int64_t updated = 0;
    auto key = argv[1];
    if (key_ok(key) != 0) {
        return call.push_null();
    }
    barch::sharded_store kstore(call.kspace());
    // ZINCRBY creates the set when it is not there, so it claims the name like ZADD does
    if (!barch::container_writable(kstore, argv[1], barch::container_kind::ordered_map)) {
        return call.push_error(barch::wrong_type_message());
    }
    auto t = kstore.write_locked(argv[1]);
    auto fcfk = [&](const art::node_ptr& ) -> void {
        ++updated;
    };

    double incr = 0.0f;
    // an infinite increment is allowed - redis stores it and only complains when the
    // arithmetic would produce a nan, which is adding opposite infinities. A nan handed
    // in directly is refused here, in redis's wording rather than "invalid argument"
    if (!read_score(argv[2], incr)) {
        return call.push_error("value is not a valid float");
    }
    auto v = argv[3];

    if (key_ok(v) != 0) {
        return call.push_error("invalid argument");
    }
    auto target = conversion::convert(v, true);
    auto target_member = target.get_value();
    auto container = conversion::convert(key);
    query q1, q2, qfield;
    art::value_type field_key = qfield->create(art::ts_ordered_map, {IX_MEMBER, container, target});
    auto prefix = q1->create(art::ts_ordered_map, {container},false);

    art::iterator fields(t, field_key);
    if (fields.ok()) {
        auto kf = fields.key();
        if (kf.starts_with(field_key)) {
            art::iterator scores(t, fields.value());
            if (scores.ok()) {
                auto k = scores.key();
                auto val = scores.value();
                if (k.starts_with(prefix)) {
                    auto encoded_number = k.sub(prefix.size, numeric_key_size);
                    auto member = k.sub(prefix.size + numeric_key_size);
                    if (target_member == member) {
                        double number = conversion::enc_bytes_to_dbl(encoded_number);
                        number += incr;
                        // adding opposite infinities is the one arithmetic redis refuses,
                        // because the result cannot be ordered against anything
                        if (std::isnan(number)) {
                            return call.push_error("resulting score is not a number (NaN)");
                        }
                        q1->push(conversion::comparable_key(number));
                        q1->push(member);
                        art::value_type qkey = q1->create();
                        t->insert({}, qkey, val, true, fcfk);
                        t->insert( kf, qkey, true);

                        q1->pop(2);
                        if (!scores.remove()) // remove the current one
                        {
                            return call.push_error("internal error");
                        };

                        ++responses;
                        return call.push_double(number);
                    }
                }
            }
        };
    }


    if (responses == 0) {
        // A member has two keys: the score ordered one, and its entry in the member index
        // that says where to find it. This branch wrote only the first, so nothing could
        // find the member afterwards - ZSCORE answered nil, and the next ZINCRBY did not
        // find it either, so instead of adding to the score it wrote a second entry for
        // the same member. That is where the duplicate in a ZRANGE reply came from, which
        // TODO 64 had put down to the translated tests leaning on each other. It was this.
        composite score_key, member_key;
        auto mk = conversion::convert(v);
        score_key.create(art::ts_ordered_map, {container, conversion::comparable_key(incr), mk});
        member_key.create(art::ts_ordered_map, {IX_MEMBER, container, mk});
        ordered_keys fresh(score_key, member_key, v);
        insert_ordered(call, fresh);
        return call.push_double(incr);
    }

    return 0;
}
int cmd_ZINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINCRBY);
}


extern "C"
int ZCOUNT(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    size_t nlen, minlen, maxlen;
    const char *n = argv[1].chars(); nlen = argv[1].size;
    const char *smin = argv[2].chars(); minlen = argv[2].size;
    const char *smax = argv[3].chars();maxlen = argv[3].size;

    if (key_ok(smin, minlen) != 0 || key_ok(smax, maxlen) != 0 || key_ok(n, nlen) != 0) {
        return call.push_null();
    }

    auto container = conversion::convert(n, nlen);
    auto mn = conversion::convert(smin, minlen, true);
    auto mx = conversion::convert(smax, maxlen, true);
    query lq, uq, pq;
    auto lower = lq->create(art::ts_ordered_map, {container, mn});
    auto prefix = pq->create(art::ts_ordered_map, {container});
    auto upper = uq->create(art::ts_ordered_map, {container, mx});
    long long count = 0;
    art::iterator ai(t, lower);
    while (ai.ok()) {
        auto ik = ai.key();
        if (!ik.starts_with(prefix.pref(1))) break;
        if (ik.sub(0, prefix.size + numeric_key_size) <= upper) {
            ++count;
        } else {
            break;
        }
        ai.next();
    }
    return call.push_ll(count);
}
int cmd_ZCOUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZCOUNT);

}

/**
 * ZRANGE and ZREVRANGE with start and stop read as positions, which is what redis means
 * by them unless BYSCORE or BYLEX says otherwise.
 *
 * They used to be converted to scores and used as bounds, so `ZRANGE key 0 -1` - the way
 * almost every example fetches a whole set - described a range whose upper bound sorted
 * below its lower one and answered empty. See TODO 38.
 *
 * Positions are zero based and both ends are inclusive; a negative counts back from the
 * end, so -1 is the last member. Out of range positions are clamped rather than refused,
 * again as redis does, so 0 to -1 is the whole set whatever its size and an empty set
 * answers with an empty array rather than an error.
 */
static int zrange_by_index(caller& call, barch::shard_ptr t, const art::zrange_spec &spec) {
    auto parse = [](const std::string& text, int64_t& out) -> bool {
        if (text.empty()) return false;
        char* end = nullptr;
        long long v = std::strtoll(text.c_str(), &end, 10);
        if (end == text.c_str() || *end != '\0') return false;
        out = (int64_t) v;
        return true;
    };
    int64_t start = 0, stop = 0;
    if (!parse(spec.start, start) || !parse(spec.stop, stop)) {
        return call.push_error("value is not an integer or out of range");
    }

    auto container = conversion::convert(spec.key);
    query lq, pq;
    auto lower = lq->create(art::ts_ordered_map, {container});
    auto prefix = pq->create(art::ts_ordered_map, {container}, false);

    // one pass in key order, which is score order. The members are held as they are
    // found, the way the existing REV path already does, and the shard stays locked
    // for the whole call
    // score and member are kept separately because only the score has a fixed width. The
    // pair used to be held as one slice of numeric_key_size * 2 and the member read back
    // out of its second half, which silently cut every member to ten characters - a
    // member is whatever length the caller gave it, and there is no bound to slice at
    struct scored { art::value_type score, member; };
    heap::std_vector<scored> found;
    art::iterator ai(t, lower);
    while (ai.ok()) {
        auto v = ai.key();
        if (!v.starts_with(prefix)) break;
        if (v.size <= prefix.size + numeric_key_size) { ai.next(); continue; }
        found.push_back({v.sub(prefix.size, numeric_key_size),
                         v.sub(prefix.size + numeric_key_size,
                               v.size - prefix.size - numeric_key_size)});
        ai.next();
    }
    const int64_t n = (int64_t) found.size();
    if (start < 0) start += n;
    if (stop < 0) stop += n;
    if (start < 0) start = 0;
    if (stop >= n) stop = n - 1;

    call.start_array();
    if (n > 0 && start <= stop && start < n) {
        for (int64_t i = start; i <= stop; ++i) {
            // REV counts positions from the high score end
            const auto& rec = spec.REV ? found[(size_t) (n - 1 - i)] : found[(size_t) i];
            call.push_encoded_key(rec.member);
            if (spec.has_withscores) {
                call.push_encoded_key(rec.score);
            }
        }
    }
    call.end_array();
    return call.ok();
}

static int zrange(caller& call, barch::shard_ptr t, const art::zrange_spec &spec) {

    auto container = conversion::convert(spec.key);
    // A lex bound carries the character that says whether its end is open - `[a` includes
    // a, `(a` excludes it, and `-` and `+` are the ends of the range. barch only ever
    // understood the bare form, so the bracket has to come off before the bound is
    // encoded, or `[a` looks for a member whose name starts with a bracket
    std::string lex_start(spec.start);
    std::string lex_stop(spec.stop);
    bool lex_open_start = false, lex_open_stop = false;
    // `-` and `+` are the ends of the range rather than members, so they bound nothing
    bool lex_from_start = false, lex_to_end = false;
    if (spec.BYLEX) {
        auto strip = [](std::string v, bool& open) -> std::string {
            if (v.empty()) return v;
            if (v[0] == '(' || v[0] == '[') {
                open = (v[0] == '(');
                return v.substr(1);
            }
            return v;
        };
        lex_from_start = (lex_start == "-");
        lex_to_end = (lex_stop == "+");
        if (!lex_from_start) lex_start = strip(lex_start, lex_open_start);
        if (!lex_to_end) lex_stop = strip(lex_stop, lex_open_stop);
    }
    auto mn = conversion::convert(spec.BYLEX ? lex_start : std::string(spec.start), true);
    auto mx = conversion::convert(spec.BYLEX ? lex_stop : std::string(spec.stop), true);
    query lq, uq, pq, tq, xq;
    art::value_type upper_exact;
    art::value_type lower;
    art::value_type prefix;
    art::value_type nprefix;
    art::value_type upper;
    if (spec.BYLEX) {
        // it is implied that mn and mx are non-numeric strings
        prefix = pq->create(art::ts_ordered_map, {IX_MEMBER, container},false);
        nprefix = tq->create(art::ts_ordered_map, {container},false);
        // `-` starts at the first member there is, which is the prefix itself
        lower = lex_from_start ? prefix
                               : lq->create(art::ts_ordered_map, {IX_MEMBER, container, mn});
        upper = uq->create(art::ts_ordered_map, {IX_MEMBER, container, mx},false);
        // the whole key for the member the stop names, which is what an exclusive stop has
        // to recognise. Comparing the truncated form cannot do it: the member equal to the
        // bound is a prefix of it and so reads as less than, which is exactly the case
        // being excluded
        upper_exact = xq->create(art::ts_ordered_map, {IX_MEMBER, container, mx});
    } else {
        lower = lq->create(art::ts_ordered_map, {container, mn});
        prefix = pq->create(art::ts_ordered_map, {container},false);
        nprefix = prefix;
        upper = uq->create(art::ts_ordered_map, {container, mx},false);
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
        // an exclusive start names a member that is not in the range. The walk begins at
        // that member, so it is the first entry and skipping it is the whole of it
        if (spec.BYLEX && lex_open_start && v == lower) {
            ai.next();
            continue;
        }
        // an exclusive stop ends the walk at the member it names, before reporting it
        if (spec.BYLEX && lex_open_stop && !lex_to_end && v == upper_exact) {
            break;
        }
        art::value_type current_comp;
        if (spec.BYLEX) {
            current_comp = v.sub(0, prefix.size + mx.get_size() - 1);
            v = ai.value(); // in case of bylex the value is a fk to by-score
        } else {
            current_comp = v.sub(0, prefix.size + numeric_key_size);
        }
        // `+` has no upper bound to compare against, and an exclusive stop excludes the
        // member it names rather than including it
        bool within = (spec.BYLEX && lex_to_end) || current_comp <= upper;
        if (within) {
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
                    score_key.create(art::ts_ordered_map, {container, encoded_number, member});
                    // fyi: member key means lex key
                    member_key.create(art::ts_ordered_map, {IX_MEMBER, container, member});
                    removals.push_back({score_key, member_key, art::value_type()});
                }
                if (!pushed && !spec.REMOVE) // bylex should be in correct order
                {
                    call.push_encoded_key(member);
                    ++replies;
                    if (spec.has_withscores) {
                        call.push_encoded_key(encoded_number);
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
            call.push_encoded_key(rec.first);
            ++replies;
            if (spec.has_withscores) {
                call.push_encoded_key(rec.second);
                ++replies;
            }
        }
    } else if (spec.REV && !spec.REMOVE) {
        std::sort(rev.begin(), rev.end(), [](auto &a, auto &b) {
            return b < a;
        });
        for (auto &rec: rev) {
            call.push_encoded_key(rec.sub(numeric_key_size, numeric_key_size));
            ++replies;
            if (spec.has_withscores) {
                call.push_encoded_key(rec.sub(0, numeric_key_size));
                ++replies;
            }
        }
    };
    if (!spec.REMOVE) {
        call.end_array();
    } else {
        for (auto &r: removals) {
            remove_ordered(call, r.score_key, r.member_key);
        }
        return call.push_ll(removals.size());
    }

    return 0;
}
extern "C"
int ZRANGE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    if (!spec.BYSCORE && !spec.BYLEX) {
        return zrange_by_index(call, t, spec);
    }
    return zrange(call, t, spec);
}

int cmd_ZRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx,argv,argc,ZRANGE);
}
extern "C"
int ZCARD(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    auto n = argv[1];

    if (key_ok(n) != 0) {
        return call.push_null();
    }

    auto container = conversion::convert(n);
    query lq, uq;
    auto lower = lq->create(art::ts_ordered_map, {container});
    auto upper = uq->create(art::ts_ordered_map, {container, art::ts_end});
    long long count = 0;
    art::iterator ai(t, lower);
    while (ai.ok()) {
        if (!ai.key().starts_with(lower.pref(1))) break;
        if (ai.key() <= upper) {
            ++count;
        } else {
            break;
        }
        ai.next();
    }
    return call.push_ll(count);
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


/**
 * Is `member` in this ordered set, and at what score?
 *
 * Through the member index, which is what it is for. The set algebra used to ask a
 * different question - it sought the other set at `{set, score}` and checked whether the
 * score sitting there matched - so it answered "does that set hold anything with the same
 * score", and two sets sharing a score looked like a match whatever their members were.
 * That is why an intersection came back looking like a union.
 */
static bool member_score(barch::sharded_store& kstore, const std::string& set,
                         art::value_type member, double& out) {
    composite mq, cq;
    auto container = conversion::convert(art::value_type{set});
    art::value_type mkey = mq.create(art::ts_ordered_map,
                                     {IX_MEMBER, container, conversion::comparable_key(member)});
    art::value_type prefix = cq.create(art::ts_ordered_map, {container}, false);
    bool found = false;
    kstore.with_container_read(art::value_type{set}, [&](const barch::shard_ptr& t) {
        auto n = t->search(mkey);
        if (n.null() || !n.is_leaf) return;
        auto l = n.const_leaf();
        if (l->is_tomb() || l->deleted() || l->expired()) return;
        // the index holds the score key, and the score is the component after the name
        auto sk = l->get_value();
        if (sk.size < prefix.size + numeric_key_size) return;
        out = conversion::enc_bytes_to_dbl(sk.sub(prefix.size, numeric_key_size));
        found = true;
    });
    return found;
}

/** every member of one ordered set, with its score */
static void each_member(barch::sharded_store& kstore, const std::string& set,
                        const std::function<void(art::value_type, double)>& cb) {
    composite lq;
    auto container = conversion::convert(art::value_type{set});
    art::value_type lower = lq.create(art::ts_ordered_map, {container});
    kstore.with_container_read(art::value_type{set}, [&](const barch::shard_ptr& t) {
        for (art::iterator i(t, lower); i.ok(); i.next()) {
            auto v = i.key();
            if (!v.starts_with(lower.pref(1))) break;
            if (v.size < lower.size + numeric_key_size) continue;
            const art::leaf *l = i.l();
            if (!l || l->is_tomb() || l->deleted() || l->expired()) continue;
            auto encoded_number = v.sub(lower.size, numeric_key_size);
            cb(v.sub(lower.size + numeric_key_size),
               conversion::enc_bytes_to_dbl(encoded_number));
        }
    });
}

/**
 * ZUNION, ZINTER and ZDIFF, and the STORE and CARD forms of them.
 *
 * One member of the result is one member of the inputs, and its score is what AGGREGATE
 * says to do with the scores it had in each input it appeared in, after each of those was
 * multiplied by that input's WEIGHT. Both of those used to be wrong in the same way: the
 * weight was indexed by how far through the first set the walk had got rather than by
 * which set the score came from, and only the first set's score was ever used, so the
 * aggregate had nothing to aggregate.
 *
 * A union has to walk every input, not just the first. That is why it was left unfinished -
 * the shape of the old loop could not express it.
 */
static int ZOPER(
    caller& call,
    const arg_t& argv,
    ops operate,
    art::value_type store = {},
    bool card = false,
    bool removal = false,
    const char *named = "zunion") {

    // three is enough: a count and one input. It used to want four, so a single input set
    // - which redis allows and its tests use - was refused as a wrong argument count
    if (argv.size() < 3)
        return call.wrong_arity();
    art::zops_spec spec(argv);

    if (spec.parse_options() != call.ok()) {
        if (spec.bad_limit) {
            return call.push_error("LIMIT can't be negative");
        }
        if (spec.no_keys) {
            return call.push_error(("at least 1 input key is needed for '"
                                    + std::string(named) + "' command").c_str());
        }
        return call.syntax_error();
    }
    if (spec.keys.empty()) {
        return call.push_error(("at least 1 input key is needed for '"
                                + std::string(named) + "' command").c_str());
    }
    // a STORE form writes a set rather than answering one, so it has nowhere to put the
    // scores WITHSCORES asks for - redis calls that a syntax error and its tests check it
    if (!store.empty() && spec.has_withscores) {
        return call.syntax_error();
    }
    barch::sharded_store kstore(call.kspace());

    auto weight_of = [&](size_t which) -> double {
        return which < spec.weight_values.size() ? spec.weight_values[which] : 1.0;
    };
    auto combine = [&](double have, double add, size_t seen) -> double {
        switch (spec.aggr) {
            case art::zops_spec::min: return seen ? std::min(have, add) : add;
            case art::zops_spec::max: return seen ? std::max(have, add) : add;
            default:                  return seen ? have + add : add;   // sum, and the default
        }
    };

    // member bytes to the score it has earned so far, and how many inputs it came from.
    // the encoded member is the identity here, exactly as it is in the store
    struct acc { double score{}; size_t seen{}; };
    std::map<std::string, acc> gathered;
    heap::std_vector<std::string> order;   // first appearance, so a tie is stable

    auto note = [&](art::value_type member, double score, size_t which) {
        std::string id(member.chars(), member.size);
        auto it = gathered.find(id);
        if (it == gathered.end()) {
            order.push_back(id);
            gathered[id] = acc{score * weight_of(which), 1};
        } else {
            it->second.score = combine(it->second.score, score * weight_of(which), it->second.seen);
            ++it->second.seen;
        }
    };

    if (operate == onion) {
        for (size_t k = 0; k < spec.keys.size(); ++k) {
            each_member(kstore, spec.keys[k], [&](art::value_type m, double sc) {
                note(m, sc, k);
            });
        }
    } else {
        // intersection and difference are both decided by the first set's members
        each_member(kstore, spec.keys[0], [&](art::value_type m, double sc) {
            size_t found = 0;
            double total = sc * weight_of(0);
            size_t seen = 1;
            for (size_t k = 1; k < spec.keys.size(); ++k) {
                double other = 0;
                if (member_score(kstore, spec.keys[k], m, other)) {
                    ++found;
                    total = combine(total, other * weight_of(k), seen);
                    ++seen;
                }
            }
            bool keep = (operate == intersect) ? (found == spec.keys.size() - 1)
                                               : (found == 0);
            if (keep) {
                std::string id(m.chars(), m.size);
                order.push_back(id);
                gathered[id] = acc{total, seen};
            }
        });
    }

    if (card) {
        long long n = (long long) order.size();
        if (spec.limit > 0 && n > spec.limit) n = spec.limit;
        return call.push_ll(n);
    }

    // redis answers these in score order, and by member where scores tie
    std::stable_sort(order.begin(), order.end(),
        [&](const std::string& a, const std::string& b) {
            double sa = gathered[a].score;
            double sb = gathered[b].score;
            if (sa != sb) return sa < sb;
            return a < b;
        });

    if (store.empty()) {
        call.start_array();
        for (const auto& id : order) {
            call.push_encoded_key(art::value_type{id});
            if (spec.has_withscores) {
                call.push_double(gathered[id].score);
            }
        }
        call.end_array();
        return call.ok();
    }

    // the destination is replaced, not added to, which is what redis does with it
    barch::remove_container(kstore, store);
    for (const auto& id : order) {
        composite score_key, member_key;
        auto dest = conversion::convert(store);
        art::value_type member{id};
        conversion::comparable_key sc(gathered[id].score);
        conversion::comparable_key mk(member);
        score_key.create(art::ts_ordered_map, {dest, sc, mk});
        member_key.create(art::ts_ordered_map, {IX_MEMBER, dest, mk});
        ordered_keys ok(score_key, member_key, {});
        insert_ordered(call, ok);
    }
    (void) removal;
    return call.push_ll((long long) order.size());
}

extern "C"
int ZDIFF(caller& call, const arg_t& argv) {
    try {
        return ZOPER(call, argv, difference, {}, false, false, "zdiff");
    } catch (std::exception &e) {
        barch::err({e.what(), __FILE__, __LINE__});
    }
    return call.push_error("internal error");
}

int cmd_ZDIFF(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZDIFF);
}
extern "C"
int ZDIFFSTORE(caller& call, const arg_t& argv) {
    // argv[1] was read before anything checked there was an argv[1], so calling this
    // bare answered with small_vector's `at()` rather than a wrong arity
    if (argv.size() < 4)
        return call.wrong_arity();
    auto member = argv[1];
    if (member.empty())
        return call.push_error("syntax error");
    arg_t narg;
    std::copy(++argv.begin(), argv.end(), std::back_inserter(narg));
    return ZOPER(call, narg, difference, member, false, false, "zdiffstore");
}

int cmd_ZDIFFSTORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZDIFFSTORE);
}
extern "C"
int ZINTERSTORE(caller& call, const arg_t& argv) {
    // argv[1] was read before anything checked there was an argv[1], so calling this
    // bare answered with small_vector's `at()` rather than a wrong arity
    if (argv.size() < 4)
        return call.wrong_arity();
    auto member = argv[1];
    if (member.empty())
        return call.push_error("syntax error");
    arg_t narg;
    std::copy(++argv.begin(), argv.end(), std::back_inserter(narg));
    return ZOPER(call, narg, intersect, member, false, false, "zinterstore");
}

int cmd_ZINTERSTORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTERSTORE);
}
/**
 * ZRANDMEMBER key [count [WITHSCORES]]
 *
 * HRANDFIELD over an ordered set, and it follows it closely (DONE 54): no count answers
 * one member, a positive count answers that many distinct ones, a negative count answers
 * exactly that many and allows repeats. The same bound applies for the same reason - the
 * whole reply is built before it is sent, so a magnitude past a million is refused rather
 * than attempted.
 */
/**
 * ZREMRANGEBYSCORE key min max - remove every member whose score falls in the range.
 *
 * Both ends are inclusive, and each member has two keys to remove: the score ordered one
 * and its entry in the member index. Removing while walking would invalidate the iterator,
 * so the range is collected first and the run is the size of what is being removed rather
 * than the size of the set.
 */
extern "C"
int ZREMRANGEBYSCORE(caller& call, const arg_t& argv) {
    if (argv.size() != 4)
        return call.wrong_arity();
    if (key_ok(argv[1]) != 0)
        return call.push_null();
    double lo = 0, hi = 0;
    if (!read_score(argv[2], lo) || !read_score(argv[3], hi)) {
        return call.push_error("min or max is not a float");
    }
    barch::sharded_store kstore(call.kspace());
    if (barch::kind_of(kstore, argv[1]) == barch::key_kind::string) {
        return call.push_error(barch::wrong_type_message());
    }
    std::string set(argv[1].chars(), argv[1].size);
    heap::std_vector<std::string> doomed;
    each_member(kstore, set, [&](art::value_type m, double sc) {
        if (sc < lo || sc > hi) return;
        doomed.emplace_back(m.chars(), m.size);
    });
    long long removed = 0;
    auto container = conversion::convert(argv[1]);
    for (const auto& id : doomed) {
        double sc = 0;
        art::value_type member{id};
        if (!member_score(kstore, set, member, sc)) continue;
        composite score_key, member_key;
        conversion::comparable_key mk(member);
        score_key.create(art::ts_ordered_map, {container, conversion::comparable_key(sc), mk});
        member_key.create(art::ts_ordered_map, {IX_MEMBER, container, mk});
        ordered_keys ok(score_key, member_key, {});
        remove_ordered(call, ok);
        ++removed;
    }
    return call.push_ll(removed);
}
int cmd_ZREMRANGEBYSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREMRANGEBYSCORE);
}

extern "C"
int ZRANDMEMBER(caller& call, const arg_t& argv) {
    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();
    if (key_ok(argv[1]) != 0)
        return call.push_null();
    bool counted = argv.size() > 2;
    long long count = 1;
    if (counted && !conversion::to_ll(argv[2], count)) {
        return call.push_error("value is not an integer or out of range");
    }
    bool with_scores = false;
    if (argv.size() == 4) {
        std::string opt(argv[3].chars(), argv[3].size);
        for (auto& ch : opt) ch = (char) toupper(ch);
        if (opt != "WITHSCORES") return call.syntax_error();
        with_scores = true;
    }
    if (count < -std::numeric_limits<long long>::max()
        || (with_scores && count < -(std::numeric_limits<long long>::max() / 2))
        || count < -1000000) {
        return call.push_error("value is out of range");
    }
    barch::sharded_store kstore(call.kspace());
    if (barch::kind_of(kstore, argv[1]) == barch::key_kind::string) {
        return call.push_error(barch::wrong_type_message());
    }
    std::string set(argv[1].chars(), argv[1].size);
    heap::std_vector<std::string> members;
    heap::std_vector<double> scores;
    each_member(kstore, set, [&](art::value_type m, double sc) {
        members.emplace_back(m.chars(), m.size);
        scores.push_back(sc);
    });
    auto emit = [&](size_t ix) {
        call.push_encoded_key(art::value_type{members[ix]});
        if (with_scores) call.push_double(scores[ix]);
    };
    if (members.empty()) {
        if (!counted) return call.push_null();
        call.start_array();
        call.end_array();
        return call.ok();
    }
    if (!counted) {
        call.push_encoded_key(art::value_type{members[std::rand() % members.size()]});
        return call.ok();
    }
    call.start_array();
    if (count < 0) {
        for (long long i = 0; i < -count; ++i) emit(std::rand() % members.size());
    } else {
        heap::std_vector<size_t> order;
        for (size_t i = 0; i < members.size(); ++i) order.push_back(i);
        for (size_t i = order.size(); i > 1; --i) {
            std::swap(order[i - 1], order[std::rand() % i]);
        }
        size_t want = std::min<size_t>((size_t) count, order.size());
        for (size_t i = 0; i < want; ++i) emit(order[i]);
    }
    call.end_array();
    return call.ok();
}
int cmd_ZRANDMEMBER(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANDMEMBER);
}

/**
 * ZSCORE key member, and ZMSCORE key member [member ...]
 *
 * The score a member holds, or nil when the set or the member is not there - which is the
 * one question the member index answers directly, so neither walks the set.
 */
extern "C"
int ZSCORE(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    if (key_ok(argv[1]) != 0)
        return call.push_null();
    barch::sharded_store kstore(call.kspace());
    if (barch::kind_of(kstore, argv[1]) == barch::key_kind::string) {
        return call.push_error(barch::wrong_type_message());
    }
    std::string set(argv[1].chars(), argv[1].size);
    auto wanted = conversion::convert(argv[2]);
    double score = 0;
    if (!member_score(kstore, set, wanted.get_value(), score)) {
        return call.push_null();
    }
    return call.push_double(score);
}
int cmd_ZSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZSCORE);
}
extern "C"
int ZMSCORE(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    if (key_ok(argv[1]) != 0)
        return call.push_null();
    barch::sharded_store kstore(call.kspace());
    if (barch::kind_of(kstore, argv[1]) == barch::key_kind::string) {
        return call.push_error(barch::wrong_type_message());
    }
    std::string set(argv[1].chars(), argv[1].size);
    call.start_array();
    for (size_t i = 2; i < argv.size(); ++i) {
        auto wanted = conversion::convert(argv[i]);
        double score = 0;
        if (member_score(kstore, set, wanted.get_value(), score)) {
            call.push_double(score);
        } else {
            call.push_null();
        }
    }
    call.end_array();
    return call.ok();
}
int cmd_ZMSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZMSCORE);
}
extern "C"
int ZUNION(caller& call, const arg_t& argv) {
    return ZOPER(call, argv, onion, {}, false, false, "zunion");
}
int cmd_ZUNION(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZUNION);
}
extern "C"
int ZUNIONSTORE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    auto dest = argv[1];
    if (dest.empty())
        return call.push_error("syntax error");
    arg_t narg;
    std::copy(++argv.begin(), argv.end(), std::back_inserter(narg));
    return ZOPER(call, narg, onion, dest, false, false, "zunionstore");
}
int cmd_ZUNIONSTORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZUNIONSTORE);
}
extern "C"
int ZINTERCARD(caller& call, const arg_t& argv) {
    return ZOPER(call, argv, intersect, {"#",1}, true, false, "zintercard");
}

int cmd_ZINTERCARD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTERCARD);
}
extern "C"
int ZINTER(caller& call, const arg_t& argv) {
    try {
        return ZOPER(call, argv, intersect, {}, false, false, "zinter");
    } catch (std::exception &e) {
        barch::err({e.what(), __FILE__, __LINE__});
    }
    return call.push_error("internal error");
}

int cmd_ZINTER(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZINTER);
}
static bool refuse_if_not_zset(caller& call, barch::sharded_store& store, art::value_type name) {
    if (barch::kind_of(store, name) == barch::key_kind::string) {
        call.push_error(barch::wrong_type_message());
        return true;
    }
    auto held = barch::kind_of_container(store, name);
    if (held != barch::container_kind::none && held != barch::container_kind::ordered_map) {
        call.push_error(barch::wrong_type_message());
        return true;
    }
    return false;
}

extern "C"
int ZPOPMIN(caller& call, const arg_t& argv) {

    if (argv.size() < 2)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    // type before the count: `ZPOPMIN k 0` on a string is WRONGTYPE, not an empty array
    if (refuse_if_not_zset(call, kstore, argv[1])) return 0;
    auto t = kstore.write_locked(argv[1]);
    long long count = 1;
    long long replies = 0;
    auto k = argv[1];
    if (argv.size() == 3) {
        if (call.ok() != conversion::to_ll(argv[2], count)) {
            return call.push_error("invalid count");
        }
    }

    if (key_ok(k) != 0) {
        return call.push_null();
    }
    auto container = conversion::convert(k);
    query l, u;
    auto lower = l->create(art::ts_ordered_map, {container});
    call.start_array();

    for (long long c = 0; c < count; ++c) {
        art::iterator i(t, lower);
        if (!i.ok()) {
            break;
        }
        auto v = i.key();
        if (!v.starts_with(lower.pref(1))) break;
        auto encoded_number = v.sub(lower.size, numeric_key_size);
        auto member = v.sub(lower.size + numeric_key_size); // theres a 0 char and I'm not sure where it comes from
        // member first, then its score - redis's order. it used to be the other way
        // round, which every client's ZPOPMIN/ZPOPMAX parser reads backwards
        call.push_encoded_key(member);
        call.push_encoded_key(encoded_number);
        replies += 2;
        if (!i.remove()) {
            break;
        };
    }
    call.end_array();
    return call.ok();
}
int cmd_ZPOPMIN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZPOPMIN);
}
extern "C"
int ZPOPMAX(caller& call, const arg_t& argv) {

    if (argv.size() < 2)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    if (refuse_if_not_zset(call, kstore, argv[1])) return 0;
    auto t = kstore.write_locked(argv[1]);
    long long count = 1;
    long long replies = 0;
    auto k = argv[1];
    if (argv.size() == 3) {
        if (!conversion::to_ll(argv[2], count)) {
            return call.push_error("invalid count");
        }
    }

    if (key_ok(k) != 0) {
        return call.push_null();
    }

    auto container = conversion::convert(k);
    query l, u;
    auto lower = l->create(art::ts_ordered_map, {container},false);
    auto upper = u->create(art::ts_ordered_map, {container, art::ts_end});
    call.start_array();
    for (long long c = 0; c < count; ++c) {
        art::iterator i(t, upper);
        if (!i.ok()) {
            // this may be because upper > last item in tree and therefore there's none not less than
            i.last();
            if (!i.ok())
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
        // member first, then its score - redis's order. it used to be the other way
        // round, which every client's ZPOPMIN/ZPOPMAX parser reads backwards
        call.push_encoded_key(member);
        call.push_encoded_key(encoded_number);
        replies += 2;

        if (!i.remove()) {
            barch::log({"Could not remove key"});
        };
    }
    call.end_array();
    return call.ok();
}

int cmd_ZPOPMAX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZPOPMAX);
}

struct zpopped {
    std::string member;
    std::string score;
};

/** pop up to count members from one set already locked on t. false when it was empty */
static bool zmpop_one(const barch::shard_ptr& t, art::value_type name,
                      bool want_max, int64_t count,
                      heap::std_vector<zpopped>& out) {
    auto container = conversion::convert(name);
    query lq, uq;
    auto lower = lq->create(art::ts_ordered_map, {container}, false);
    auto upper = uq->create(art::ts_ordered_map, {container, art::ts_end});
    for (int64_t n = 0; n < count; ++n) {
        art::iterator i(t, want_max ? upper : lower);
        art::value_type v{};
        if (!want_max) {
            if (!i.ok()) break;
            v = i.key();
            if (!v.starts_with(lower)) break;
        } else {
            if (!i.ok()) {
                i.last();
                if (!i.ok()) break;
            }
            v = i.key();
            if (!v.starts_with(lower)) {
                i.previous();
                if (!i.ok()) break;
                v = i.key();
            }
            if (!v.starts_with(lower)) break;
        }
        if (v.size < lower.size + numeric_key_size) break;
        auto encoded_number = v.sub(lower.size, numeric_key_size);
        auto member = v.sub(lower.size + numeric_key_size);
        zpopped one;
        one.member.assign(member.chars(), member.size);
        one.score.assign(encoded_number.chars(), encoded_number.size);
        // the index first, then the score key: ZPOPMIN used to leave the index behind
        composite member_key;
        member_key.create(art::ts_ordered_map,
                          {IX_MEMBER, container, art::value_type{one.member}});
        t->remove(member_key.create());
        if (!i.remove()) break;
        out.push_back(std::move(one));
    }
    return !out.empty();
}

static void reply_zmpop(caller& call, art::value_type name,
                        const heap::std_vector<zpopped>& popped) {
    call.start_array();
    call.push_vt(name);
    call.start_array();
    for (const auto& one : popped) {
        call.start_array();
        call.push_encoded_key(art::value_type{one.member});
        call.push_encoded_key(art::value_type{one.score});
        call.end_array();
    }
    call.end_array();
    call.end_array();
}

/**
 * ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
 * BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
 *
 * Pop from the first of several sets that has anything. Removal takes both keys -
 * the score-ordered one and the member index - which is what ZREM does and what
 * ZPOPMIN/ZPOPMAX still do not. The blocking form uses parse_block_timeout.
 */
static int zmpop(caller& call, const arg_t& argv, bool blocking) {
    const size_t numkeys_idx = blocking ? 2 : 1;
    if (argv.size() < numkeys_idx + 3) {
        return call.wrong_arity();
    }
    if (blocking && call.has_blocks()) {
        return call.push_error("block already set");
    }
    uint64_t time_out = 0;
    if (blocking && !parse_block_timeout(call, argv[1], time_out)) {
        return 0;
    }
    long long numkeys = 0;
    if (!conversion::to_ll(argv[numkeys_idx], numkeys) || numkeys < 1) {
        return call.push_error("numkeys should be greater than 0");
    }
    const size_t where_idx = numkeys_idx + (size_t) numkeys + 1;
    if (where_idx >= argv.size()) {
        return call.syntax_error();
    }
    std::string where(argv[where_idx].chars(), argv[where_idx].size);
    for (char& ch : where) ch = (char) std::toupper((unsigned char) ch);
    bool want_max = false;
    if (where == "MAX") {
        want_max = true;
    } else if (where != "MIN") {
        return call.syntax_error();
    }
    long long count = 1;
    bool saw_count = false;
    for (size_t i = where_idx + 1; i < argv.size(); ++i) {
        std::string opt(argv[i].chars(), argv[i].size);
        for (char& ch : opt) ch = (char) std::toupper((unsigned char) ch);
        if (!saw_count && opt == "COUNT" && i + 1 < argv.size()) {
            ++i;
            if (!conversion::to_ll(argv[i], count) || count < 1) {
                return call.push_error("count should be greater than 0");
            }
            saw_count = true;
        } else {
            return call.syntax_error();
        }
    }

    barch::sharded_store store(call.kspace());
    for (size_t i = 0; i < (size_t) numkeys; ++i) {
        if (refuse_if_not_zset(call, store, argv[numkeys_idx + 1 + i])) return 0;
    }

    auto spc = call.kspace();
    std::optional<barch::sharded_store::write_guard> space_lock;
    if (numkeys > 1) {
        space_lock = store.lock_space_write();
    }
    caller::keys_t blocks;
    for (size_t i = 0; i < (size_t) numkeys; ++i) {
        auto name = argv[numkeys_idx + 1 + i];
        if (key_ok(name) != 0) {
            return call.push_error("invalid key");
        }
        barch::sharded_store::write_locked_shard one;
        barch::shard_ptr t;
        if (numkeys == 1) {
            one = store.write_locked(name);
            t = one.ptr();
        } else {
            t = spc->get(name);
        }
        heap::std_vector<zpopped> popped;
        if (zmpop_one(t, name, want_max, count, popped)) {
            reply_zmpop(call, name, popped);
            return call.ok();
        }
        if (blocking) {
            blocks.emplace_back(name.to_string(), t->get_shard_number());
        }
    }
    if (blocking && !blocks.empty()) {
        call.add_block(blocks, time_out,
            [want_max, count](caller& cc, const caller::keys_t& keys) {
                if (keys.empty()) {
                    cc.push_null();
                    return;
                }
                barch::sharded_store st(cc.kspace());
                for (auto& k : keys) {
                    art::value_type name{k.key};
                    auto t = st.write_locked(name);
                    heap::std_vector<zpopped> popped;
                    if (zmpop_one(t, name, want_max, count, popped)) {
                        reply_zmpop(cc, name, popped);
                        return;
                    }
                }
                cc.push_null();
            });
        return 0;
    }
    return call.push_null();
}
extern "C"
int ZMPOP(caller& call, const arg_t& argv) {
    return zmpop(call, argv, false);
}
extern "C"
int BZMPOP(caller& call, const arg_t& argv) {
    return zmpop(call, argv, true);
}
extern "C"
int ZREVRANGE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = false;
    if (!spec.BYSCORE) {
        return zrange_by_index(call, t, spec);
    }
    return zrange(call, t, spec);
}

int cmd_ZREVRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGE);
}
// this is deprecated in later redis and can be replaced by range using BYSCORE command
/**
 * A score range bound, as redis reads one: a number, or an exclusive form written with a
 * leading `(`, or the words -inf and +inf. Refusing a bound that is none of those is the
 * point - a caller with a typo used to get an empty reply and no complaint.
 */
static bool score_bound(art::value_type v, double& out) {
    std::string t(v.chars(), v.size);
    if (t.empty()) return false;
    if (t[0] == '(') t.erase(0, 1);
    if (t.empty()) return false;
    return read_score(art::value_type{t}, out);
}

/**
 * A lex range bound. redis requires the leading character to say whether the end is open,
 * so `(a` and `[a` are bounds and a bare `a` is an error, with `-` and `+` meaning the
 * ends of the range.
 */
static bool lex_bound(art::value_type v) {
    if (v.size == 0) return false;
    char c = v.chars()[0];
    if (v.size == 1 && (c == '-' || c == '+')) return true;
    return c == '(' || c == '[';
}

extern "C"
int ZRANGEBYSCORE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    double lo = 0, hi = 0;
    if (!score_bound(argv[2], lo) || !score_bound(argv[3], hi)) {
        return call.push_error("min or max is not a float");
    }
    spec.REV = false;
    spec.BYLEX = false;
    return zrange(call, t, spec);
}

int cmd_ZRANGEBYSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANGEBYSCORE);
}
// also deprecated can be replaced by REVRANGE with BYSCORE arg
extern "C"
int ZREVRANGEBYSCORE(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = false;
    // by score is in the name, so the index path never applies here - an earlier
    // edit to ZREVRANGE matched this function too and sent 3.01 down it
    spec.BYSCORE = true;
    return zrange(call, t, spec);
}
int cmd_ZREVRANGEBYSCORE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGEBYSCORE);
}
extern "C"
int ZREMRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
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
extern "C"
int ZRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    if (!lex_bound(argv[2]) || !lex_bound(argv[3])) {
        return call.push_error("min or max not valid string range item");
    }
    spec.REV = false;
    spec.BYLEX = true;
    return zrange(call, t, spec);
}

int cmd_ZRANGEBYLEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANGEBYLEX);
}
extern "C"
int ZREVRANGEBYLEX(caller& call, const arg_t& argv) {
    if (argv.size() < 4)
        return call.wrong_arity();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
    art::zrange_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.push_error("syntax error");
    }
    spec.REV = true;
    spec.BYLEX = true;
    return zrange(call, t, spec);
}

int cmd_ZREVRANGEBYLEX(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZREVRANGEBYLEX);
}
extern "C"
/**
 * ZRANK key member [WITHSCORE]
 *
 * redis's ZRANK: the position of one member counting from the lowest score, zero based,
 * and nil when the member is not in the set. It used to take two bounds and answer how
 * many members fell between them, which is a different question - ZFASTRANK still answers
 * that one, in constant time, and is the right command for it. See TODO 38.
 */
int ZRANK(caller& call, const arg_t& argv) {
    if (argv.size() < 3 || argv.size() > 4) {
        return call.wrong_arity();
    }
    bool withscore = false;
    if (argv.size() == 4) {
        auto opt = argv[3].to_string();
        for (auto& ch : opt) ch = (char) toupper((unsigned char) ch);
        if (opt != "WITHSCORE") {
            return call.syntax_error();
        }
        withscore = true;
    }
    auto c = argv[1];
    if (c.empty() || argv[2].empty()) {
        return call.wrong_arity();
    }
    // the member is stored encoded, the same way ZADD writes it - which is why zrange
    // hands it back through push_encoded_key. Comparing the raw argument against the
    // stored bytes never matches
    auto wanted_key = conversion::convert(argv[2]);
    art::value_type wanted = wanted_key.get_value();
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.read_locked(c);

    auto container = conversion::convert(c);
    query lq, pq;
    auto lower = lq->create(art::ts_ordered_map, {container});
    auto prefix = pq->create(art::ts_ordered_map, {container}, false);

    int64_t position = 0;
    bool found = false;
    art::value_type score{};
    art::iterator ai(t, lower);
    while (ai.ok()) {
        auto v = ai.key();
        if (!v.starts_with(prefix)) break;
        auto member = v.sub(prefix.size + numeric_key_size);
        if (member == wanted) {
            score = v.sub(prefix.size, numeric_key_size);
            found = true;
            break;
        }
        ++position;
        ai.next();
    }
    if (!found) {
        return call.push_null();
    }
    if (withscore) {
        call.start_array();
        call.push_ll(position);
        call.push_encoded_key(score);
        call.end_array();
        return call.ok();
    }
    return call.push_ll(position);
}
int cmd_ZRANK(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ZRANK);
}
extern "C"
int ZFASTRANK(caller& call, const arg_t& argv) {
    if (argv.size() != 4) {
        return call.wrong_arity();
    }
    barch::sharded_store kstore(call.kspace());
    auto t = kstore.write_locked(argv[1]);
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
    auto min_key = qlower.create(art::ts_ordered_map, {container, lower},false);
    auto max_key = qupper.create(art::ts_ordered_map, {container, upper}, false);
    if (max_key < min_key) {
        return call.push_ll(0);
    }

    art::iterator first(t, min_key);
    art::iterator last(t, max_key);

    int64_t rank = 0;
    if (first.ok() && last.ok()) {
        rank += last.key() == max_key ? 1 : 0;
        rank += first.fast_distance(last);
    }

    return call.push_ll(rank);
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

/* the ordered set commands as a RESP client sees them */
void register_ordered_api(function_map& r) {
    // the categories say what a command does, and authorisation refuses a caller who
    // lacks any bit the command declares - so a reader marked write demands a
    // permission it never uses. Only the forms that actually mutate are write here

    r["ZADD"] = {::ZADD,{"write","orderedset","data"}};
    r["ZREM"] = {::ZREM,{"write","orderedset","data"}};
    r["ZINCRBY"] = {::ZINCRBY,{"write","orderedset","data"}};
    r["ZRANGE"] = {::ZRANGE,{"read","orderedset","data"}};
    r["ZCARD"] = {::ZCARD,{"read","orderedset","data"}};
    r["ZCOUNT"] = {::ZCOUNT,{"read","orderedset","data"}};
    r["ZDIFF"] = {::ZDIFF,{"read","orderedset","data"}};
    r["ZDIFFSTORE"] = {::ZDIFFSTORE,{"write","orderedset","data"}};
    r["ZINTERSTORE"] = {::ZINTERSTORE,{"write","orderedset","data"}};
    r["ZREMRANGEBYSCORE"] = {::ZREMRANGEBYSCORE,{"write","orderedset","data"}};
    r["ZRANDMEMBER"] = {::ZRANDMEMBER,{"read","orderedset","data"}};
    r["ZSCORE"] = {::ZSCORE,{"read","orderedset","data"}};
    r["ZMSCORE"] = {::ZMSCORE,{"read","orderedset","data"}};
    r["ZUNION"] = {::ZUNION,{"read","orderedset","data"}};
    r["ZUNIONSTORE"] = {::ZUNIONSTORE,{"write","orderedset","data"}};
    r["ZINTERCARD"] = {::ZINTERCARD,{"read","orderedset","data"}};
    r["ZINTER"] = {::ZINTER,{"read","orderedset","data"}};
    r["ZPOPMIN"] = {::ZPOPMIN,{"write","orderedset","data"}};
    r["ZPOPMAX"] = {::ZPOPMAX,{"write","orderedset","data"}};
    r["ZMPOP"] = {::ZMPOP,{"write","orderedset","data"}};
    r["BZMPOP"] = {::BZMPOP,{"write","orderedset","data"}};
    r["ZREVRANGE"] = {::ZREVRANGE,{"read","orderedset","data"}};
    r["ZRANGEBYSCORE"] = {::ZRANGEBYSCORE,{"read","orderedset","data"}};
    r["ZREVRANGEBYSCORE"] = {::ZREVRANGEBYSCORE,{"read","orderedset","data"}};
    r["ZREMRANGEBYLEX"] = {::ZREMRANGEBYLEX,{"write","orderedset","data"}};
    r["ZRANGEBYLEX"] = {::ZRANGEBYLEX,{"read","orderedset","data"}};
    r["ZREVRANGEBYLEX"] = {::ZREVRANGEBYLEX,{"read","orderedset","data"}};
    r["ZRANK"] = {::ZRANK,{"read","orderedset","data"}};
    r["ZFASTRANK"] = {::ZFASTRANK,{"read","orderedset","data"}};
}
