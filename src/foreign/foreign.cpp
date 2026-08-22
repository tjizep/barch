#include "foreign.h"
#include "driver.h"
#include "sql.h"
#include "pool.h"

#include "conversion.h"
#include "dictionary_compressor.h"
#include "keys.h"
#include "keyspec.h"
#include "key_type.h"
#include "lzr_log.h"
#include "rpc/server.h"
#include "sharded_store.h"
#include "shard.h"
#include "statistics.h"

#include <cctype>
#include <chrono>
#include <vector>

namespace barch {
namespace foreign {

static std::string key_bytes(art::value_type key) {
    return {key.chars(), key.size};
}

static shard* as_shard(const shard_ptr& t) {
    return static_cast<shard*>(t.get());
}

static bool leaf_stops_fetch(const art::node_ptr& n) {
    return !n.null() && !n.cl()->expired();
}

static void release_inflight(const key_space_ptr& space, shard::foreign_flight& fl) {
    if (!fl.owns_inflight) return;
    fl.owns_inflight = false;
    if (space->foreign_inflight > 0)
        --space->foreign_inflight;
}

static void maybe_erase(const shard_ptr& t, const std::string& kstr,
                        const std::shared_ptr<shard::foreign_flight>& fl) {
    if (!fl) return;
    // a running fetch must stay in the map so a waiter timeout cannot drop it
    if (fl->resp_pending == 0 && fl->swig_waiters == 0 && fl->finished) {
        auto it = as_shard(t)->flights.find(kstr);
        if (it != as_shard(t)->flights.end() && it->second == fl)
            as_shard(t)->flights.erase(it);
    }
}

static int push_get_value(caller& call, const art::node_ptr& n) {
    auto cl = n.const_leaf();
    auto vt = cl->get_value();
    if (cl->is_compressed())
        vt = dictionary::decompress(vt);
    return call.push_vt(vt);
}

static int reply_after_wait(caller& call, art::value_type key, bool as_exists, bool dec_resp) {
    sharded_store store(call.kspace());
    std::string kstr = key_bytes(key);
    std::shared_ptr<shard::foreign_flight> fl;
    store.with_key_write(key, [&](const shard_ptr& t) {
        auto it = as_shard(t)->flights.find(kstr);
        if (it != as_shard(t)->flights.end())
            fl = it->second;
        if (dec_resp && fl && fl->resp_pending > 0)
            --fl->resp_pending;
        if (dec_resp && statistics::foreign_waiters > 0)
            --statistics::foreign_waiters;
        if (fl && fl->state == shard::foreign_flight::state::failed) {
            maybe_erase(t, kstr, fl);
            return;
        }
        maybe_erase(t, kstr, fl);
    });
    if (fl && fl->state == shard::foreign_flight::state::failed)
        return call.push_error(fl->error.empty() ? "FOREIGN failed" : fl->error.c_str());

    int r = call.ok();
    bool found = store.search(key, [&](const art::node_ptr& n) {
        if (as_exists)
            r = call.push_ll(1);
        else
            r = push_get_value(call, n);
    });
    if (found) return r;
    return as_exists ? call.push_ll(0) : call.push_null();
}

static void finish_fetch(key_space_ptr space, std::string kstr, uint64_t generation,
                         std::shared_ptr<shard::foreign_flight> fl, result res,
                         int64_t started) {
    auto elapsed = art::now() - started;
    if (elapsed > 100) {
        ++statistics::foreign_slow;
        barch::warn({"foreign query slow", space->get_canonical_name(),
                     "key_len", kstr.size(), "ms", elapsed});
    }

    art::value_type key{kstr};
    sharded_store store(space);
    shard_ptr wake;
    try {
    store.with_key_write(key, [&](const shard_ptr& t) {
        auto finish = [&] {
            fl->finished = true;
            fl->swig_cv.notify_all();
            release_inflight(space, *fl);
            maybe_erase(t, kstr, fl);
            wake = t;
        };
        if (fl->generation != generation || fl->state == shard::foreign_flight::state::cancelled) {
            ++statistics::foreign_cancelled;
            finish();
            return;
        }
        if (fl->state == shard::foreign_flight::state::failed) {
            finish();
            return;
        }
        auto leaf = t->local_leaf(key);
        if (leaf_stops_fetch(leaf)) {
            finish();
            return;
        }
        if (res.status == result::status::error) {
            fl->state = shard::foreign_flight::state::failed;
            fl->error = res.payload.empty() ? "FOREIGN failed" : res.payload;
            ++statistics::foreign_errors;
            finish();
            return;
        }
        const bool hashed = !space->opt_ordered_keys;
        if (res.status == result::status::missing) {
            uint64_t ttl_ms = space->missing_ttl ? space->missing_ttl * 1000 : 0;
            as_shard(t)->insert_cached_miss(key, ttl_ms, hashed);
            if (space->missing_ttl)
                repl::call({"FOREIGN_MISS", kstr, "EX", std::to_string(space->missing_ttl)});
            else
                repl::call({"FOREIGN_MISS", kstr});
        } else if (res.status == result::status::value) {
            art::key_options opts;
            opts.set_hashed(hashed);
            t->opt_rpc_insert(opts, key, art::value_type{res.payload}, true, [](const art::node_ptr&) {});
            repl::call({"SET", kstr, res.payload});
        }
        finish();
    });
    if (wake)
        wake->call_unblock(kstr);
    } catch (const std::exception& e) {
        fl->state = shard::foreign_flight::state::failed;
        fl->error = e.what();
        fl->finished = true;
        fl->swig_cv.notify_all();
        release_inflight(space, *fl);
        barch::err({"foreign write-back", space->get_canonical_name(), e.what()});
    }
}

static void run_fetch(key_space_ptr space, std::string kstr, uint64_t generation,
                      std::shared_ptr<shard::foreign_flight> fl) {
    driver* drv = nullptr;
    if (space->opt_foreign == key_space::foreign_kind::fake)
        drv = &fake_driver();
    else if (space->opt_foreign == key_space::foreign_kind::luau)
        drv = &luau_driver();
    else if (space->opt_foreign == key_space::foreign_kind::mysql)
        drv = &mysql_driver();
    else if (space->opt_foreign == key_space::foreign_kind::postgres)
        drv = &postgres_driver();
    ++statistics::foreign_queries;
    auto started = art::now();
    if (!drv) {
        finish_fetch(std::move(space), std::move(kstr), generation, std::move(fl),
                     {result::status::error, "FOREIGN no driver"}, started);
        return;
    }
    drv->fetch_async(space->get_canonical_name(), kstr, space->foreign_query_timeout_ms,
                     [space, kstr, generation, fl, started](result res) {
                         finish_fetch(space, kstr, generation, fl, std::move(res), started);
                     });
}

static int park_or_wait(caller& call, art::value_type key, bool as_exists) {
    auto space = call.kspace();
    std::string kstr = key_bytes(key);
    const int ctx = call.get_context();
    if (ctx == ctx_valkey || ctx == ctx_rpc)
        return call.push_error("FOREIGN not supported on this path");

    std::shared_ptr<shard::foreign_flight> fl;
    std::shared_ptr<shard::foreign_flight> start_fl;
    uint64_t start_gen = 0;
    bool created = false;
    bool overloaded = false;
    sharded_store store(space);
    store.with_key_write(key, [&](const shard_ptr& t) {
        auto leaf = t->local_leaf(key);
        if (leaf_stops_fetch(leaf))
            return;
        auto it = as_shard(t)->flights.find(kstr);
        if (it != as_shard(t)->flights.end()) {
            if (it->second->state == shard::foreign_flight::state::failed) {
                fl = it->second;
                return;
            }
            if (it->second->state == shard::foreign_flight::state::pending
                && !it->second->finished) {
                fl = it->second;
                ++statistics::foreign_coalesced;
            }
        }
        if (!fl) {
            if (space->foreign_inflight.load() >= space->foreign_max_inflight) {
                overloaded = true;
                ++statistics::foreign_overloaded;
                return;
            }
            fl = std::make_shared<shard::foreign_flight>();
            as_shard(t)->flights[kstr] = fl;
            ++space->foreign_inflight;
            created = true;
        }
        if (ctx == ctx_resp) {
            ++fl->resp_pending;
            ++statistics::foreign_waiters;
            size_t shard_i = space->get_shard_index(key);
            caller::keys_t blocks;
            blocks.emplace_back(kstr, shard_i);
            call.add_block(blocks, space->waiter_timeout_ms(),
                           [kstr, as_exists](caller& c, const caller::keys_t& keys) {
                               art::value_type k{kstr};
                               if (keys.empty()) {
                                   sharded_store st(c.kspace());
                                   st.with_key_write(k, [&](const shard_ptr& sh) {
                                       auto i = as_shard(sh)->flights.find(kstr);
                                       if (i == as_shard(sh)->flights.end()) return;
                                       if (i->second->resp_pending > 0)
                                           --i->second->resp_pending;
                                       if (statistics::foreign_waiters > 0)
                                           --statistics::foreign_waiters;
                                       maybe_erase(sh, kstr, i->second);
                                   });
                                   c.push_error("FOREIGN timeout");
                                   return;
                               }
                               reply_after_wait(c, k, as_exists, true);
                           });
        } else if (ctx == ctx_swig) {
            ++fl->swig_waiters;
            ++statistics::foreign_waiters;
            if (created && !fl->enqueued) {
                fl->enqueued = true;
                start_gen = fl->generation;
                start_fl = fl;
            }
        }
    });
    if (start_fl)
        enqueue([space, kstr, start_gen, start_fl]() { run_fetch(space, kstr, start_gen, start_fl); });

    if (overloaded)
        return call.push_error("FOREIGN overloaded");

    if (!fl) {
        // filled under the lock
        if (as_exists) {
            return store.exists(key) ? call.push_ll(1) : call.push_ll(0);
        }
        int r = call.ok();
        bool found = store.search(key, [&](const art::node_ptr& n) { r = push_get_value(call, n); });
        return found ? r : call.push_null();
    }
    if (fl->state == shard::foreign_flight::state::failed)
        return call.push_error(fl->error.empty() ? "FOREIGN failed" : fl->error.c_str());

    if (ctx == ctx_resp)
        return call.ok();

    // SWIG: enqueue here (no session race) and wait
    {
        std::unique_lock lk(fl->swig_mu);
        fl->swig_cv.wait_for(lk, std::chrono::milliseconds(space->waiter_timeout_ms()),
                             [&] { return fl->finished; });
    }
    store.with_key_write(key, [&](const shard_ptr& t) {
        if (fl->swig_waiters > 0)
            --fl->swig_waiters;
        if (statistics::foreign_waiters > 0)
            --statistics::foreign_waiters;
        maybe_erase(t, kstr, fl);
    });
    if (!fl->finished)
        return call.push_error("FOREIGN timeout");
    return reply_after_wait(call, key, as_exists, false);
}

int point_get(caller& call, art::value_type key) {
    return park_or_wait(call, key, false);
}

int point_exists(caller& call, art::value_type key) {
    return park_or_wait(call, key, true);
}

struct join_handle {
    enum class status { ready, failed, overloaded, waiting } status{status::ready};
    std::shared_ptr<shard::foreign_flight> fl;
    std::string kstr;
    std::string error;
};

static join_handle start_or_join(const key_space_ptr& space, art::value_type key) {
    join_handle h;
    h.kstr = key_bytes(key);
    std::shared_ptr<shard::foreign_flight> start_fl;
    uint64_t start_gen = 0;
    sharded_store store(space);
    store.with_key_write(key, [&](const shard_ptr& t) {
        auto leaf = t->local_leaf(key);
        if (leaf_stops_fetch(leaf))
            return;
        auto it = as_shard(t)->flights.find(h.kstr);
        if (it != as_shard(t)->flights.end()) {
            if (it->second->state == shard::foreign_flight::state::failed) {
                h.fl = it->second;
                h.status = join_handle::status::failed;
                h.error = it->second->error.empty() ? "FOREIGN failed" : it->second->error;
                return;
            }
            if (it->second->state == shard::foreign_flight::state::pending
                && !it->second->finished) {
                h.fl = it->second;
                ++statistics::foreign_coalesced;
            }
        }
        if (!h.fl) {
            if (space->foreign_inflight.load() >= space->foreign_max_inflight) {
                h.status = join_handle::status::overloaded;
                ++statistics::foreign_overloaded;
                return;
            }
            h.fl = std::make_shared<shard::foreign_flight>();
            as_shard(t)->flights[h.kstr] = h.fl;
            ++space->foreign_inflight;
        }
        ++h.fl->swig_waiters;
        ++statistics::foreign_waiters;
        h.status = join_handle::status::waiting;
        if (!h.fl->enqueued) {
            h.fl->enqueued = true;
            start_gen = h.fl->generation;
            start_fl = h.fl;
        }
    });
    if (start_fl)
        enqueue([space, kstr = h.kstr, start_gen, start_fl]() {
            run_fetch(space, kstr, start_gen, start_fl);
        });
    return h;
}

static void release_join(const key_space_ptr& space, join_handle& h) {
    if (!h.fl) return;
    art::value_type key{h.kstr};
    sharded_store store(space);
    store.with_key_write(key, [&](const shard_ptr& t) {
        if (h.fl->swig_waiters > 0)
            --h.fl->swig_waiters;
        if (statistics::foreign_waiters > 0)
            --statistics::foreign_waiters;
        maybe_erase(t, h.kstr, h.fl);
    });
    h.fl.reset();
}

static int wait_joins(caller& call, const key_space_ptr& space, std::vector<join_handle>& hs) {
    auto deadline = art::now() + static_cast<int64_t>(space->waiter_timeout_ms());
    std::string err;
    bool timed_out = false;
    bool overloaded = false;
    for (auto& h : hs) {
        if (h.status == join_handle::status::overloaded) {
            overloaded = true;
            continue;
        }
        if (h.status == join_handle::status::failed) {
            if (err.empty())
                err = h.error.empty() ? "FOREIGN failed" : h.error;
            continue;
        }
        if (h.status != join_handle::status::waiting || !h.fl)
            continue;
        auto left = deadline - art::now();
        if (left < 0) left = 0;
        {
            std::unique_lock lk(h.fl->swig_mu);
            h.fl->swig_cv.wait_for(lk, std::chrono::milliseconds(left),
                                   [&] { return h.fl->finished; });
        }
        if (!h.fl->finished)
            timed_out = true;
        else if (h.fl->state == shard::foreign_flight::state::failed && err.empty())
            err = h.fl->error.empty() ? "FOREIGN failed" : h.fl->error;
        release_join(space, h);
    }
    for (auto& h : hs)
        if (h.fl)
            release_join(space, h);
    if (overloaded) {
        call.push_error("FOREIGN overloaded");
        return -1;
    }
    if (timed_out) {
        call.push_error("FOREIGN timeout");
        return -1;
    }
    if (!err.empty()) {
        call.push_error(err.c_str());
        return -1;
    }
    return call.ok();
}

int mget(caller& call, const arg_t& argv) {
    const int ctx = call.get_context();
    if (ctx == ctx_valkey || ctx == ctx_rpc)
        return call.push_error("FOREIGN not supported on this path");
    auto space = call.kspace();
    sharded_store store(space);
    std::vector<join_handle> hs;
    for (size_t arg = 1; arg < argv.size(); ++arg) {
        if (key_ok(argv[arg]) != 0)
            continue;
        auto converted = call.kspace()->encode_key(argv[arg]);
        auto key = converted.get_value();
        bool found = store.search(key, [](const art::node_ptr&) {});
        if (!found)
            hs.push_back(start_or_join(space, key));
    }
    int w = wait_joins(call, space, hs);
    if (w != 0)
        return w;
    call.start_array();
    for (size_t arg = 1; arg < argv.size(); ++arg) {
        if (key_ok(argv[arg]) != 0) {
            call.push_null();
            continue;
        }
        auto converted = call.kspace()->encode_key(argv[arg]);
        auto key = converted.get_value();
        store.with_key_read(key, [&](const shard_ptr& t) {
            art::node_ptr r = t->search(key);
            if (r.null()) {
                call.push_null();
            } else {
                call.push_vt(r.const_leaf()->get_value());
            }
        });
    }
    call.end_array();
    return call.ok();
}

int exists_many(caller& call, const arg_t& argv) {
    const int ctx = call.get_context();
    if (ctx == ctx_valkey || ctx == ctx_rpc)
        return call.push_error("FOREIGN not supported on this path");
    auto space = call.kspace();
    sharded_store store(space);
    std::vector<join_handle> hs;
    // exists() asks about the plain key and kind_of_container asks about the three
    // container lead bytes, so neither can answer for the other - but the pass that
    // counts used to repeat both for every argument, including the ones this pass has
    // already found. Keep what was learned instead. A key that is nowhere cost four
    // lock-and-walk probes here and four more below to say the same thing twice.
    struct probed {
        conversion::comparable_key key{};
        bool present{false};
    };
    std::vector<probed> probes;
    probes.reserve(argv.size() - 1);
    for (size_t i = 1; i < argv.size(); ++i) {
        if (key_ok(argv[i]) != 0)
            return call.key_check_error(argv[i]);
        probed p{call.kspace()->encode_key(argv[i]), false};
        p.present = store.exists(p.key.get_value())
                    || barch::kind_of_container(store, argv[i]) != barch::container_kind::none;
        probes.push_back(p);
        if (!p.present)
            hs.push_back(start_or_join(space, probes.back().key.get_value()));
    }
    int w = wait_joins(call, space, hs);
    if (w != 0)
        return w;
    int64_t found = 0;
    for (size_t i = 0; i < probes.size(); ++i) {
        // only a key that was fetched can have arrived since; one that was already here
        // stays here, and the reply is a snapshot either way
        if (probes[i].present
            || store.exists(probes[i].key.get_value())
            || barch::kind_of_container(store, argv[i + 1]) != barch::container_kind::none)
            ++found;
    }
    return call.push_ll(found);
}

void kick(const key_space_ptr& space, const std::string& kstr) {
    if (!space || !space->has_foreign())
        return;
    art::value_type key{kstr};
    std::shared_ptr<shard::foreign_flight> start_fl;
    uint64_t start_gen = 0;
    sharded_store store(space);
    store.with_key_write(key, [&](const shard_ptr& t) {
        auto it = as_shard(t)->flights.find(kstr);
        if (it == as_shard(t)->flights.end())
            return;
        auto fl = it->second;
        auto leaf = t->local_leaf(key);
        if (leaf_stops_fetch(leaf) || fl->state == shard::foreign_flight::state::failed
            || fl->state == shard::foreign_flight::state::cancelled) {
            fl->finished = true;
            fl->swig_cv.notify_all();
            t->call_unblock(kstr);
            release_inflight(space, *fl);
            maybe_erase(t, kstr, fl);
            return;
        }
        if (fl->enqueued)
            return;
        if (fl->state == shard::foreign_flight::state::pending) {
            fl->enqueued = true;
            start_gen = fl->generation;
            start_fl = fl;
        }
    });
    if (start_fl)
        enqueue([space, kstr, start_gen, start_fl]() { run_fetch(space, kstr, start_gen, start_fl); });
}

int FAKE(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    std::string kind = argv[1].to_string();
    if (kind != "FAKE" && kind != "fake")
        return call.syntax_error();
    auto space = call.kspace();
    if (space->opt_foreign != key_space::foreign_kind::fake)
        return call.push_error("FOREIGN FAKE is off");
    std::string sub = argv[2].to_string();
    for (auto& c : sub) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (sub == "SET") {
        if (argv.size() != 5) return call.wrong_arity();
        auto converted = call.kspace()->encode_key(argv[3]);
        std::string k = key_bytes(converted.get_value());
        std::string v = argv[4].to_string();
        std::lock_guard lk(space->fake_mu);
        space->fake_source[k] = std::move(v);
        return call.push_simple("OK");
    }
    if (sub == "DEL") {
        if (argv.size() != 4) return call.wrong_arity();
        auto converted = call.kspace()->encode_key(argv[3]);
        std::string k = key_bytes(converted.get_value());
        std::lock_guard lk(space->fake_mu);
        space->fake_source.erase(k);
        return call.push_simple("OK");
    }
    if (sub == "FAIL") {
        if (argv.size() != 4) return call.wrong_arity();
        std::string on = argv[3].to_string();
        std::lock_guard lk(space->fake_mu);
        space->fake_fail = (on == "ON" || on == "on" || on == "1");
        return call.push_simple("OK");
    }
    if (sub == "DELAY") {
        if (argv.size() != 4) return call.wrong_arity();
        uint64_t ms = 0;
        conversion::to(argv[3], ms);
        std::lock_guard lk(space->fake_mu);
        space->fake_delay_ms = ms;
        return call.push_simple("OK");
    }
    if (sub == "QUERIES") {
        if (argv.size() != 3) return call.wrong_arity();
        return call.push_ll(static_cast<int64_t>(space->fake_queries.load()));
    }
    if (sub == "PARTS") {
        if (argv.size() != 4) return call.wrong_arity();
        auto converted = space->encode_key(argv[3]);
        auto parts = key_parts(key_bytes(converted.get_value()), space.get());
        call.start_array();
        for (auto& p : parts)
            call.push_string(p);
        return call.end_array();
    }
    if (sub == "RESET") {
        if (argv.size() != 3) return call.wrong_arity();
        std::lock_guard lk(space->fake_mu);
        space->fake_source.clear();
        space->fake_fail = false;
        space->fake_delay_ms = 0;
        space->fake_queries = 0;
        return call.push_simple("OK");
    }
    return call.syntax_error();
}

int MISS(caller& call, const arg_t& argv) {
    if (argv.size() != 2 && argv.size() != 4)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    uint64_t ttl_ms = 0;
    if (argv.size() == 4) {
        std::string ex = argv[2].to_string();
        for (auto& c : ex) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        if (ex != "EX")
            return call.syntax_error();
        long long given = 0;
        if (!conversion::to_ll(argv[3], given))
            return call.push_error("value is not an integer or out of range");
        int64_t deadline = 0;
        if (given <= 0 || !art::expiry_ms(given, true, true, deadline))
            return call.push_error("invalid expire time in 'foreign_miss' command");
        auto left = deadline - art::now();
        ttl_ms = left > 0 ? static_cast<uint64_t>(left) : 1;
    }
    auto converted = call.kspace()->encode_key(k);
    auto key = converted.get_value();
    const bool hashed = !call.kspace()->opt_ordered_keys;
    sharded_store store(call.kspace());
    store.with_key_write(key, [&](const shard_ptr& t) {
        as_shard(t)->insert_cached_miss(key, ttl_ms, hashed);
    });
    return call.push_simple("OK");
}

}
}
