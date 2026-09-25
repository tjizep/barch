//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "keyspace_api.h"
#include <set>
#include <map>
#include <mutex>
#include "ids.h"
#include <algorithm>
#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>
#include <vector>

#include "barch_apis.h"
#include "caller.h"
#include "vk_caller.h"
#include "module.h"
#include "conversion.h"
#include "version.h"
#include "glob.h"
#include "keys.h"
#include "art/art.h"
#include "art/iterator.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "sharded_store.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"
#include "statistics.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "auth_api.h"
#include "rpc/server.h"
#include "rpc/restarter.h"
#include "rpc/redis_parser.h"

extern "C" {
#include "../external/include/valkeymodule.h"
}

extern "C" {

static size_t save(caller& call) {
    std::atomic<size_t> errors = 0;
    barch::sharded_store store(call.kspace());
    // a stateful method can move a key between shards while they are being
    // written. holding every shard shared stops that for the snapshot, so a
    // key cannot land in two files or in neither. hash sharding is a function
    // of the key and does not pay. save() takes its own shared latch on a
    // worker thread, which is allowed to share with this one.
    barch::sharded_store::read_guard held;
    if (store.space()->is_stateful_sharding()) {
        held = store.lock_space_read();
    }
    /*
     * Where the log is before anything is saved. The shards are saved one after
     * another while writes carry on, so a write to a shard that's already been
     * saved is in the log and in no file. The checkpoint below covers this mark
     * and not "everything so far", or the trim right after it would drop those
     * writes and a crash would lose them - TODO 452.
     */
    const auto& save_log = store.space()->get_change_log();
    const uint64_t saved_through = save_log ? save_log->mark() : 0;
    store.each_shard_parallel([&](const barch::shard_ptr& shard) {
        if (!shard->save(true)) {
            barch::err({"could not save", shard->get_shard_number()});
            ++errors;
        }
    });
    save_auth();
    /*
     * The checkpoint goes here and nowhere else - TODO 355.
     *
     * It says every write up to `saved_through` is in the shard files, so it can
     * only be written when every shard of the space is on disk. That is true here and
     * only here: the interval save in shard.cpp saves one shard at a time, so a
     * checkpoint after one of those would claim the whole space was saved when
     * fifteen of seventeen shards had not been.
     *
     * And only when nothing failed. A checkpoint after a partial save is a lie
     * that survives a crash, and a replay believing it skips records that are
     * still the only copy of what they describe.
     *
     * The trim right after is what bounds the file: everything the checkpoint
     * covers is in the shard files now, so it does not need to be in the log
     * as well.
     */
    if (errors == 0) {
        if (save_log) {
            try {
                save_log->checkpoint(store.space()->space_name(), saved_through);
                save_log->trim_to_last_checkpoint();
            } catch (const std::exception& e) {
                // the save worked; the log bookkeeping did not, and saying so is
                // better than failing a save that is already on disk
                barch::err({"saved, but could not checkpoint the change log:", e.what()});
            }
        }
    }
    return errors;
}
/* B.KSPACE
    - Key space operators:
    - `KSPACE DEPENDS {depend[e|a]nt key space} ON {source key space name} [STATIC]`
      Let a key space depend on a list of one or more source key spaces (dependant missing keys are resolved in source)
      keys are added to the dependent and not propagated to the source
    - `KSPACE RELEASE {depend[e|a]nt key space} FROM {source key space name}`
     release a source from a dependent
    - `KSPACE DEPENDANTS {key space name}`
     list the dependants
    - `KSPACE DROP {key space name}`
         list the dependants
    - `KSPACE MERGE {depend[e|a]nt key space} [TO {source key space name}]`
      Merge a dependent named key space to its sources or any other random key space
    - `KSPACE OPTION [SET|GET] ORDERED [ON|OFF]` sets the current key space to ordered or unordered, option is saved in key space shards
    - `KSPACE OPTION [SET|GET] LRU [ON|OFF|VOLATILE]` sets the current key space to evict lru
    - `KSPACE OPTION [SET|GET] RANDOM [ON|OFF|VOLATILE]` sets the current key space to evict randomly
    - `KSPACE OPTION GET FOREIGN|MISSING_TTL|FOREIGN_TIMEOUT|FOREIGN_QUERY_TIMEOUT|FOREIGN_INFLIGHT|FOREIGN_POOL_MAX_AGE` reports the foreign-source options read when the space was built. SET of those names is a syntax error.
    - `KSPACE OPTION GET FUNCTION_SLICE|FUNCTION_DEADLINE` reports what a stored function gets: the instructions it runs before yielding, and the wall clock bound on the whole call. Both answer the server setting unless the space overrides it.
    - `KSPACE EXIST {key space name} return `1` if space exists else `0`
 */
int KSPACE(caller& call, const arg_t& argv) {
    if (argv.size() < 3) {
        return call.wrong_arity();
    }

    art::kspace_spec parser(argv);
    if (parser.parse_options() != 0) {
        return call.syntax_error();
    }
    if (parser.is_acl) {
        /*
         * `KSPACE ACL [KSNAME] SETUSER alice -read -write +function`
         *
         * The rights a user holds in one key space, as the differences from their
         * global ones. A space with no rule leaves them exactly as they are, so
         * nothing that works today changes. See TODO 135.
         */
        std::string space = parser.name.empty()
            ? call.kspace()->get_canonical_name() : parser.name;
        // the arguments from the verb onwards read as an ordinary ACL, so the same
        // parser handles them - it is the same vocabulary and should stay so
        arg_t tail;
        tail.push_back(art::value_type{"ACL"});
        for (size_t i = parser.acl_at; i < argv.size(); ++i) {
            tail.push_back(argv[i]);
        }
        art::acl_spec spec(tail);
        if (spec.parse_options() != 0) {
            return call.syntax_error();
        }
        if (spec.is_filter) {
            return call.push_error("ACL key patterns are not supported");
        }
        if (spec.is_secret) {
            // a secret belongs to the user, not to their rights in one space
            return call.push_error("a secret cannot be set per key space");
        }
        if (spec.get) {
            auto all = barch::read_space_overrides(spec.user);
            auto it = all.find(space);
            call.start_array();
            if (it != all.end()) {
                for (const auto& c : it->second) {
                    call.push_values({"$" + c.first, c.second ? "true" : "false"});
                }
            }
            return call.end_array();
        }
        if (spec.del) {
            barch::write_space_overrides(spec.user, space, {});
            return call.push_simple("OK");
        }
        auto& valid = get_category_map();
        for (const auto& c : spec.cat) {
            if (c.first != "all" && !valid.count(c.first)) {
                return call.push_error("ACL category not found");
            }
        }
        barch::write_space_overrides(spec.user, space, spec.cat);
        return call.push_simple("OK");
    }
    if (parser.is_exist) {
        return call.push_bool(barch::keyspace_exists(parser.name));
    }
    if (parser.is_depends) {
        auto source = barch::get_keyspace(parser.source);
        auto dependent = barch::get_keyspace(parser.dependant);
        if (dependent->get_shard_count() != source->get_shard_count()) {
            return call.push_error("source and dependant shard counts do not match");
        }
        // canonical order, not the order they are named in - see keyspace_locks.h.
        // This and RELEASE below used to take the same pair in opposite orders
        ks_two held(source, ks_mode::shared, dependent, ks_mode::unique);
        dependent->depends(source);
        return call.push_simple("OK");
    }

    if (parser.is_dependants) {
        auto dependent = barch::get_keyspace(parser.dependant); // this will throw if parameter is wrong
        auto source = dependent->source();
        if (source) {
            return call.push_string(source->get_canonical_name());
        }
        return call.push_null();
    }

    if (parser.is_merge) {
        if (parser.is_merge_default) {
            merge_options opts;
            opts.set_compressed(parser.is_merge_compress);
            call.kspace()->merge(opts);
            return call.push_simple("OK");
        }
        auto to = barch::get_keyspace(parser.source);
        auto from = barch::get_keyspace(parser.dependant);
        barch::key_space_ptr old = nullptr;
        if (to == from->source()) {
            old = to;
            from->depends(nullptr);
        }
        ks_two held(from, ks_mode::shared, to, ks_mode::unique);

        // COMPRESS was parsed and then dropped here - TODO 451
        merge_options opts;
        opts.set_compressed(parser.is_merge_compress);
        from->merge(to, opts);
        if (old) {
            from->depends(to);
        }
        return call.push_simple("OK");
    }

    if (parser.is_release) {
        auto dependent = barch::get_keyspace(parser.dependant);
        auto source = dependent->source();
        if (!source || source->get_canonical_name() != parser.source) {
            if (!parser.source.empty())
                return call.push_error("Invalid source keyspace name");
        }
        ks_two held(dependent, ks_mode::unique, source, ks_mode::shared);
        dependent->depends(nullptr);
        return call.push_simple("OK");
    }
    if (parser.is_drop) {
        auto source = parser.source.empty() ? call.kspace() : barch::get_keyspace(parser.source);
        {
            ks_unique shl(source);
            source->depends(nullptr);
            barch::sharded_store dropped(source);
            dropped.each_shard([](const barch::shard_ptr& shrd) {
                shrd->opt_drop_on_release = true;
            });
        }
        // unload takes the shard locks again to fail foreign flights. holding
        // them here is EDEADLK on a mutex that is not recursive.
        source = nullptr;
        if (barch::unload_keyspace(parser.source))
            return call.push_simple("OK");
    }

    if (parser.is_option && parser.is_get) {
        auto spc = call.kspace();
        ks_shared ul(spc);
        if (parser.name == "ORDERED") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_ordered_keys);
            return 0;
        }
        if (parser.name == "HYBRID") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_hybrid_keys);
            return 0;
        }
        if (parser.name == "LRU") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_evict_all_keys_lru);
            return 0;
        }
        if (parser.name == "RANDOM") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_evict_all_keys_random);
            return 0;
        }
        if (parser.name == "FOREIGN") {
            return call.push_vt(art::value_type{spc->foreign_kind_name()});
        }
        if (parser.name == "MISSING_TTL") {
            return call.push_ll(static_cast<int64_t>(spc->missing_ttl));
        }
        if (parser.name == "FOREIGN_TIMEOUT") {
            return call.push_ll(static_cast<int64_t>(spc->waiter_timeout_ms()));
        }
        if (parser.name == "FOREIGN_QUERY_TIMEOUT") {
            return call.push_ll(static_cast<int64_t>(spc->foreign_query_timeout_ms));
        }
        if (parser.name == "FUNCTION_SLICE") {
            return call.push_ll(static_cast<int64_t>(spc->function_slice()));
        }
        if (parser.name == "FUNCTION_DEADLINE") {
            return call.push_ll(static_cast<int64_t>(spc->function_deadline()));
        }
        if (parser.name == "FOREIGN_INFLIGHT") {
            call.start_array();
            call.push_ll(0);
            call.push_ll(static_cast<int64_t>(spc->foreign_max_inflight));
            return call.end_array();
        }
        if (parser.name == "FOREIGN_POOL_MAX_AGE") {
            return call.push_ll(static_cast<int64_t>(spc->pool_max_age_ms()));
        }
        if (parser.name == "KEY_SPLIT") {
            return call.push_vt(art::value_type{spc->key_split});
        }
        return call.push_simple("OK");
    }

    if (parser.is_option && parser.is_set) {
        auto spc = call.kspace();
        barch::sharded_store store(spc);
        ks_unique ul(spc);
        if (parser.name == "ORDERED") {
            bool on = parser.value == "ON";
            spc->opt_ordered_keys = on;
            store.each_shard([on](const barch::shard_ptr& shrd) { shrd->opt_ordered_keys = on; });
            return call.push_simple("OK");
        }
        if (parser.name == "HYBRID") {
            bool on = parser.value == "ON";
            spc->opt_hybrid_keys = on;
            store.each_shard([on](const barch::shard_ptr& shrd) {
                shrd->opt_hybrid_keys = on;
                shrd->apply_hybrid_keys();
            });
            return call.push_simple("OK");
        }
        if (parser.name == "LRU") {
            bool on = parser.value == "ON";
            bool evict_volatile = parser.value == "VOLATILE";
            store.each_shard([on, evict_volatile](const barch::shard_ptr& shrd) {
                shrd->opt_evict_all_keys_lru = on;
                shrd->opt_evict_volatile_keys_lru = evict_volatile;
                shrd->apply_lru_options();
            });
            return call.push_simple("OK");
        }
        if (parser.name == "RANDOM") {
            bool on = parser.value == "ON";
            bool evict_volatile = parser.value == "VOLATILE";
            store.each_shard([on, evict_volatile](const barch::shard_ptr& shrd) {
                shrd->opt_evict_all_keys_random = on;
                shrd->opt_evict_volatile_keys_random = evict_volatile;
            });
            return call.push_simple("OK");
        }

    }

    return call.push_null();
}
int cmd_KSPACE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KSPACE);
}
/* B.USE
 * @return OK.
 */
int USE(caller& call, const arg_t& argv) {
    if (argv.size() == 1) {
        call.use("");
        return call.push_simple("OK");
    }
    if (argv.size() != 2) {
        return call.wrong_arity();
    }
    auto name = argv[1].to_string();
    if (name == "0") {
        call.use("");
    }else {
        call.use(name);
    }
    return call.push_simple("OK");
}
int cmd_USE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, USE);
}
/* SELECT <index> | <name>
 *
 * redis's SELECT takes a database number. This used to be another name for USE, so
 * `SELECT 1` gave a key space literally called "1" - which works, but means the numbered
 * databases a redis client believes it is switching between are only spaces whose names
 * happen to be digits, and `SELECT 0` matched the default space by a special case.
 *
 * A number is now a database: 0 is the default space, and n above zero is the space named
 * by `db_number_prefix` followed by the number - `db1`, `db2`, unless the prefix is
 * configured otherwise. A name still selects that space, which barch has always allowed
 * here, so this is a superset of redis rather than a departure and nothing that used the
 * name form has to change. USE remains the command that only takes a name.
 *
 * Two things follow from key spaces being named where redis's databases are numbered, and
 * both are written up in the docs rather than hidden here. A space whose name is a bare
 * number cannot be reached through SELECT, because the number is read as a database - use
 * USE for that. And there is no fixed count of databases: redis refuses SELECT 16 by
 * default, while here `SELECT 999` brings that space into being like any other.
 * See TODO 38.
 */
int SELECT(caller& call, const arg_t& argv) {
    if (argv.size() != 2) {
        return call.wrong_arity();
    }
    long long index = 0;
    if (conversion::to_ll(argv[1], index)) {
        if (index < 0) {
            return call.push_error("DB index is out of range");
        }
        if (index == 0) {
            call.use("");
        } else {
            // the prefix is configurable - `CONFIG SET db_number_prefix ...` - because
            // which name a number maps to is a choice barch has to make and redis does
            // not. Setting it to "" gives the pre-existing behaviour, where SELECT 1
            // selects a space literally named "1"
            call.use(barch::get_db_number_prefix() + std::to_string(index));
        }
        return call.push_simple("OK");
    }
    // not a number, so barch's own form: a key space chosen by name. This is a superset
    // of what redis accepts rather than a departure from it, and spacethreadtest.py
    // exercises it on purpose - "Yes! we can select strings too"
    call.use(argv[1].to_string());
    return call.push_simple("OK");
}
int cmd_SELECT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SELECT);
}
int UNLOAD(caller& call, const arg_t& argv) {
    if (argv.size() == 1) {
        barch::unload_keyspace("");
        return call.push_simple("OK");
    }
    if (argv.size() != 2) {
        return call.wrong_arity();
    }
    barch::unload_keyspace(argv[1].to_string());
    return call.push_simple("OK");
}
int cmd_UNLOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, UNLOAD);
}
int SPACES(caller& call, const arg_t& argv) {

    if (argv.size() == 1) {
        call.start_array();
        barch::all_spaces([&call](const std::string& name, const barch::key_space_ptr& space) {
            uint64_t size = 0;
            barch::sharded_store store(space);
            store.each_shard([&size](const barch::shard_ptr& s) { size += s->get_size(); });
            call.push_values({name,size});
        });
        call.end_array();
    }else {
        return KSPACE(call, argv);
    }
    return call.ok();
}
int cmd_SPACES(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SPACES);
}
/* B.SIZE
 * @return the size or o.k.a. key count.
 */
/**
 * The number of keys stored, which is deliberately not redis's DBSIZE.
 *
 * redis counts one per name. barch counts one per stored key, and a collection stores a
 * key per entry, so a hash with twenty fields weighs twenty here and one there. KEYS and
 * SCAN were brought into line with redis and report names (DONE 51); this was not, and
 * the difference is worth stating rather than hiding.
 *
 * The reason is what it would cost. This reads counters the shards already keep, so it
 * answers without walking anything. Counting names instead means either a full walk of
 * the key space on every call, or a second set of counters maintained on every container
 * write - and barch's storage is different enough from redis's that the number would
 * still not mean the same thing. A caller that wants names can count what KEYS answers.
 */
int SIZE(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    auto size = 0ll;
    barch::sharded_store store(call.kspace());
    // get_size only reads counters, and read_lock still takes the source chain shared,
    // which get_size recurses into
    store.each_shard_read([&](const barch::shard_ptr& t) {
        size += (int64_t) t->get_size();
    });
    size += call.kspace()->hash_buf_size();
    return call.push_ll(size);
}
int cmd_SIZE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SIZE);
}
/* B.SAVE
 * saves the data to files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int SAVE(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    size_t errors = save(call);
    return errors ? call.push_error("some shards not saved"): call.push_simple("OK");
}
int cmd_SAVE(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SAVE);
}
int SAVEALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    // hold every stateful space shared, in canonical name order, so a key
    // cannot move between two of that space's files while they are being
    // written. hash-sharded spaces are walked without the lock. save() takes
    // its own shared latch on each shard, which is allowed to share with these.
    heap::vector<barch::key_space_ptr> stateful;
    barch::all_spaces([&](const std::string&, const barch::key_space_ptr& ks) {
        if (ks && ks->is_stateful_sharding()) stateful.push_back(ks);
    });
    std::sort(stateful.begin(), stateful.end(),
              [](const barch::key_space_ptr& a, const barch::key_space_ptr& b) {
                  return a->get_canonical_name() < b->get_canonical_name();
              });
    std::vector<barch::sharded_store::read_guard> held;
    held.reserve(stateful.size());
    for (const auto& ks : stateful) {
        barch::sharded_store store(ks);
        held.push_back(store.lock_space_read());
    }
    /*
     * Each space's log position before anything is saved, so its checkpoint
     * covers only what the files can hold - TODO 452, the same as save() above.
     * A space that turns up after this has no mark and gets no checkpoint.
     */
    std::map<std::string, uint64_t> marks;
    barch::all_spaces([&](const std::string&, const barch::key_space_ptr& ks) {
        if (ks && ks->get_change_log())
            marks[ks->space_name()] = ks->get_change_log()->mark();
    });
    /*
     * Which spaces saved cleanly, so only those get a checkpoint - TODO 355.
     * This used to ignore the result of every save; a checkpoint has to know,
     * because it is a claim about the file that save produced.
     */
    std::mutex failed_mut;
    std::set<std::string> failed;
    barch::all_shards([&](auto& shard) {
        if (!shard->save(true)) {
            std::lock_guard lock(failed_mut);
            failed.insert(shard->space_name());
        }
    });
    barch::all_spaces([&](const std::string&, const barch::key_space_ptr& ks) {
        if (!ks)
            return;
        const auto& change_log = ks->get_change_log();
        if (!change_log || failed.count(ks->space_name()))
            return;
        const auto mark = marks.find(ks->space_name());
        if (mark == marks.end())
            return;
        try {
            change_log->checkpoint(ks->space_name(), mark->second);
            change_log->trim_to_last_checkpoint();
        } catch (const std::exception& e) {
            barch::err({"saved, but could not checkpoint the change log for",
                        ks->space_name(), "-", e.what()});
        }
    });

    return call.push_simple("OK");
}
int cmd_SAVEALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SAVEALL);
}
int SIZEALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    uint64_t size = 0;

    barch::all_shards([&](auto& shard) {
        size += shard->get_size();
    });

    return call.push_int(size);
}
int cmd_SIZEALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SIZEALL);
}
int CLEAR(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    barch::sharded_store store(call.kspace());
    store.each_shard([](const barch::shard_ptr& shard) { shard->clear(); });
    // the id counter is a key, so clearing the space reset it - a block cached from
    // before would hand out numbers the counter is about to hand out again
    barch::forget_sequences(call.kspace()->get_canonical_name());

    return call.push_simple("OK");
}
int cmd_CLEAR(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int CLEARALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    /*
     * Every space but `configuration`, which is not data - see TODO 149.
     *
     * It holds each space's foreign settings, key_split and shard count, and the
     * global stored functions, so clearing it with the caches would silently
     * unconfigure the server. FLUSHDB still clears the selected space, so someone who
     * means to clear configuration can `USE configuration` and say so.
     */
    barch::all_spaces([](const std::string& name, const barch::key_space_ptr& ks) {
        if (name == "configuration" || name == "configuration_")
            return;
        for (auto& shard : ks->get_shards()) {
            shard->clear();
        }
        barch::forget_sequences(ks->get_canonical_name());
    });


    return call.push_simple("OK");
}
int cmd_CLEARALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEARALL);
}
int KSOPTIONS(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    if (argv[1] == "SET") {
        if (argv[2] == "UNORDERED") {
            auto spc = call.kspace();
            spc->opt_ordered_keys = false;
            barch::sharded_store store(spc);
            store.each_shard([](const barch::shard_ptr& shard) { shard->opt_ordered_keys = false; });
            return call.push_simple("OK");
        }
        if (argv[2] == "ORDERED") {
            auto spc = call.kspace();
            spc->opt_ordered_keys = true;
            barch::sharded_store store(spc);
            store.each_shard([](const barch::shard_ptr& shard) { shard->opt_ordered_keys = true; });
            return call.push_simple("OK");
        }
        if (argv[2] == "HYBRID") {
            auto spc = call.kspace();
            ks_unique ul(spc);
            barch::sharded_store store(spc);
            spc->opt_hybrid_keys = true;
            store.each_shard([](const barch::shard_ptr& shard) {
                shard->opt_hybrid_keys = true;
                shard->apply_hybrid_keys();
            });
            return call.push_simple("OK");
        }
    }


    return call.push_error("Unknown option");
}
int cmd_KSOPTIONS(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KSOPTIONS);
}
int BEGIN(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    // every shard's write latch at once, in the one lock order, so the whole
    // space begins at a single moment. One shard at a time let a write reach a
    // later shard after an earlier one had already begun - TODO 416
    auto spc = call.kspace();
    ks_unique held(spc);
    barch::sharded_store store(spc);
    store.each_shard([](const barch::shard_ptr& t) { t->begin_holding_lock(); });
    // an answer, like COMMIT and ROLLBACK give - ok() sent none, which RESP read as nil
    return call.push_simple("OK");
}
int cmd_BEGIN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, BEGIN);
}
int COMMIT(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    auto ks = call.kspace();
    barch::sharded_store store(call.kspace());
    store.each_shard([](const barch::shard_ptr& t) { t->commit(); });
    return call.push_simple("OK");
}
int cmd_COMMIT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, COMMIT);
}
int ROLLBACK(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    store.each_shard([](const barch::shard_ptr& t) { t->rollback(); });
    return call.push_simple("OK");
}
int cmd_ROLLBACK(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ROLLBACK);
}
}

/* B.KSRESIDENT [ALL]
 * @return what a space's arenas cost in address space and in RAM right now.
 */
/**
 * Mapped bytes against resident bytes, per space, split by arena kind - TODO 340.
 *
 * The two are nearly the same number for an anonymous arena and come apart for a
 * file backed one, which is the point of `<space>.arena_dir`: those pages are
 * written to the file and can be dropped again, so the space is bounded by the
 * device rather than by RAM, and `resident` says how much of it is costing RAM at
 * the moment it was asked. Without this there was no way to see that - the only
 * resident number barch reported was one figure for the whole process, in INFO.
 *
 * A command rather than an INFO section on purpose. `resident` comes from mincore,
 * which walks page tables, and a space has two arenas per shard - so it is worth
 * answering when somebody asks and not worth putting in front of whatever polls
 * INFO every second.
 *
 * Each shard is measured under its own read lock, because the alternative is
 * reading `page_data` and its length while another thread is in the middle of
 * mremap'ing them.
 */
struct arena_residency {
    uint64_t leaves_mapped = 0, leaves_resident = 0;
    uint64_t nodes_mapped = 0, nodes_resident = 0;
    uint64_t arenas = 0, file_backed = 0;
    [[nodiscard]] uint64_t mapped() const { return leaves_mapped + nodes_mapped; }
    [[nodiscard]] uint64_t resident() const { return leaves_resident + nodes_resident; }
};

static arena_residency measure_residency(const barch::key_space_ptr& space) {
    arena_residency t{};
    if (!space)
        return t;
    barch::sharded_store store(space);
    store.each_shard_read([&t](const barch::shard_ptr& s) {
        const alloc_pair& ap = s->get_ap();
        const auto& leaves = ap.get_leaves();
        const auto& nodes = ap.get_nodes();
        t.leaves_mapped += leaves.mapped_bytes();
        t.leaves_resident += leaves.resident_bytes();
        t.nodes_mapped += nodes.mapped_bytes();
        t.nodes_resident += nodes.resident_bytes();
        t.arenas += 2;
        t.file_backed += (leaves.is_file_backed() ? 1u : 0u) + (nodes.is_file_backed() ? 1u : 0u);
    });
    return t;
}

int KSRESIDENT(caller& call, const arg_t& argv) {
    if (argv.size() > 2)
        return call.wrong_arity();

    auto emit = [&call](const std::string& name, const arena_residency& t) {
        call.start_map();
        call.push_string("space");                 call.push_string(name);
        call.push_string("arenas");                call.push_ll((int64_t) t.arenas);
        call.push_string("file_backed");           call.push_ll((int64_t) t.file_backed);
        call.push_string("mapped_bytes");          call.push_ll((int64_t) t.mapped());
        call.push_string("resident_bytes");        call.push_ll((int64_t) t.resident());
        call.push_string("leaves_mapped_bytes");   call.push_ll((int64_t) t.leaves_mapped);
        call.push_string("leaves_resident_bytes"); call.push_ll((int64_t) t.leaves_resident);
        call.push_string("nodes_mapped_bytes");    call.push_ll((int64_t) t.nodes_mapped);
        call.push_string("nodes_resident_bytes");  call.push_ll((int64_t) t.nodes_resident);
        call.end_map();
    };

    if (argv.size() == 2) {
        if (!(argv[1] == "ALL") && !(argv[1] == "all"))
            return call.push_error("KSRESIDENT takes ALL or nothing");
        call.start_array();
        barch::all_spaces([&](const std::string& name, const barch::key_space_ptr& space) {
            emit(name, measure_residency(space));
        });
        call.end_array();
        return call.ok();
    }

    auto spc = call.kspace();
    emit(spc ? spc->canonical() : std::string(), measure_residency(spc));
    return call.ok();
}
int cmd_KSRESIDENT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KSRESIDENT);
}

int add_keyspace_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(SELECT), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(USE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZEALL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(KSRESIDENT), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SAVE), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(CLEAR), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(CLEARALL), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(KSOPTIONS), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(BEGIN), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(COMMIT), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ROLLBACK), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

void register_keyspace_api(function_map& r) {
    r["SIZE"] = {::SIZE,{"read"}};
    r["DBSIZE"] = {::SIZE,{"read"}};
    r["SIZEALL"] = {::SIZEALL,{"read"}};
    r["USE"] = {::USE,{"write"}};
    r["SELECT"] = {::SELECT,{"write"}};
    r["KSOPTIONS"] = {::KSOPTIONS,{"write"}};
    r["UNLOAD"] = {::UNLOAD,{"write"}};
    r["SPACES"] = {::SPACES,{"read"}};
    r["KSRESIDENT"] = {::KSRESIDENT,{"read"}};
    r["KSPACE"] = {::KSPACE,{"read","write"}};
    r["SAVE"] = {::SAVE,{"read"}};
    r["SAVEALL"] = {::SAVEALL,{"read"}};
    r["FLUSHDB"] = {::CLEAR,{"write","dangerous"}};
    // FLUSHALL reaches every key space, as it does in redis; FLUSHDB stays on the
    // selected one. these were the same handler, so FLUSHALL cleared only one space
    r["FLUSHALL"] = {::CLEARALL,{"write","dangerous"}};
    r["CLEARALL"] = {::CLEARALL,{"write","dangerous"}};
    // over RESP too, so a backup can hold the pages still while it walks them -
    // TODO 416. Not "data": a replica has its own pages. ROLLBACK joins them now
    // that it puts the allocator back as well as the tree - TODO 417
    r["BEGIN"] = {::BEGIN,{"write"}};
    r["COMMIT"] = {::COMMIT,{"write"}};
    r["ROLLBACK"] = {::ROLLBACK,{"write"}};
}
