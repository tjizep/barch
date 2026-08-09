//
// Created by teejip on 8/1/26
//
// Carved out of barch.cpp, which now holds only the module entry points.
//

#include "keyspace_api.h"
#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>

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
    store.each_shard_parallel([&](const barch::shard_ptr& shard) {
        if (!shard->save(true)) {
            barch::err({"could not save", shard->get_shard_number()});
            ++errors;
        }
    });
    save_auth();
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
    if (parser.is_exist) {
        return call.push_bool(barch::is_keyspace(parser.name));
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

        from->merge(to, {});
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
        ks_unique shl(source);
        source->depends(nullptr);
        barch::sharded_store dropped(source);
        dropped.each_shard([](const barch::shard_ptr& shrd) {
            shrd->opt_drop_on_release = true;
        });
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
        return call.push_simple("OK");
    }

    if (parser.is_option && parser.is_set) {
        auto spc = call.kspace();
        barch::sharded_store store(spc);
        ks_unique ul(spc);
        if (parser.name == "ORDERED") {
            bool on = parser.value == "ON";
            store.each_shard([on](const barch::shard_ptr& shrd) { shrd->opt_ordered_keys = on; });
            return call.push_simple("OK");
        }
        if (parser.name == "LRU") {
            bool on = parser.value == "ON";
            bool evict_volatile = parser.value == "VOLATILE";
            store.each_shard([on, evict_volatile](const barch::shard_ptr& shrd) {
                shrd->opt_evict_all_keys_lru = on;
                shrd->opt_evict_volatile_keys_lru = evict_volatile;
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
    barch::all_shards([](auto& shard) {
        shard->save(true);
    });

    return call.push_simple("OK");
}
int cmd_SAVEALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
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
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int CLEAR(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    barch::sharded_store store(call.kspace());
    store.each_shard([](const barch::shard_ptr& shard) { shard->clear(); });

    return call.push_simple("OK");
}
int cmd_CLEAR(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int CLEARALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::all_shards([](auto& shard) {
        shard->clear();
    });


    return call.push_simple("OK");
}
int cmd_CLEARALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int KSOPTIONS(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    if (argv[1] == "SET") {
        if (argv[2] == "UNORDERED") {
            barch::sharded_store store(call.kspace());
            store.each_shard([](const barch::shard_ptr& shard) { shard->opt_ordered_keys = false; });
            return call.push_simple("OK");
        }
        if (argv[2] == "ORDERED") {
            barch::sharded_store store(call.kspace());
            store.each_shard([](const barch::shard_ptr& shard) { shard->opt_ordered_keys = true; });
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
    barch::sharded_store store(call.kspace());
    store.each_shard([](const barch::shard_ptr& t) { t->begin(); });
    return call.ok();
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

int add_keyspace_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(SELECT), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(USE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZEALL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
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
    r["KSPACE"] = {::KSPACE,{"read","write"}};
    r["SAVE"] = {::SAVE,{"read"}};
    r["SAVEALL"] = {::SAVEALL,{"read"}};
    r["FLUSHDB"] = {::CLEAR,{"write","dangerous"}};
    // FLUSHALL reaches every key space, as it does in redis; FLUSHDB stays on the
    // selected one. these were the same handler, so FLUSHALL cleared only one space
    r["FLUSHALL"] = {::CLEARALL,{"write","dangerous"}};
    r["CLEARALL"] = {::CLEARALL,{"write","dangerous"}};
}
