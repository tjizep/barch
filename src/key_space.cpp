//
// Created by teejip on 10/22/25.
//

#include "key_space.h"
#include "message_queue.h"
#include <sys/stat.h>
#include <unistd.h>
#include "dictionary_compressor.h"
#include <filesystem>
#include "ids.h"
#include <thread>
#include <version.h>

#include "shard.h"
#include "keys.h"
#include "module.h"
#include "swig_api.h"
#include "thread_pool.h"
#include "rpc/server.h"
#include "a5hash.h"
#include "configuration.h"
#include "foreign/driver.h"
#include "foreign/sql.h"
#include "fs.h"
#include "http_api.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <mutex>
#include <atomic>
namespace barch {
    /** the durability setting as a queue file policy - TODO 352, 355 */
    static sync_policy aof_policy() {
        // the same mapping a queue transport's durability goes through - one
        // copy of it, in message_queue.h. See TODO 366
        return barch::mq::policy_of(barch::get_aof_sync());
    }

    /** "off", "none" and friends all mean no directory */
    static bool cfg_off(const std::string& v) {
        return v.empty() || v == "off" || v == "none" || v == "no" || v == "false";
    }

    static std::atomic<uint64_t> scratch_ids{0};

    static std::string lower_copy(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return s;
    }

    static key_space::foreign_kind parse_foreign_kind(const std::string& raw) {
        auto v = lower_copy(raw);
        if (v.empty() || v == "off" || v == "none" || v == "no" || v == "false")
            return key_space::foreign_kind::off;
        if (v == "mysql") return key_space::foreign_kind::mysql;
        if (v == "postgres" || v == "postgresql") return key_space::foreign_kind::postgres;
        if (v == "luau") return key_space::foreign_kind::luau;
        if (v == "fake") return key_space::foreign_kind::fake;
        return key_space::foreign_kind::off;
    }

    const char *key_space::foreign_kind_name() const {
        switch (opt_foreign) {
            case foreign_kind::mysql: return "mysql";
            case foreign_kind::postgres: return "postgres";
            case foreign_kind::luau: return "luau";
            case foreign_kind::fake: return "fake";
            case foreign_kind::off:
            default: return "off";
        }
    }

    uint64_t key_space::waiter_timeout_ms() const {
        if (foreign_timeout_ms != 0) return foreign_timeout_ms;
        return get_foreign_timeout_ms();
    }

    uint64_t key_space::script_insns() const {
        if (foreign_script_insns != 0) return foreign_script_insns;
        return get_foreign_script_insns();
    }

    uint64_t key_space::function_slice() const {
        if (function_slice_insns != 0) return function_slice_insns;
        return get_function_slice_insns();
    }

    uint64_t key_space::function_deadline() const {
        if (function_deadline_ms != 0) return function_deadline_ms;
        return get_function_deadline_ms();
    }

    uint64_t key_space::pool_max_age_ms() const {
        if (foreign_pool_max_age_ms != 0) return foreign_pool_max_age_ms;
        return get_foreign_pool_max_age_ms();
    }

    void key_space::drop_idle_sql() {
        if (sql)
            sql->drop_idle();
    }

    static bool literal_split_char(const std::string& pat, char& ch) {
        if (pat.size() != 1)
            return false;
        ch = pat[0];
        return true;
    }

    conversion::comparable_key key_space::encode_key(art::value_type v, bool noint) const {
        char ch = 0;
        if (literal_split_char(key_split, ch))
            return conversion::as_composite(v, noint, ch);
        if (key_split_re)
            return conversion::as_composite(v, noint, key_split_re.get());
        return conversion::as_composite(v, noint);
    }

    static void read_u64(KeyValue& kv, const std::string& key, uint64_t& dest) {
        auto s = kv.get(key);
        if (!s.empty())
            conversion::to(s, dest);
    }

    struct key_spaces {
        key_spaces() {
            barch::log({"Starting Barch",
                "\n",
                "\n\tversion","[",BARCH_PROJECT_VERSION,"]",
                "\n\tpage_size","[",(size_t)page_size,"] bytes",
                "maximum_allocation_size [",(size_t)maximum_allocation_size,"] bytes",
                "\n\tshards","[",get_shard_count().size(),"]",
                "\n\tactive_defrag","[",get_active_defrag(),"]",
                "ordered_keys","[",get_ordered_keys(),"]",
                "\n\tmax_module_memory","[",get_max_module_memory()/(1024.0f*1024.0f*1024.0f),"] GB"
                "\n\tsave_interval","[",get_save_interval(),"] ms"
                "\n\tmin_threads","[",thread_pool::get_min_threads(),"]",
                "\n\tresp service threads","[",(thread_pool::get_system_threads()*resp_pool_factor)/100.0f,"] "
                "socket accept threads","[",(thread_pool::get_system_threads()*tcp_accept_pool_factor)/100.0f,"]"
                "\n\tdefault eviction policy","[",get_eviction_policy(),"]",
                "\n\tcompression","[",get_compression_enabled(),"]","\n"});

        };
        ~key_spaces() = default;
        /*
         * The dictionaries, declared before `spaces` on purpose - TODO 330.
         *
         * Members are destroyed in reverse declaration order, so `spaces` goes
         * first and `~key_space` joins every maintenance thread, and only then do
         * the dictionaries go. The compression pass runs on those threads and
         * reaches the dictionaries, so that order is the whole point.
         *
         * It used to be a static in dictionary_compressor.cpp, which gets this
         * exactly backwards: it is built on the first compression, later than
         * this registry, and statics are destroyed in reverse order of
         * construction - so it was freed while the clock was still ticking. TSan
         * reported twenty one use-after-frees at the end of a test, every one
         * inside a correctly held lock, because the lock was never the problem.
         * An explicit `dictionary::shutdown()` called from here was worse: by the
         * time this destructor runs, that static is already gone, so the call was
         * itself a use after free - 126 reports and seven red tests.
         */
        dictionary::store dictionaries{};
        std::recursive_mutex lock{};
        std::string ks_pattern = "[0-9,A-Z,a-z,_]+";
        std::string ks_pattern_error = "space name does not match the "+ks_pattern+" pattern";
        std::regex name_check{ks_pattern};
        heap::map<std::string, key_space_ptr> spaces{};

    };

    key_spaces& ksp() {
        static key_spaces _ksp;
        return _ksp;
    }

    dictionary::store& ks_dictionaries() {
        return ksp().dictionaries;
    }


    static std::string decorate(const std::string& name_) {
        if (name_.empty() || name_ == "0")
            return "node";
        return name_ + "_"; // so that system stores dont get clobbered
    }

    static std::string undecorate(const std::string& name_) {
        if (name_ == "node")
            return "";
        std::string r = name_;
        if (!r.empty())
            r.resize(r.size()-1);
        return r;
    }

    std::string ks_undecorate(const std::string& name) {
        return undecorate(name);
    }

    const std::string& get_ks_pattern_error() {
        return ksp().ks_pattern_error;
    }

    void all_shards(const std::function<void(const barch::shard_ptr&)>& cb ) {

        heap::map<std::string, key_space_ptr> spaces;
        {
            std::unique_lock l(ksp().lock);
            spaces = ksp().spaces;
        }
        for (auto &ks : spaces) {
            auto shards = ks.second->get_shards();
            for (auto &shard_ : shards) {
                cb(shard_);
            }
        }
    }

    void all_spaces(const std::function<void(const std::string& name, const barch::key_space_ptr&)>& cb ) {
        heap::map<std::string, key_space_ptr> spaces;
        {
            std::unique_lock l(ksp().lock);
            spaces = ksp().spaces;
        }
        for (auto ks : spaces) {
            auto un = undecorate(ks.first);
            if (un.empty()) un = "(default)";
            cb(un, ks.second);
        }
    }

    bool check_ks_name(const std::string& name_) {
        auto name = decorate(name_);
        return std::regex_match(name, ksp().name_check);
    }
    bool is_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            return false;
        }
        std::unique_lock l(ksp().lock);
        std::string name = decorate(name_);
        auto s = ksp().spaces.find(name);
        return  (s != ksp().spaces.end());
    }
    key_space_ptr get_keyspace(const std::string &name_) {
        if (!check_ks_name(name_)) {
            throw_exception<std::invalid_argument>(get_ks_pattern_error().c_str());
        }
        std::unique_lock l(ksp().lock);
        std::string name = decorate(name_);
        auto s = ksp().spaces.find(name);
        if (s != ksp().spaces.end()) {
            return s->second;
        }

        heap::allocator<key_space> alloc;
        // cannot create keyspace without memory
        auto ks = std::allocate_shared<key_space>(alloc, name);
        ksp().spaces[name] = ks;
        return ks;
    }

    bool unload_keyspace(const std::string& name) {
        return flush_keyspace(name);
    }

    void snapshot_arenas() {
        /*
         * No global check here: a space can map while the server default is off, so
         * the only honest answer is to ask every arena. One that is not mapped has
         * no `backing_path` and says no straight away. TODO 268.
         */
        size_t written = 0;
        all_shards([&written](const barch::shard_ptr& s) {
            if (s && s->save_snapshot())
                ++written;
        });
        if (written)
            barch::log({"wrote", written, "arena snapshots"});
    }

    bool flush_keyspace(const std::string& name_) {
        bool r = false;
        if (!check_ks_name(name_)) {
            throw_exception<std::invalid_argument>(get_ks_pattern_error().c_str());
        }
        std::string name = decorate(name_);
        key_space_ptr held;
        {
            std::unique_lock l(ksp().lock);
            auto s = ksp().spaces.find(name);
            if (s != ksp().spaces.end()) {
                held = s->second;
                ksp().spaces.erase(s);
                r = true;
            }
        }
        if (held)
            held->fail_foreign_flights();
        // the space is going away and will be rebuilt from disk if it comes back, so
        // a cached id block belongs to a counter that may no longer exist - TODO 253
        barch::forget_sequences(undecorate(name));
        // and what it asked of its arenas, so a space that comes back reads it fresh
        barch::forget_space_arena(name);
        return r; // destruction happens in callers thread - so hopefully no dl because shared ptr
    }


/**
 * How many shards a saved store for this space was written with, or 0 when
 * there is nothing on disk yet.
 *
 * Taken from the file names rather than from inside them, because that is what
 * covers a store written before anything recorded the count - which is every
 * store that exists today. A shard's arenas are saved as
 * `leaves_<space><n>.dat`, so the highest n plus one is the count.
 *
 * See TODO 314 for why this has to be checked. Loading a store with a different
 * shard count than it was written with does not fail, it silently half works:
 * measured on 5,000 keys written at 347 and loaded at 37, DBSIZE came back 529,
 * `KEYS` listed those 529, and `GET` on every one of them answered empty -
 * because routing hashes modulo the *current* count, so a key that lived in
 * shard 200 now looks in 200 % 37 and finds nothing. The rest of the data is
 * still on disk in the files nobody opened.
 */
/**
 * Refuse to load a space whose shard count is not the one configured - TODO 314,
 * and one message rather than two since TODO 328.
 *
 * Said the same way from both places that can notice: the names on disk, and the
 * count a shard file carries inside it. The two differ only in where `saved`
 * came from, and a message kept in step by hand is a message that drifts.
 */
[[noreturn]] static void refuse_shard_count(const std::string& name, uint64_t saved,
                                            size_t wanted) {
    const bool is_default = (name == "node");
    const std::string knob = is_default
        ? std::string("internal_shards")
        : barch::ks_undecorate(name) + ".shards";
    // undecorate("node") is the empty string - the default space has no name of
    // its own, so say the one it is known by
    const std::string shown = is_default ? std::string("node") : barch::ks_undecorate(name);
    const std::string msg =
        "space '" + shown + "' was saved with " + std::to_string(saved)
        + " shards and this server wants " + std::to_string(wanted)
        + ". Loading it would leave most of its keys unreachable. Set "
        + knob + " to " + std::to_string(saved)
        + ", or move the shard files aside.";
    throw_exception<std::runtime_error>(msg.c_str());
}

static size_t shards_on_disk(const std::string& decorated_name) {
    const std::string prefix = "leaves_" + decorated_name;
    const std::string ext = ".dat";
    size_t highest = 0;
    std::error_code ec;
    std::filesystem::directory_iterator it(std::filesystem::current_path(), ec);
    if (ec)
        return 0;
    for (const auto& entry : it) {
        auto fn = entry.path().filename().string();
        if (fn.size() <= prefix.size() + ext.size())
            continue;
        if (fn.compare(0, prefix.size(), prefix) != 0)
            continue;
        if (fn.compare(fn.size() - ext.size(), ext.size(), ext) != 0)
            continue;
        // the shard number, and nothing but - `leaves_node2_5.dat` belongs to a
        // space called `node2`, not to shard "2_5" of this one
        auto digits = fn.substr(prefix.size(), fn.size() - ext.size() - prefix.size());
        if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos)
            continue;
        highest = std::max<size_t>(highest, std::stoull(digits) + 1);
    }
    return highest;
}

    key_space::key_space(const std::string &name) :name(name), canonical_name(undecorate(name)) {
        if (shards.empty()) {
            // everything allocated while this space is built counts towards startup memory
            uint64_t memory_before = get_total_memory();
            decltype(shards) shards_out;
            if (name == "configuration" || name == "configuration_") {
                opt_shard_count = 1;
            }
            if (name != "configuration_" && name != "node") {
                // cannot configure configuration or the default ns "node" it is what it is
                std::string real = undecorate(name);
                KeyValue kv("configuration"); // this will also be replicated
                auto sc = kv.get(real+".shards");
                if (!sc.empty())
                    conversion::to(sc, opt_shard_count);
                auto ordered = kv.get(real+".ordered");
                if (!ordered.empty())
                    opt_ordered_keys = ordered != "0";
                auto hybrid = kv.get(real+".hybrid");
                if (!hybrid.empty())
                    opt_hybrid_keys = hybrid != "0";
                auto compression = kv.get(real+".compression");
                if (!compression.empty()) {
                    auto c = lower_copy(compression);
                    opt_compression = !(c == "0" || c == "off" || c == "none"
                                     || c == "no" || c == "false");
                }
                auto ranged = kv.get(real+".range_sharded");
                if (!ranged.empty())
                    opt_range_sharded = ranged != "0";
                auto foreign = kv.get(real+".foreign");
                if (!foreign.empty()) {
                    auto kind = lower_copy(foreign);
                    bool explicit_off = kind == "off" || kind == "none" || kind == "no" || kind == "false";
                    opt_foreign = parse_foreign_kind(foreign);
                    if (opt_foreign == foreign_kind::off && !explicit_off) {
                        barch::err({"unknown foreign source - ignoring it for space", name, foreign});
                    }
                }
                foreign_dsn = kv.get(real+".foreign_dsn");
                foreign_host = kv.get(real+".foreign_host");
                foreign_user = kv.get(real+".foreign_user");
                foreign_password = kv.get(real+".foreign_password");
                foreign_database = kv.get(real+".foreign_database");
                foreign_query = kv.get(real+".foreign_query");
                foreign_script = kv.get(real+".foreign_script");
                /*
                 * Where this space's arenas map from, if it wants something other
                 * than the server default - TODO 268. Registered before a shard
                 * exists, because an arena reads it the first time it allocates.
                 */
                arena_dir = kv.get(real+".arena_dir");
                arena_map = kv.get(real+".arena_map");
                aof_dir = kv.get(real+".aof_dir");
                aof_on = kv.get(real+".aof");
                if (!arena_map.empty() && arena_map != "all" && arena_map != "leaves"
                    && arena_map != "nodes" && arena_map != "off") {
                    barch::err({"arena_map is all, leaves, nodes or off - ignoring it for space",
                                name, arena_map});
                    arena_map.clear();
                }
                barch::set_space_arena(name, arena_dir, arena_map);
                fs_source = kv.get(real+".fs_source");
                fs_source_list = kv.get(real+".fs_source_list");
                read_u64(kv, real+".fs_cache_bytes", fs_cache_bytes);
                read_u64(kv, real+".foreign_port", foreign_port);
                read_u64(kv, real+".missing_ttl", missing_ttl);
                read_u64(kv, real+".foreign_timeout_ms", foreign_timeout_ms);
                read_u64(kv, real+".foreign_query_timeout_ms", foreign_query_timeout_ms);
                read_u64(kv, real+".foreign_max_inflight", foreign_max_inflight);
                read_u64(kv, real+".foreign_pool_size", foreign_pool_size);
                read_u64(kv, real+".foreign_pool_max_age_ms", foreign_pool_max_age_ms);
                read_u64(kv, real+".foreign_script_insns", foreign_script_insns);
                read_u64(kv, real+".function_slice_insns", function_slice_insns);
                read_u64(kv, real+".function_deadline_ms", function_deadline_ms);
                key_split = kv.get(real+".key_split");
                if (!key_split.empty()) {
                    try {
                        key_split_re = std::make_shared<std::regex>(
                            key_split, std::regex::ECMAScript | std::regex::optimize);
                    } catch (const std::regex_error& e) {
                        barch::err({"key_split is not a regex - ignoring it for space",
                                    name, key_split, e.what()});
                        key_split_re.reset();
                    }
                }
                if (opt_foreign == foreign_kind::mysql || opt_foreign == foreign_kind::postgres) {
                    if (foreign_dsn.empty() && foreign_host.empty()) {
                        barch::err({"foreign source needs a dsn or host - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (foreign_query.empty()) {
                        barch::err({"foreign source needs a query - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign::query_has_placeholder(foreign_query)) {
                        barch::err({"foreign query needs ? or $n or $$ - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (opt_foreign == foreign_kind::mysql
                               && !foreign::prepare_mysql(*this)) {
                        opt_foreign = foreign_kind::off;
                    } else if (opt_foreign == foreign_kind::postgres
                               && !foreign::prepare_postgres(*this)) {
                        opt_foreign = foreign_kind::off;
                    }
                } else if (opt_foreign == foreign_kind::luau) {
                    if (foreign_script.empty()) {
                        barch::err({"luau foreign source needs a script - ignoring it for space", name});
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign::prepare_luau(*this)) {
                        opt_foreign = foreign_kind::off;
                    } else if (!foreign_dsn.empty() || !foreign_host.empty()) {
                        std::string err;
                        auto resolved = foreign::resolve_dsn(*this, err);
                        auto look = resolved.empty() ? foreign_dsn : resolved;
                        auto dsn_looks_pg = look.find("postgres") != std::string::npos
                            || look.find("dbname=") != std::string::npos;
                        if (dsn_looks_pg)
                            foreign::prepare_postgres(*this);
                        else
                            foreign::prepare_mysql(*this);
                    }
                }
            }
            if (opt_range_sharded && !opt_ordered_keys) {
                // a range only means something where the keys are in order. Refused
                // rather than quietly ignored, so that reading the option back tells
                // the truth about what the space is doing
                barch::err({"range sharding needs ordered keys - ignoring it for space",
                            name});
                opt_range_sharded = false;
            }
            opt_shard_count = std::max<size_t>(opt_shard_count, 1);
            /*
             * Refuse a store that was written with a different number of shards,
             * rather than coming up with most of its keys unreachable - TODO 314.
             * Nothing migrates yet: a rehash into a new count is a rebuild of
             * the space, and saying so is the useful half.
             *
             * This half only catches a count that shrank, and deliberately: a
             * shard with nothing in it writes no file at all, so the highest
             * index on disk is a lower bound on what the space was cut into, not
             * the count. A space of 7 that only ever filled shards 0 and 2
             * leaves files up to 2, and refusing that would be a false alarm.
             * Files *above* what is about to be loaded are not ambiguous - they
             * hold keys nothing will open. The exact count comes off the file
             * itself, checked below once a shard has read it.
             */
            if (const size_t written = shards_on_disk(name);
                written > opt_shard_count) {
                refuse_shard_count(name, written, opt_shard_count);
            }
            shards_out.resize(opt_shard_count);
            /*
             * The change log, before the shards, because each of them is handed
             * the same one - TODO 355.
             *
             * Opt in per space - TODO 357. `<space>.aof_dir` names a directory
             * for this space and is itself the asking; `<space>.aof` on opts in
             * to the server's `aof_dir` instead. A bare `aof_dir` says where
             * logs would go and gives none, so no space - the internal ones
             * least of all - keeps a log it was not asked to keep.
             */
            const bool own_dir = !aof_dir.empty() && !cfg_off(aof_dir);
            const bool asked = own_dir || (!aof_on.empty() && !cfg_off(aof_on));
            const std::string dir = own_dir ? aof_dir : barch::get_aof_dir();
            if (asked && dir.empty()) {
                // asking and getting nothing silently is the one outcome nobody
                // wants: the records were the point of asking
                barch::err({"space", name, "asks for a change log and neither",
                            "<space>.aof_dir nor the server's aof_dir names anywhere",
                            "to put one - it will run without one"});
            }
            /*
             * A log this space did not ask for, sitting where its log would be
             * - TODO 359.
             *
             * `<space>.aof` is a key in the `configuration` space, so it is
             * data: a server killed before that space is saved comes back
             * without it, and this space then quietly stops keeping a history
             * and quietly ignores the one it already has. Every other per space
             * setting is data in the same way - there are thirty of them, and
             * losing `.shards` matters far more - but the others announce
             * themselves. A space with no foreign source plainly does not fetch;
             * a shard count that does not match is refused outright by
             * `refuse_shard_count`. This one just goes missing, and the records
             * sit in a file nobody opens.
             *
             * So it is said out loud. The check only covers the server's own
             * directory: a space that named its own in `<space>.aof_dir` and
             * then lost that setting leaves nowhere to look, which is worth
             * knowing as a limit rather than pretended away.
             */
            if (!asked) {
                if (const auto server_dir = barch::get_aof_dir(); !server_dir.empty()) {
                    const std::string orphan = server_dir + "/" + name + ".aof";
                    if (::access(orphan.c_str(), F_OK) == 0) {
                        barch::err({"space", name, "has a change log at", orphan,
                                    "but was not asked to keep one, so it is not being"
                                    " read. If it should be, set", name + ".aof",
                                    "on - the opt in lives in the configuration space and"
                                    " is lost if that space is not saved. If it should"
                                    " not be, move the file aside"});
                    }
                }
            }
            if (asked && !dir.empty()) {
                try {
                    ::mkdir(dir.c_str(), 0755);              // already there is fine
                    change_log = std::make_shared<aof::log>(dir + "/" + name + ".aof",
                                                            aof_policy());
                    barch::log({"change log for", name, "in", dir,
                                "durability", barch::get_aof_durability()});
                } catch (const std::exception& e) {
                    // a space that cannot log still works; it just does not log
                    barch::err({"no change log for space", name, "-", e.what()});
                    change_log.reset();
                }
            }

            heap::allocator<barch::shard> alloc;
            auto start_time = std::chrono::high_resolution_clock::now();
            size_t shards_loaded = shard_thread_processor(shards_out.size(),[&](size_t shard_num) {
                shard_ptr& shard = shards_out[shard_num];
                shard = std::allocate_shared<barch::shard>(alloc,  name, 0, shard_num);
                shard->opt_ordered_keys = opt_ordered_keys.load();
                shard->opt_hybrid_keys = opt_hybrid_keys.load();
                shard->opt_compression = opt_compression.load();
                // the log is handed over *after* a replay, not here - see below
                shard->space_shards = opt_shard_count;   // recorded on save - TODO 314
                shard->apply_lru_options();  // compression shares the LRU bits
                shard->load(true);
            });
            if (shards_out.size() != shards_loaded) {
                abort_with("shard loading threads invalid count");
            }
            // each shard file carries ordered/hybrid in its extra; load puts those
            // on the shard. SET uses the space flag and GET uses the shard, so the
            // space has to take the loaded fact or a HashBenchy save (ordered off)
            // comes up with SET writing ART and GET looking in the hash.
            // the default space "node" is not configured from KV. See TODO 218.
            /*
             * The exact count, off the files rather than their names. Any shard
             * that carried one is authoritative; a file written before this was
             * recorded says 0 and is left to the name based check above.
             */
            for (const auto& s : shards_out) {
                const uint64_t saved = s->saved_space_shards.load();
                if (saved != 0 && saved != opt_shard_count) {
                    refuse_shard_count(name, saved, opt_shard_count);
                }
            }
            opt_ordered_keys = shards_out[0]->opt_ordered_keys.load();
            opt_hybrid_keys = shards_out[0]->opt_hybrid_keys.load();
            statistics::shards = shards_out.size();
            auto end_time = std::chrono::high_resolution_clock::now();
            double millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            shards.swap(shards_out);
            barch::log({"Loaded",shards.size(),"shards in", millis/1000.0f, "s", shards_loaded});
            // one line for all of them, not one each - see arena::mapped_count
            if (auto mapped = arena::mapped_count().exchange(0); mapped > 0)
                barch::log({"mapped", mapped, "arenas back rather than loading them"});
            if (opt_range_sharded) {
                build_range_index();
            }
            /*
             * Replay what the shard files do not have yet - TODO 356.
             *
             * The order here is the whole argument, and three parts of it are
             * not interchangeable.
             *
             * After the swap and after the range index, because routing a record
             * goes through `get_shard_index`, which divides by
             * `get_shard_count()` - and that counts `shards`, which is empty
             * until the swap above. Replaying before it divided by zero and took
             * the process down with SIGFPE.
             *
             * After the shard files are loaded, because they are the state as of
             * the last checkpoint and the log holds what happened after it. The
             * files first, the log over the top, eldest first.
             *
             * And before the shards are given the log, which is the line below.
             * A shard holding it during a replay records every replayed write
             * back into the same log: it would grow by its own length on every
             * start, and nothing would look wrong until the disk filled.
             *
             * Replaying twice is harmless, which matters because nothing removes
             * these records until a save writes the next checkpoint - so a crash
             * before that save replays the same records again. A `set` of a
             * value already there and an `erase` of a key already gone both
             * leave the space as it is.
             */
            if (change_log) {
                replay_change_log();
            }
            for (auto& shard : shards) {
                shard->change_log = change_log;      // null when there is none
            }
            // other threads allocate concurrently so only a growth is meaningful here
            uint64_t memory_after = get_total_memory();
            if (memory_after > memory_before) {
                add_startup_memory(memory_after - memory_before);
            }
        }
        start_maintain();
    }
    /**
     * The index is a function of the shards, so a load rebuilds it rather than reading
     * it back - there is no index file, nothing to write atomically and nothing to find
     * out of step with the data it describes.
     *
     * The rebuild only works if the shards are already an ordered partition, which they
     * are if this space was range sharded the last time it was written. If it was hash
     * sharded then every shard holds keys from all over the order, and routing by the
     * boundaries of that would lose most of them. That case is repartitioned: nearly
     * every key moves, which is why it happens once, at load, and says so in the log.
     */
    void key_space::build_range_index() {
        if (rindex.rebuild(shards)) {
            return;
        }
        uint64_t keys = 0;
        for (auto& s : shards) keys += s->get_tree_size();
        barch::log({"key space", name, "is range sharded but its", keys,
                    "keys are not in shard order - repartitioning"});
        auto start_time = std::chrono::high_resolution_clock::now();
        size_t moved = rindex.repartition(shards);
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();
        if (rindex.rebuild(shards)) {
            barch::log({"repartitioned", name, "moving", moved, "keys in",
                        (double) millis / 1000.0, "s"});
        } else {
            // the space still works - every route lands somewhere - but it is not the
            // ordered partition the option asked for, so say so rather than let the
            // ordered operations quietly return partial answers
            barch::err({"could not repartition key space", name,
                        "- range routing will not find every key"});
        }
    }

    key_space::key_space(scratch_t)
        : name("-s" + std::to_string(++scratch_ids)),
          canonical_name(name) {
        opt_shard_count = 1;
        opt_ordered_keys = barch::get_ordered_keys();
        opt_hybrid_keys = barch::get_hybrid_keys();
        opt_range_sharded = false;
        shards.resize(1);
        heap::allocator<barch::shard> alloc;
        shards[0] = std::allocate_shared<barch::shard>(alloc, name, barch::shard::scratch_t{});
        shards[0]->opt_ordered_keys = opt_ordered_keys.load();
        shards[0]->opt_hybrid_keys = opt_hybrid_keys.load();
        shards[0]->opt_compression = opt_compression.load();
        shards[0]->apply_lru_options();
        shards[0]->opt_drop_on_release = true;
    }

    key_space::key_space_ptr key_space::make_scratch() {
        heap::allocator<key_space> alloc;
        return std::allocate_shared<key_space>(alloc, scratch_t{});
    }

    /*
     * Is the server both over its pre-eviction threshold and running a policy that
     * was asked to take keys with no expire set? Both have to be true before a
     * stored file goes - see TODO 302.
     */
    static bool fs_evictable(const barch::shard_ptr& s) {
        if (!s)
            return false;
        /*
         * The space's own switch, not the global one. `CONFIG SET eviction_policy`
         * only reaches the default space's shards; a named space is switched with
         * `KSPACE OPTION SET LRU ON`, which writes these. Whole file eviction has to
         * follow whatever governs the key level sweep for this space, or the two
         * disagree about whether the space is being evicted at all.
         *
         * All-keys only: a volatile policy was asked to touch keys with an expire
         * set, and a stored file has none.
         */
        if (!(s->opt_evict_all_keys_lru || s->opt_evict_all_keys_lfu
              || s->opt_evict_all_keys_random))
            return false;
        auto mm = barch::get_max_module_memory();
        if (mm == 0)
            return false;
        return statistics::logical_allocated >= (uint64_t) (mm * barch::get_pre_evict_thresh());
    }

    void key_space::replay_change_log() {
        // no log was asked for - the ordinary case, and it says nothing
        if (!change_log)
            return;
        if (shards.empty()) {
            // a space with a log and no shards cannot happen from here: the caller
            // replays after `shards.swap`. If it ever does, the log is not applied
            // and the space silently loses every write since the last checkpoint,
            // so say it rather than return into the dark
            barch::err({"change log for", name, "was not replayed because the space has"
                        " no shards. Every write after its last checkpoint is still in"
                        " the log and is not in this space"});
            return;
        }

        /*
         * Does this log belong to this space at all - TODO 358.
         *
         * Every record carries the key space it was written for, so a record
         * naming another one is proof rather than suspicion: a file renamed, a
         * log copied in from somewhere else. That is checked first and on its
         * own, before anything is applied, because applying half of somebody
         * else's history and then noticing is worse than not starting - the
         * space would look right and be wrong.
         */
        uint32_t foreign = 0;
        std::string other;
        change_log->replay([&](const aof::record& r) {
            if (!r.space.empty() && r.space != name) {
                ++foreign;
                if (other.empty())
                    other = r.space;
            }
        });
        if (foreign) {
            barch::err({"change log for", name, "holds", foreign, "records written for",
                        other, "- not applying any of it. The file may have been renamed"
                        " or copied from another space; move it aside. This space has"
                        " loaded from its shard files alone"});
            return;
        }

        /*
         * Whether a recorded placement can be checked - TODO 358. Where routing
         * is a hash of the key it is a pure function of the key, so a record
         * that says which shard it went to can be verified against it. Range
         * routing is not: `rindex` moves keys between shards while the space
         * runs, so a disagreement would mean nothing at all.
         */
        const bool routing_is_a_function_of_the_key = !opt_range_sharded;

        uint32_t applied = 0, erased = 0, rerouted = 0, agreed = 0, disagreed = 0;
        // records that reached the callback and were not applied. Each one is a
        // write this space is missing, so each is counted and the first is named
        uint32_t off_the_end = 0, no_shard = 0, not_a_write = 0;
        size_t first_off_the_end = 0;
        bool off_the_end_seen = false;
        std::string first_dropped_key;
        bool dropped_key_seen = false;
        uint8_t first_odd_type = 0;
        // the first one named, not the last, and a flag rather than "is it empty"
        // because an empty key is a key
        const auto note_dropped = [&](const art::value_type& key) {
            if (!dropped_key_seen) {
                first_dropped_key.assign(key.chars(), key.size);
                dropped_key_seen = true;
            }
        };
        const auto outcome = change_log->replay([&](const aof::record& r) {
            const art::value_type key{r.key.data(), (unsigned) r.key.size()};
            /*
             * Where the record says it went, when that still means something.
             *
             * A key does not say which shard it belongs to. A container's
             * entries live on the shard its *name* routes to, and the composite
             * `container|field` keys are written into that shard - so routing a
             * composite key by itself puts it somewhere else, and the read,
             * which routes by the container, never finds it. Nothing recovers a
             * container name from a composite key, so the record carries the
             * placement instead.
             *
             * With a different shard count those numbers mean nothing, so the
             * key is routed instead. Right for plain keys, wrong for container
             * entries, which is counted and reported rather than left for a read
             * that returns nothing.
             */
            size_t at;
            if (r.shard_count == shards.size()) {
                at = r.shard;
                if (routing_is_a_function_of_the_key) {
                    // a plain key has to hash to where it says it went. a
                    // container entry is placed by its container's name and will
                    // not, so this is read as a shape at the end rather than
                    // acted on one record at a time
                    if (get_shard_index(key) == at) {
                        ++agreed;
                    } else {
                        ++disagreed;
                    }
                }
            } else {
                at = get_shard_index(key);
                ++rerouted;
            }
            if (at >= shards.size()) {
                // the record names a shard this space does not have. Only reachable
                // when the count matched and the recorded number was out of range,
                // which means the file is not what it claims to be
                ++off_the_end;
                if (!off_the_end_seen) {
                    first_off_the_end = at;
                    off_the_end_seen = true;
                }
                note_dropped(key);
                return;
            }
            const auto& shard = shards[at];
            if (!shard) {
                // a hole in the shard vector. Nothing here creates one, so this is
                // a load that went wrong upstream rather than anything about the log
                ++no_shard;
                note_dropped(key);
                return;
            }
            unique_latch release(shard->get_latch());
            if (r.type == aof::record_type::set) {
                // the options as recorded, so a compressed value goes back as a
                // compressed value rather than as its bytes - TODO 355
                const art::key_options opts(r.options, (uint64_t) r.expiry_ms);
                const art::value_type value{r.value.data(), (unsigned) r.value.size()};
                shard->insert(opts, key, value, true, [](const art::node_ptr&) {});
                ++applied;
            } else if (r.type == aof::record_type::erase) {
                shard->remove(key, [](const art::node_ptr&) {});
                ++erased;
            } else {
                // replay() is meant to hand over writes and deletes only - it
                // starts after the last checkpoint, so a checkpoint here is a bug
                // in it, and any other type is a record this build does not know
                ++not_a_write;
                if (!first_odd_type)
                    first_odd_type = (uint8_t) r.type;
                note_dropped(key);
            }
        });

        if (applied || erased) {
            barch::log({"replayed", applied, "writes and", erased, "deletes into",
                        name, "from its change log"});
        } else if (outcome.records) {
            // read something and applied none of it. Not normal - a record that
            // is neither a write nor a delete is the only way here
            barch::log({"change log for", name, "read", outcome.records,
                        "records after its last checkpoint and applied none of them"});
        } else {
            // nothing after the last checkpoint, which is what a clean shutdown
            // leaves. Said out loud because it is also what a log being written
            // somewhere else looks like, and one line per space at start is cheap
            barch::log({"change log for", name,
                        "holds nothing after its last checkpoint - nothing to replay"});
        }
        if (off_the_end) {
            barch::err({"change log for", name, "has", off_the_end, "records naming shard",
                        first_off_the_end, "or higher, and this space has", shards.size(),
                        "- those writes were not applied. The first was for key",
                        first_dropped_key, ". A log saying that is not this space's log"});
        }
        if (no_shard) {
            barch::err({"change log for", name, "could not be applied for", no_shard,
                        "records because the shard they route to is not there. The first"
                        " was for key", first_dropped_key,
                        "- the space did not load completely, so this is not about the log"});
        }
        if (not_a_write) {
            barch::err({"change log for", name, "holds", not_a_write, "records that are"
                        " neither a write nor a delete - the first is type",
                        (int) first_odd_type, ". They were not applied. Either the replay"
                        " handed over a checkpoint, which is a bug in it, or the file was"
                        " written by a newer build of barch"});
        }
        if (agreed == 0 && disagreed >= 8) {
            barch::err({"change log for", name, "has", disagreed, "records and not one of"
                        " them hashes to the shard it says it went to. That is what a log"
                        " from another space or a changed key_split looks like - it has"
                        " been applied, but check it is the right file"});
        }
        if (rerouted) {
            barch::err({"change log for", name, "was written at a different shard count,"
                        " so", rerouted, "records were routed by key instead of by where"
                        " they were. Plain keys are fine; entries of a hash, list or"
                        " ordered set may be on the wrong shard and unreadable. Load this"
                        " space at the count it was written with, or rebuild it"});
        }
        if (outcome.stopped_early) {
            /*
             * The log ended in something that did not verify, which is what a
             * crash under a durability weaker than `each` leaves behind. What
             * came before it has been applied and is sound; what follows cannot
             * be trusted and is not guessed at.
             */
            barch::err({"change log for", name, "stops at sequence", outcome.at_sequence,
                        "-", aof::describe(outcome.why),
                        "- later records, if any, were not applied"});
        }
    }

    void key_space::start_maintain() {
        exiting = false;
        maintain_running = true;
        tmaintain = std::thread([&]() -> void {

            try {
                auto tshards = this->get_shards();

                while (!this->thread_control.wait((int64_t)get_maintenance_poll_delay()*1000ll)) {
                   tshards = this->get_shards();
                   repl::distribute();
                    ++statistics::maintenance_cycles;

                   /*
                    * Bound the process's own cgroup to the working set plus
                    * headroom - TODO 348. Off unless asked for, rate limited
                    * inside, and it says why rather than failing quietly: the
                    * usual reason is that the process does not own a writable
                    * cgroup, which is a deployment fact and not an error here.
                    */
                   /*
                    * `aof_durability = timer` means the log is pushed to the
                    * device on this tick and not per record - TODO 355. Only
                    * for timer: `each` is already synchronous, `none` asked for
                    * no syncing at all, and a byte threshold does its own.
                    */
                   if (change_log
                       && barch::get_aof_sync().mode == aof_sync_setting::timer) {
                       try {
                           change_log->sync();
                       } catch (const std::exception& e) {
                           barch::err({"could not sync the change log for", name, e.what()});
                       }
                   }

                   if (barch::get_cgroup_memory_control()) {
                       std::string why;
                       // it says why itself, once per distinct reason - a shared
                       // cgroup or an unwritable one is a deployment fact that
                       // should be stated once and not once a tick
                       heap::apply_cgroup_memory_max(why);
                   }

                   if (opt_range_sharded) {
                       // rebalancing lives here rather than on the insert path. Two
                       // reasons, and the second is the one that matters: an insert that
                       // has to move a block of keys is a latency spike a background
                       // sweep does not have, and putting every write to the table on
                       // one thread is what lets a router validate its route by simply
                       // re-reading the table under the lock it just took.
                       //
                       // A sweep is bounded by how many pairs of shard locks it may
                       // take, not by how much work is left, so it cannot hold up this
                       // thread; what it does not finish, the next one continues
                       try {
                           rindex.sweep(tshards, get_range_shard_budget(),
                                        get_range_shard_tolerance(), tshards.size() * 64);
                       } catch (std::exception& e) {
                           barch::err({"exception rebalancing range shards:", e.what()});
                       }
                   }

                   /*
                    * Whole file eviction - see TODO 302.
                    *
                    * The key level sweep in shard.cpp is refused `fs:` keys, because
                    * a stored file is a name record, an inode and one key per chunk
                    * and that sweep sees one leaf at a time. So the memory it is not
                    * allowed to take comes back here, where a file can be staged
                    * into one commit and either goes entirely or not at all.
                    *
                    * It runs on the same terms the key level sweep does: only over
                    * the pre-eviction threshold, and only when an all-keys policy is
                    * on, since a volatile policy has not been asked to touch
                    * anything without an expire and a stored file has none. Budgeted
                    * at a few files a pass rather than "until it fits", the way the
                    * range rebalancer above is - what this one does not finish, the
                    * next cycle continues.
                    */
                   try {
                       if (!fs_source.empty() && !tshards.empty()
                           && fs_evictable(tshards[0])) {
                           auto self = barch::get_keyspace(get_canonical_name());
                           if (self)
                               statistics::files_evicted += barch::fs::evict_some(self, 8);
                       }
                   } catch (std::exception& e) {
                       barch::err({"exception evicting files:", e.what()});
                   }

                   for (auto s : tshards) {
                       try {
                           s->maintenance();
                       }catch (std::exception& e) {
                           barch::err({"exception in maintenance:",e.what()});
                       }
                       if (exiting) break;
                   }

                   try {
                       drop_idle_sql();
                   } catch (std::exception& e) {
                       barch::err({"exception dropping idle sql:", e.what()});
                   }

                }
            }catch (std::exception& e){
               barch::err({"shard maintenance thread error:",e.what()});
            }
            thread_exit.signal(1);
        });
    }
    void key_space::fail_foreign_flights() {
        heap::vector<abstract_session_ptr> sessions;
        for (auto& sh : shards) {
            if (!sh) continue;
            // DROP used to hold these already; try_lock_for then fails at once
            // (EDEADLK / false) on a non-recursive mutex. Still fail the
            // flights: the space is going away either way.
            std::unique_lock lck(sh->get_latch(), std::defer_lock);
            if (!lck.try_lock_for(std::chrono::milliseconds(sh->lock_to_ms)))
                barch::warn({"foreign unload lock busy", name});
            auto* s = static_cast<shard*>(sh.get());
            s->fail_foreign("FOREIGN space unloaded", sessions);
            for (auto& [k, fl] : s->flights) {
                if (!fl->owns_inflight) continue;
                fl->owns_inflight = false;
                if (foreign_inflight > 0)
                    --foreign_inflight;
            }
        }
        // a space being unloaded wakes every waiter it has, and there is no key behind
        // this one - the block is being failed, not satisfied, so there is no turn to
        // pass on
        for (auto& sess : sessions)
            sess->do_block_continue(std::string());
    }

    key_space::~key_space() {
        barch::stop_http_server(canonical_name);
        exiting = true;
        if (maintain_running) {
            thread_control.signal(1);
            thread_exit.wait();
            if (tmaintain.joinable())
                tmaintain.join();
        }
        fail_foreign_flights();
        shards.clear();
    }

    shard_ptr key_space::get_local() {
        static std::atomic<uint64_t> sid;
        thread_local shard_ptr shard;
        if (!shard) {
            heap::allocator<key_space> alloc;
            ++sid;
            shard = std::allocate_shared<barch::shard>(alloc,  name + std::to_string(sid.load()), 0, 0);
            //shard->load(true);
        }
        return shard;
    }
    shard_ref key_space::get_ref(size_t shard) {
        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()].get();
        if (r == nullptr) {
            abort_with("shard not found");
        }
        return r;
    }


    std::shared_ptr<abstract_shard> key_space::get(size_t shard) {
        if (shards.empty()) {
            abort_with("shard configuration is empty");
        }
        auto r = shards[shard % shards.size()];
        if (r == nullptr) {
            abort_with("shard not found");
        }
        return r;
    }

    size_t key_space::get_shard_index(art::value_type key) {
        return get_shard_index(key.chars(), key.size);
    }
#if 0
    static uint64_t hash_fun(const char *str, size_t size) {
        uint64_t hash = 5381;
        int c;
        for (size_t s = 0; s < size; s++) {
            c = str[s];
            hash = ((hash << 5) + hash) + c; // hash * 33 + c
        }
        return hash;
    }
#else
    static uint64_t hash_fun(const char *str, size_t size) {
        //return ankerl::unordered_dense::detail::wyhash::hash(str, size);
        return a5hash(str,size,0);
    }
#endif
    size_t key_space::get_shard_index(const char* key, size_t key_len) {
        const size_t n = get_shard_count();
        if (n == 1) {
            return 0;
        }
        auto shard_key = art::value_type{key,key_len};

        if (opt_range_sharded) {
            // a binary search of at most shard_count boundaries, against a table that is
            // replaced rather than mutated, so this takes no lock and never sees a half
            // written one. It can still be overtaken by a rebalance - see route_moved
            return rindex.route(shard_key);
        }

        return hash_fun(shard_key.chars(), shard_key.size) % n;
    }

    bool key_space::is_stateful_sharding() const {
        // hash routing is a function of the key. range routing is not: the table
        // and which shard holds which key change while the space runs. SAVE,
        // LOAD, RELOAD and SAVEALL freeze the space when this is true. a later
        // method that can move a key returns true here. today only range does.
        return opt_range_sharded;
    }

    bool key_space::route_moved(art::value_type key, const shard_ptr& t) {
        if (!opt_range_sharded || !t) return false;
        return get_shard_index(key) != t->get_shard_number();
    }

    size_t key_space::get_shard_index(const std::string& key) {
        return get_shard_index(key.c_str(), key.size());
    }

    size_t key_space::get_shard_index(ValkeyModuleString **argv) {
        size_t nlen = 0;
        const char *n = ValkeyModule_StringPtrLen(argv[1], &nlen);
        if (key_ok(n, nlen) != 0) {
            abort_with("invalid shard key");
        }
        return get_shard_index(n,nlen);
    }

    shard_ptr key_space::get(ValkeyModuleString **argv) {
        return get(get_shard_index(argv));
    }

    shard_ptr key_space::get(art::value_type key) {
        return get(get_shard_index(key.chars(), key.size));
    }
    shard_ref key_space::get_ref(art::value_type key) {
        return get_ref(get_shard_index(key.chars(), key.size));
    }
    shard_ref key_space::get_ref(ValkeyModuleString **argv) {
        return get_ref(get_shard_index(argv));
    }

    [[nodiscard]] std::string key_space::get_name() const {
        return name;
    };
    [[nodiscard]] std::string key_space::get_canonical_name() const {
        return undecorate(name);
    };

    const heap::vector<shard_ptr>& key_space::get_shards() {
        return shards;
    };
    void key_space::merge(merge_options options) {
        merge(source(), options);
    }
    void key_space::each_shard(std::function<void(shard_ptr)> f) {
        for (auto& s: shards) {
            f(s);
        }
    }
    size_t key_space::get_shard_count() const {
        return shards.size();
    }
    size_t key_space::hash_buf_size() const {
        return 0;
    }

    bool key_space::buffer_insert(const std::string &key, const std::string &value) {
        try {
            auto fc = [&](const art::node_ptr &) -> void {};
            auto k = encode_key(art::value_type{key});
            auto v = art::value_type{value};
            auto t = this->get(v);
            key_options spec;
            spec.set_hashed(!opt_ordered_keys);
            storage_release r(t);
            t->opt_insert(spec,k.get_value(),v,true,fc);
            return true;
        }catch (std::exception& ) {
            return false;
        }
    }

    void key_space::merge(key_space_ptr into, merge_options options) {
        if (!into) return;
        for (auto &d : shards) {
            auto sn = d->get_shard_number();
            d->merge(into->get(sn),options);
        }
    }
    void key_space::depends(const key_space_ptr& source) {
        this->src = source;
        auto current = source;
        while (current && current.get() != this) {
            current = current->source();
        }
        if (current && current.get() == this) {
            throw_exception<std::invalid_argument>("cannot have cyclic dependencies");
        }
        for (auto &d : shards) {
            auto sn = d->get_shard_number();
            d->depends(source ? source->get(sn) : nullptr);
        }

    }
    key_space_ptr key_space::source() const {
        return this->src;
    }
} // barch