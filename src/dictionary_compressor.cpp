#include "dictionary_compressor.h"
#include "data_dir.h"
#include "key_space.h"
#include <chrono>
#include "composite.h"
#include "sharded_store.h"
#include "meta_keys.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <fstream>
#include <zstd.h>
#include <zdict.h>

#include "constants.h"
#include "hash_arena.h"
#include "ioutil.h"
#include "statistics.h"

dictionary_compressor::dictionary_compressor(size_t min_samples_size)
    :   min_samples_size(min_samples_size)
{
    dctx = ZSTD_createDCtx();
    cctx = ZSTD_createCCtx();
}

dictionary_compressor::~dictionary_compressor() {
    if (cdict) ZSTD_freeCDict(cdict);
    if (ddict) ZSTD_freeDDict(ddict);
    if (cctx) ZSTD_freeCCtx(cctx);
    if (dctx) ZSTD_freeDCtx(dctx);
}
const dictionary_compressor::buffer_type& dictionary_compressor::compress(const buffer_type& data) {

    return compress({data.data(), data.size()});
}
const dictionary_compressor::buffer_type& dictionary_compressor::compress(art::value_type data) {
    compressed.clear();
    if (data.empty() || data.size < min_compressed_size) {
        return compressed;
    }
    if (dict_ready && cdict && barch::get_compression_enabled()) {
        size_t bound = ZSTD_compressBound(data.size);
        compressed.resize(bound);
        
        size_t cSize = ZSTD_compress_usingCDict(
            cctx,
            compressed.data(),
            compressed.size(),
            data.data(),
            data.size,
            cdict
        );

        if (ZSTD_isError(cSize)) {
            // Handle error, for now returning empty
            return compressed;
        }

        compressed.resize(cSize);
        return compressed;
    }

    return compressed;
}

bool dictionary_compressor::add_sample(art::value_type data, buffer_type& trained) {
    if (dict_ready)
        return false;
    if (!data.empty() && data.size < min_compressed_size)
        return false;
    if (!data.empty()) {
        training_samples.insert(training_samples.end(), data.begin(), data.end());
        sample_sizes.push_back(data.size);
    }
    // an empty sample forces it, which is what TRAIN with nothing after it does
    if (!data.empty() && training_samples.size() < min_samples_size)
        return false;
    const bool ok = train_dictionary(trained);
    // used or not, these samples are done with. If the dictionary can't be
    // saved, training starts over rather than retrying on every call
    training_samples.clear();
    training_samples.shrink_to_fit();
    sample_sizes.clear();
    sample_sizes.shrink_to_fit();
    return ok;
}

bool dictionary_compressor::train_dictionary(buffer_type& dictionary_data) {
    dictionary_data.resize(training_samples.size()/10);

    // ZSTD_trainFromBuffer requires samples to be concatenated in a single buffer
    // and an array of sizes. We already have this structure.

    size_t dictSize = ZDICT_trainFromBuffer(
        dictionary_data.data(),
        dictionary_data.size(),
        training_samples.data(),
        sample_sizes.data(),
        sample_sizes.size()
    );

    if (ZSTD_isError(dictSize)) {
        size_t total_sizes = 0;
        for (auto s: sample_sizes) {
            total_sizes += s;
        }
        if (total_sizes != training_samples.size()) {
            barch::err({"sample sizes do match total"});
        }
        // Failed to train dictionary (e.g., not enough samples or entropy)
        // Reset to keep collecting? Or fail permanently? 
        // For this implementation, we'll clear and try again later.
        barch::err({"Error creating dictionary:", ZSTD_getErrorName(dictSize)});
        dictionary_data.clear();
        return false;
    }

    // Resize dictionary to actual size generated
    dictionary_data.resize(dictSize);
    return true;
}

art::value_type dictionary_compressor::decompress(art::value_type compressed_data) {

    /*
     * Every failure throws - TODO 518. This used to log and answer empty, and
     * the callers took the empty answer for the value: GET said "", APPEND
     * wrote back only what it appended, and a replica was sent "". No compressed
     * value is ever empty, since compress() never takes an empty one.
     */
    const auto fail = [](const std::string& why) {
        ++statistics::exceptions_raised;
        throw std::runtime_error("could not decompress a value: " + why);
    };
    if (!dict_ready || !ddict) {
        fail("there is no dictionary");
    }

    const unsigned long long rSize = ZSTD_getFrameContentSize(compressed_data.data(), compressed_data.size);
    if (rSize == ZSTD_CONTENTSIZE_ERROR || rSize == ZSTD_CONTENTSIZE_UNKNOWN) {
        fail("not a zstd frame with a size");
    }
    // no leaf holds more than this, so a bigger claim is a damaged frame, and
    // resizing to it would be the damage asking for memory
    if (rSize == 0 || rSize > maximum_allocation_size) {
        fail("the frame says " + std::to_string(rSize) + " bytes, which no value is");
    }
    const unsigned frame_dict = ZSTD_getDictID_fromFrame(compressed_data.data(), compressed_data.size);
    const unsigned ours = ZSTD_getDictID_fromDDict(ddict);
    if (frame_dict != 0 && frame_dict != ours) {
        fail("it was compressed with dictionary " + std::to_string(frame_dict)
             + " and this space has " + std::to_string(ours));
    }
    decompressed.resize(rSize);

    size_t dSize = ZSTD_decompress_usingDDict(
        dctx,
        decompressed.data(),
        decompressed.size(),
        compressed_data.data(),
        compressed_data.size,
        ddict
    );

    if (ZSTD_isError(dSize)) {
        fail(ZSTD_getErrorName(dSize));
    }

    return {decompressed.data(), decompressed.size()};
}

void dictionary_compressor::create_from_dictionary(const buffer_type& other_dictionary_data) {

    if (cdict) ZSTD_freeCDict(cdict);
    if (ddict) ZSTD_freeDDict(ddict);
    dict_ready = false;

    // Create Contexts
    cdict = ZSTD_createCDict(other_dictionary_data.data(), other_dictionary_data.size(), 3); // Compression level 3
    ddict = ZSTD_createDDict(other_dictionary_data.data(), other_dictionary_data.size());

    if (cdict && ddict) {
        dictionary_data = other_dictionary_data;
        current_id.store(id_of(dictionary_data), std::memory_order_release);
        dict_ready = true;
        // Clear training data to free memory
        training_samples.clear();
        training_samples.shrink_to_fit();
        sample_sizes.clear();
        sample_sizes.shrink_to_fit();
    }

}
void dictionary_compressor::clear() {
    dict_ready = false;
    current_id.store(0, std::memory_order_release);
    if (cdict) ZSTD_freeCDict(cdict);
    if (ddict) ZSTD_freeDDict(ddict);
    cdict = nullptr;
    ddict = nullptr;
    sample_sizes.clear();
    training_samples.clear();
}

dictionary_compressor::buffer_type dictionary_compressor::get_dictionary() {
    if (dict_ready) {
        return dictionary_data;
    }
    return {};
}

bool dictionary_compressor::is_dictionary_ready() const {
    // Mutex not strictly necessary for a bool read depending on architecture, 
    // but good practice for consistency.
    // We'll skip lock here for const correctness/performance trade-off 
    // or user can rely on get_dictionary() returning empty.
    return dict_ready;
}
// in the data directory beside the shard files, not the working one - TODO 526
const std::string& get_legacy_dict_file_name() {
    static const auto* dict_file_name = new std::string(barch::data_path("barch_dict.dat"));
    return *dict_file_name;    // never destroyed - TODO 57
}
size_t dictionary_compressor::remaining_sample_data_required() const {
    if (is_dictionary_ready()) return 0;
    if (training_samples.size() < min_samples_size) {
        return min_samples_size - training_samples.size();
    }
    return 0;
}

dictionary_compressor::load_result dictionary_compressor::load_dictionary(const std::string &name) {
    std::ifstream f(name, std::ios::in | std::ios::binary | std::ios::ate);
    if (!f) {
        return load_result::missing;
    }
    /*
     * A length and that many bytes, and nothing else. A file that's shorter or
     * longer is damaged, and says so - TODO 518. It used to be read as "no
     * dictionary", and the space then trained a new one and saved it over this
     * one, which was the end of every value compressed with it.
     */
    const auto file_size = (uint64_t) f.tellg();
    f.seekg(0);
    // far more than training makes, which is a tenth of the samples
    constexpr uint64_t largest = 64ull << 20;
    size_t ds = 0;
    readp(f, ds);
    if (!f || ds == 0 || ds > largest || file_size != sizeof(ds) + ds) {
        barch::err({"the dictionary in", name,
                    "is damaged: it is", file_size, "bytes and says it holds", ds});
        return load_result::damaged;
    }
    buffer_type buff;
    buff.resize(ds);
    readp(f, buff.data(), buff.size());
    if (!f) {
        barch::err({"could not read the dictionary in", name});
        return load_result::damaged;
    }
    create_from_dictionary(buff);
    if (!is_dictionary_ready()) {
        barch::err({"ZSTD Dictionary had a format error and could not be loaded from", name});
        return load_result::damaged;
    }
    barch::log({"loaded zstd dictionary from", name});
    return load_result::loaded;
}

bool dictionary_compressor::add_known(const buffer_type& dict) {
    if (dict.empty())
        return false;
    const uint32_t id = id_of(dict);
    std::lock_guard l(known_mut);
    if (known->count(id))
        return false;
    auto grown = std::make_shared<known_map>(*known);
    grown->emplace(id, dict);
    known = std::move(grown);
    return true;
}

std::shared_ptr<const dictionary_compressor::known_map> dictionary_compressor::known_now() const {
    std::lock_guard l(known_mut);
    return known;
}

bool dictionary_compressor::knows(uint32_t id) const {
    return known_now()->count(id) != 0;
}

uint32_t dictionary_compressor::id_of(const buffer_type& dict) {
    if (dict.empty())
        return 0;
    if (const unsigned id = ZSTD_getDictID_fromDict(dict.data(), dict.size()); id != 0)
        return id;
    // a raw content dictionary has no id of its own, so FNV-1a over the bytes
    uint32_t h = 2166136261u;
    for (auto b : dict) {
        h ^= b;
        h *= 16777619u;
    }
    return h ? h : 1;
}

/*
 * Per space dictionaries - TODO 300.
 *
 * `mains()` holds the trained dictionary for each space and is the thing that
 * gets saved; `get_dc(space)` is a thread local derived from it, so the hot
 * path takes no lock once a space's dictionary is ready. Two mutexes on
 * purpose: `mains_mut()` guards only the map, and `get_dc_mut()` guards
 * training, because the training path calls into the map and a single mutex
 * would be a self deadlock.
 */
/*
 * Who owns these, and why it matters - TODO 330.
 *
 * `mains` is reached from the maintenance thread, through
 * `run_compress_cold_keys` -> `dictionary::compress` -> `get_main`, and that
 * thread can outlive a function-local static: `mains` is built on the first
 * compression, the key space registry in key_space.cpp is built on the first
 * `get_keyspace`, statics are destroyed in reverse order of construction, and
 * the thread is only joined when `~key_space` runs as that registry goes. So
 * `mains` was destroyed while the clock was still ticking, which TSan reported
 * as twenty one heap-use-after-frees at the end of a test - every one inside a
 * correctly held lock, because the lock was never the problem.
 *
 * The fix is ownership rather than immortality: `dictionary::shutdown()` empties
 * this map, and the registry calls it after it has destroyed every space, which
 * is after every maintenance thread has been joined. The statics below are then
 * destroyed in whatever order the runtime likes, holding nothing, with no thread
 * left to read them.
 */
std::mutex& get_dc_mut() {
    static std::mutex m;
    return m;
}
/*
 * Stops a space training and compressing, and says why, once - TODO 518. Called
 * with the store's mutex held.
 */
static void block(dictionary_compressor& main, const std::string& space, const std::string& why) {
    if (main.is_blocked.load(std::memory_order_relaxed))
        return;
    main.blocked = why;
    main.is_blocked.store(true, std::memory_order_release);
    barch::err({"space", space, "will not train a dictionary or compress anything:", why,
                "- its compressed values can't be read until the right dictionary is back."
                " Put it back with DICTIONARY SET, or RETRIEVE the space from a copy that has it"});
}

/*
 * Blocked or not, once both the dictionary the space has and the one its files
 * need are known - TODO 518. Called with the store's mutex held.
 */
static void check_locked(dictionary_compressor& main, const std::string& space) {
    // any the files need that the space has neither in use nor kept - TODO 534
    std::string missing;
    for (uint32_t need : main.required) {
        if (need == main.id() || main.knows(need))
            continue;
        missing += (missing.empty() ? "" : ", ") + std::to_string(need);
    }
    if (missing.empty()) {
        main.blocked.clear();
        main.is_blocked.store(false, std::memory_order_release);
        return;
    }
    block(main, space, "its shard files need dictionary " + missing + " and it doesn't have it");
}

dictionary_compressor& get_main(const std::string& space) {
    // the store belongs to the key space registry, which outlives the maintenance
    // threads that come through here. Map values are unique_ptr so the references
    // handed out stay valid as the map grows. Nothing is read here: a space loads
    // its dictionary as it opens - see dictionary::load
    auto& store = barch::ks_dictionaries();
    auto& mains = store.mains;
    std::lock_guard l(store.mut);
    auto i = mains.find(space);
    if (i != mains.end()) return *i->second;
    auto dc = std::make_unique<dictionary_compressor>();
    auto& ref = *dc;
    mains.emplace(space, std::move(dc));
    return ref;
}

dictionary_compressor& get_dc(const std::string& space) {
    thread_local std::map<std::string, std::unique_ptr<dictionary_compressor>> dcs;
    auto i = dcs.find(space);
    if (i != dcs.end()) return *i->second;
    auto dc = std::make_unique<dictionary_compressor>();
    auto& ref = *dc;
    dcs.emplace(space, std::move(dc));
    return ref;
}

/*
 * This thread's copy of the space's dictionary, built again when it's missing
 * or when the space's was replaced since - TODO 521. The generation is one
 * atomic load on the way through.
 */
static dictionary_compressor& fresh_dc(const std::string& space, dictionary_compressor& main) {
    auto& dc = get_dc(space);
    const uint64_t gen = main.generation.load(std::memory_order_acquire);
    if (!dc.is_dictionary_ready() || dc.built_from != gen) {
        std::lock_guard l(get_dc_mut());
        dc.create_from_dictionary(main.get_dictionary());
        dc.built_from = main.generation.load(std::memory_order_acquire);
    }
    return dc;
}

/** zstd takes it, or it isn't a dictionary at all */
static bool usable(const dictionary_compressor::buffer_type& d) {
    dictionary_compressor probe(0);
    probe.create_from_dictionary(d);
    return probe.is_dictionary_ready();
}

/*
 * The space's dictionary is now `bytes`, empty for none - TODO 527. Every thread's
 * copy is rebuilt when it changed, and blocked or not is worked out again.
 */
static void take(const std::string& space, const dictionary_compressor::buffer_type& bytes) {
    auto& main = get_main(space);
    {
        std::lock_guard l(get_dc_mut());
        const bool have = main.is_dictionary_ready();
        if (bytes.empty()) {
            if (have) {
                main.clear();
                main.generation.fetch_add(1, std::memory_order_acq_rel);
            }
        } else if (!have || main.get_dictionary() != bytes) {
            main.create_from_dictionary(bytes);
            if (!main.is_dictionary_ready())
                barch::err({"space", space, "has a dictionary key zstd won't take as a dictionary"});
            main.generation.fetch_add(1, std::memory_order_acq_rel);
        }
        main.pending.clear();
    }
    std::lock_guard sl(barch::ks_dictionaries().mut);
    check_locked(main, space);
}

/*
 * The dictionary key, written and on disk before anything is compressed with it -
 * TODO 518's rule, which the file's save used to keep. With a change log the
 * record is synced; without one, the space is saved. The whole space rather than
 * the shard that holds the key: a shard may not save alone after a write that
 * changed several, a FLUSHDB among them (TODO 519), and a dictionary is stored
 * about once in a space's life.
 */
static bool store_durably(const dictionary_space& space, const dictionary_compressor::buffer_type& d,
                          std::string& err) {
    const std::string v(d.begin(), d.end());
    if (!barch::meta::set(space, dictionary::meta_name, v, err))
        return false;
    if (const auto& log = space->get_change_log()) {
        try {
            log->sync();
        } catch (const std::exception& e) {
            err = std::string("the change log could not be synced: ") + e.what();
            return false;
        }
        return true;
    }
    barch::sharded_store store(space);
    if (store.save_space() != 0) {
        err = "the space could not be saved";
        return false;
    }
    return true;
}

/*
 * A store from before TODO 527 keeps the dictionary in a file beside the shard
 * files: `barch_dict_<space>.dat`, or before TODO 300 one `barch_dict.dat` for
 * every space. The space's own file is moved into its key and set aside. The
 * shared one is left where it is, since other spaces may need it too, and is only
 * stored in a space whose files say they need it - otherwise it's used as it was
 * before, from the file.
 */
static dictionary_compressor::buffer_type from_files(const dictionary_space& space,
                                                     const std::string& name) {
    const std::string own = barch::data_path("barch_dict_" + name + ".dat");
    dictionary_compressor probe(0);
    auto r = probe.load_dictionary(own);
    if (r == dictionary_compressor::load_result::damaged)
        return {};              // reported by the load; the files' stamp blocks the space
    if (r == dictionary_compressor::load_result::loaded) {
        auto d = probe.get_dictionary();
        std::string err;
        if (!store_durably(space, d, err)) {
            barch::err({"could not move the dictionary in", own, "into space", name, "-", err,
                        "- it's used from the file for now"});
            return d;
        }
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        const std::string aside = own + ".moved-" + std::to_string(ms);
        if (std::rename(own.c_str(), aside.c_str()) == 0)
            arena::sync_dir_of(aside);
        barch::log({"moved the dictionary in", own, "into space", name, "- the file is now", aside});
        return d;
    }
    dictionary_compressor legacy(0);
    if (legacy.load_dictionary(get_legacy_dict_file_name()) != dictionary_compressor::load_result::loaded)
        return {};
    auto d = legacy.get_dictionary();
    uint32_t need = 0;
    {
        auto& main = get_main(name);
        std::lock_guard l(barch::ks_dictionaries().mut);
        need = main.required_id;
    }
    if (need != 0 && need == dictionary_compressor::id_of(d)) {
        std::string err;
        if (store_durably(space, d, err))
            barch::log({"stored the shared dictionary in", get_legacy_dict_file_name(), "in space", name});
        else
            barch::err({"could not store the shared dictionary in space", name, "-", err});
    }
    return d;
}

namespace dictionary {

    art::value_type decompress(const std::string& space, const art::value_type& data) {
        auto& main = get_main(space);
        if (!main.is_dictionary_ready()) {
            // throws rather than answering empty, which callers took for the
            // value - TODO 518
            ++statistics::exceptions_raised;
            throw std::runtime_error("could not decompress a value: space " + space
                                     + " has no dictionary");
        }
        return fresh_dc(space, main).decompress(data);
    }

    art::value_type compress(const std::string& space, art::value_type data) {
        if (!barch::get_compression_enabled()) {
            return {};
        }
        auto& main = get_main(space);
        // its files need a dictionary it doesn't have - TODO 518
        if (main.is_blocked.load(std::memory_order_acquire)) {
            return {};
        }
        if (main.is_dictionary_ready()) {
            auto& compressed = fresh_dc(space, main).compress(data);
            return {compressed.data(), compressed.size()};
        }
        // still training: this feeds it a sample and answers empty, which every
        // caller treats as "leave this value alone". A dictionary that's done waits
        // in `pending` until it's stored in the space - see persist_pending
        std::lock_guard l(get_dc_mut());
        if (!main.is_dictionary_ready() && main.pending.empty()) {
            dictionary_compressor::buffer_type trained;
            if (main.add_sample(data, trained)) {
                if (usable(trained))
                    main.pending = std::move(trained);
                else
                    barch::err({"a dictionary trained for space", space, "isn't one zstd will take"});
            }
        }
        return {};
    }

    size_t train(const dictionary_space& space, art::value_type data) {
        const std::string name = space->get_name();
        {
            std::lock_guard l(get_dc_mut());
            auto& dc = get_main(name);
            if (dc.is_blocked.load(std::memory_order_acquire)) {
                std::string why;
                {
                    std::lock_guard sl(barch::ks_dictionaries().mut);
                    why = dc.blocked;
                }
                ++statistics::exceptions_raised;
                throw std::runtime_error("space " + name + " will not train: " + why);
            }
            if (dc.is_dictionary_ready()) {
                // the dictionary needs to be cleared with the data
                // that's if there is compressed data
                return 0;
            }
            if (dc.pending.empty()) {
                dictionary_compressor::buffer_type trained;
                if (dc.add_sample(data, trained)) {
                    if (!usable(trained)) {
                        ++statistics::exceptions_raised;
                        throw std::runtime_error("the dictionary trained for space " + name
                                                 + " isn't one zstd will take");
                    }
                    dc.pending = std::move(trained);
                }
            }
            if (dc.pending.empty())
                return dc.remaining_sample_data_required();
        }
        // no latch is held here, so it's stored and used now rather than next tick
        persist_pending(space);
        if (!get_main(name).is_dictionary_ready()) {
            ++statistics::exceptions_raised;
            throw std::runtime_error("the dictionary trained for space " + name
                                     + " could not be stored yet - it's kept and tried again");
        }
        return 0;
    }

    bool get(const std::string& space, dictionary_compressor::buffer_type& out) {
        // the training lock, because create_from_dictionary replaces the bytes
        // this copies
        std::lock_guard l(get_dc_mut());
        auto& main = get_main(space);
        if (!main.is_dictionary_ready()) return false;
        out = main.get_dictionary();
        return true;
    }

    bool set(const dictionary_space& space, art::value_type data, std::string& err) {
        if (data.empty()) {
            err = "an empty dictionary";
            return false;
        }
        const std::string name = space->get_name();
        dictionary_compressor::buffer_type want(data.begin(), data.end());
        {
            std::lock_guard l(get_dc_mut());
            auto& main = get_main(name);
            if (main.is_dictionary_ready()) {
                if (main.get_dictionary() == want) return true;
                err = "this space already has a different dictionary";
                return false;
            }
            if (!usable(want)) {
                err = "zstd would not take that as a dictionary";
                return false;
            }
            const uint32_t id = dictionary_compressor::id_of(want);
            std::lock_guard sl(barch::ks_dictionaries().mut);
            if (main.required_id != 0 && main.required_id != id) {
                err = "this space's files need dictionary " + std::to_string(main.required_id)
                      + " and that one is " + std::to_string(id);
                return false;
            }
        }
        // stored before it's used
        if (!store_durably(space, want, err))
            return false;
        take(name, want);
        return true;
    }

    void load(const dictionary_space& space) {
        if (!space)
            return;
        const std::string name = space->get_name();
        std::string raw;
        dictionary_compressor::buffer_type bytes;
        if (barch::meta::get(space, meta_name, raw))
            bytes.assign(raw.begin(), raw.end());
        else
            bytes = from_files(space, name);
        take(name, bytes);
    }

    void load_holding_lock(const dictionary_space& space) {
        if (!space)
            return;
        composite q;
        auto k = barch::meta::key(q, meta_name);
        barch::sharded_store store(space);
        dictionary_compressor::buffer_type bytes;
        if (auto t = store.shard_for(k)) {
            auto n = t->search(k);
            if (!n.null() && n.is_leaf) {
                auto v = n.const_leaf()->get_value();
                bytes.assign(v.begin(), v.end());
            }
        }
        take(space->get_name(), bytes);
    }

    bool install_holding_lock(const dictionary_space& space, const dictionary_compressor::buffer_type& data,
                              std::string& err) {
        if (!usable(data)) {
            err = "zstd would not take that as a dictionary";
            return false;
        }
        composite q;
        auto k = barch::meta::key(q, meta_name);
        barch::sharded_store store(space);
        auto t = store.shard_for(k);
        if (!t) {
            err = "no shard for it";
            return false;
        }
        try {
            art::key_options opts;
            opts.set_hashed(!t->opt_ordered_keys);
            auto fc = [](const art::node_ptr&) -> void {};
            t->opt_insert(opts, k, art::value_type{data.data(), data.size()}, true, fc);
        } catch (const std::exception& e) {
            err = e.what();
            return false;
        }
        if (!t->save_holding_lock(true)) {
            err = "the shard that holds it could not be saved";
            return false;
        }
        take(space->get_name(), data);
        return true;
    }

    void files_replaced(const std::string& space) {
        auto& main = get_main(space);
        std::lock_guard l(barch::ks_dictionaries().mut);
        main.required_id = 0;
        main.files_disagree = false;
    }

    void forget(const std::string& space) {
        auto& main = get_main(space);
        {
            std::lock_guard l(get_dc_mut());
            if (main.is_dictionary_ready() || !main.pending.empty()) {
                main.clear();
                main.pending.clear();
                main.generation.fetch_add(1, std::memory_order_acq_rel);
            }
        }
        std::lock_guard l(barch::ks_dictionaries().mut);
        main.required_id = 0;
        main.files_disagree = false;
        main.blocked.clear();
        main.is_blocked.store(false, std::memory_order_release);
    }

    bool has_pending(const std::string& space) {
        auto& main = get_main(space);
        std::lock_guard l(get_dc_mut());
        return !main.pending.empty() && !main.is_dictionary_ready();
    }

    void persist_pending(const dictionary_space& space) {
        if (!space)
            return;
        const std::string name = space->get_name();
        auto& main = get_main(name);
        dictionary_compressor::buffer_type want;
        {
            std::lock_guard l(get_dc_mut());
            if (main.pending.empty() || main.is_dictionary_ready())
                return;
            want = main.pending;
        }
        std::string err;
        if (!store_durably(space, want, err)) {
            // kept, and tried again on the next tick
            barch::err({"a dictionary trained for space", name, "is not in use yet:", err});
            return;
        }
        take(name, want);
        barch::log({"space", name, "trained a dictionary and stored it, id",
                    dictionary_compressor::id_of(want)});
    }

    uint32_t stamp(const std::string& space) {
        auto& main = get_main(space);
        std::lock_guard l(barch::ks_dictionaries().mut);
        // what the files already needed wins: when the space is blocked, the
        // dictionary it has (if any) is the wrong one, and a save must not
        // forget the right one
        return main.required_id ? main.required_id : main.id();
    }

    void require(const std::string& space, uint32_t id) {
        if (id == 0)
            return;         // a file from before TODO 518, or a space with no dictionary
        auto& main = get_main(space);
        std::lock_guard l(barch::ks_dictionaries().mut);
        if (main.required_id == 0) {
            main.required_id = id;
        } else if (main.required_id != id) {
            main.files_disagree = true;
            block(main, space, "its shard files need two different dictionaries, "
                  + std::to_string(main.required_id) + " and " + std::to_string(id));
            return;
        }
        // whether the space has it is checked once its dictionary is loaded
        if (main.is_dictionary_ready())
            check_locked(main, space);
    }
}
