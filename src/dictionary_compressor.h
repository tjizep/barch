#ifndef DICTIONARY_COMPRESSOR_H
#define DICTIONARY_COMPRESSOR_H

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>
#include <zstd.h>

#include "configuration.h"
#include "sastam.h"
#include "value_type.h"

namespace barch { class key_space; }
// the key space, spelled out: key_space.h includes this header
typedef std::shared_ptr<barch::key_space> dictionary_space;
// not thread safe on purpose
class dictionary_compressor {
public:
    enum {
        dc_min_samples_size = 512000
    };
    using buffer_type = heap::vector<uint8_t>;

    dictionary_compressor(size_t min_samples_size = dc_min_samples_size);
    ~dictionary_compressor();

    // Compresses data with the dictionary. Empty when there isn't one yet, or when
    // zstd failed. Training is add_sample's job, not this one's.
    const buffer_type& compress(const buffer_type& data);
    const buffer_type& compress(art::value_type data);
    /**
     * Adds a training sample. Once there are enough (or `data` is empty, which
     * forces it), trains, puts the result in `trained` and answers true.
     *
     * It doesn't take the new dictionary into use. The caller saves it first, so
     * nothing gets compressed with a dictionary that isn't on disk - TODO 518.
     */
    bool add_sample(art::value_type data, buffer_type& trained);
    // clear current training data and start training again
    void clear();
    // Decompresses data using the trained dictionary. Throws when it can't: an
    // empty answer used to be taken for the value - TODO 518
    art::value_type decompress(art::value_type compressed_data);

    // Returns the dictionary if ready, otherwise empty.
    buffer_type get_dictionary();

    [[nodiscard]] bool is_dictionary_ready() const;
    void create_from_dictionary(const buffer_type& other);

    enum class load_result { missing, loaded, damaged };
    // a damaged file is reported, not quietly treated as no file - TODO 518
    load_result load_dictionary(const std::string& name);
    /**
     * What a shard file records to say which dictionary its values need. zstd's
     * own id when the dictionary has one (a trained one does, and every frame
     * compressed with it carries it too), otherwise a hash of the bytes. Never 0,
     * which means "no dictionary" in a shard file.
     */
    static uint32_t id_of(const buffer_type& dict);
    size_t remaining_sample_data_required() const;

    /*
     * TODO 518. Both guarded by dictionary::store::mut, not by this object.
     *
     * `required` is every dictionary the space's shard files said they need.
     * `blocked` is why this space may not train or compress, empty when it may:
     * set when the files need a dictionary this process doesn't have, so training
     * a new one can't take the place of the one the old values need. Files that
     * need different dictionaries are fine as long as the space has them all -
     * TODO 534.
     */
    std::set<uint32_t> required{};
    std::string blocked{};
    /*
     * Every dictionary the space has had, by id, the one in use among them -
     * TODO 534. A value is read with the one its zstd frame names, so a space can
     * hold values compressed with several: a RETRIEVE that installed only some
     * shards, or files from two sources after a crash between installs. Every
     * shard file carries these, so any mix of files brings what its values need.
     * Replaced, never changed in place, under `known_mut`, which is taken on its
     * own and never while taking another lock.
     */
    using known_map = std::map<uint32_t, buffer_type>;
    std::shared_ptr<const known_map> known{std::make_shared<const known_map>()};
    mutable std::mutex known_mut{};
    /** add one to `known`, true when it's new */
    bool add_known(const buffer_type& dict);
    [[nodiscard]] std::shared_ptr<const known_map> known_now() const;
    [[nodiscard]] bool knows(uint32_t id) const;
    /*
     * A dictionary that finished training and isn't in use yet - TODO 527. It's
     * stored in the space first, and that needs a latch the training path may
     * already hold, so persist_pending does it from where none is. Guarded by
     * get_dc_mut(), like the training.
     */
    buffer_type pending{};
    /*
     * Goes up when a space's dictionary is replaced outright, which only a
     * RETRIEVE does - TODO 521. Each thread's copy (get_dc) remembers the one
     * it was built from and is built again when they differ.
     */
    std::atomic<uint64_t> generation{0};
    uint64_t built_from{0};
    // `blocked` is non-empty, readable without the mutex for the hot paths
    std::atomic<bool> is_blocked{false};
    // id_of() the dictionary in use, 0 while there isn't one
    [[nodiscard]] uint32_t id() const { return current_id.load(std::memory_order_acquire); }
private:
    std::atomic<uint32_t> current_id{0};
    bool train_dictionary(buffer_type& out);

    size_t min_compressed_size = barch::get_min_compressed_size();
    size_t min_samples_size{};
    std::atomic<bool> dict_ready{false};

    // Training data buffer
    std::vector<size_t> sample_sizes;
    buffer_type training_samples;

    // ZSTD contexts
    ZSTD_CDict * cdict{nullptr};
    ZSTD_DDict * ddict{nullptr};
    ZSTD_DCtx  * dctx{nullptr};
    ZSTD_CCtx  * cctx{nullptr};

    // The raw dictionary blob
    buffer_type dictionary_data{};
    // Temp decompressed data to avoid allocations
    buffer_type decompressed{};
    // Temp compressed data to avoid allocations
    buffer_type compressed{};

};
/**
 * One dictionary per key space - see TODO 300.
 *
 * A dictionary trained across every space is trained on a mixture that none of
 * the data looks like: the shop's `geo` holds 88,009 near identical JSON
 * records, `shop` holds product records with long English titles, and `users`
 * holds salted hashes that will not compress at all. Training each space on its
 * own data is a different compression ratio, not a tidier config.
 *
 * Where it's kept, and how a space reads it, is below. Every shard file also
 * records the id of the dictionary its values need (`stamp`), and a load hands
 * that back (`require`) - TODO 518. A space whose files need a dictionary it
 * doesn't have stops training and compressing, so a fresh one can't take the
 * place of the one the old values need, and says what to put back.
 */
namespace dictionary {
    /*
     * The space's dictionary is a meta key in the space, `dict` - TODO 527. So it is
     * saved in the shard files, logged, backed up and copied by RETRIEVE with the
     * values that need it, and there's no file beside them to keep in step (TODO
     * 518, 521). It isn't sent to replicas: a replica gets values plain and keeps a
     * dictionary of its own, as it always did.
     *
     * The space name is required at every call rather than defaulted, so that adding
     * a call site is a compile error until someone has decided which space it
     * belongs to. Take it from the allocator that owns the leaf -
     * `n.logical.get_ap<alloc_pair>().name` - rather than from the caller's idea of
     * context, because that is the one source that cannot be wrong.
     *
     * Getting it wrong fails loudly rather than quietly: zstd records the dictionary
     * id in the frame, so decompressing with the wrong one is an error and
     * `dictionary_compressor::decompress` throws.
     *
     * Read once, never lazily: a value is decompressed under its shard's latch, and
     * reading the key there would take another. So a space loads its dictionary as
     * it opens, and again whenever its files are replaced underneath it.
     */
    typedef std::map<std::string, std::unique_ptr<dictionary_compressor>> mains_t;
    struct store {
        std::mutex mut{};
        mains_t mains{};
    };
    /** the meta key the dictionary is kept under */
    constexpr const char* meta_name = "dict";

    art::value_type decompress(const std::string& space, const art::value_type& data);
    // compresses data if ready, may return empty if it could not compress or if dictionary is ready
    // function may block if dictionary is training else uses thread local trained dictionary
    art::value_type compress(const std::string& space, art::value_type data);
    /**
     * Train this space's encoder on given data. When it has enough, the dictionary
     * is stored in the space and used before this returns. Answers what's still
     * needed, 0 once there's a dictionary.
     */
    size_t train(const dictionary_space& space, art::value_type data);
    /**
     * The space's trained dictionary, so a backup can carry it with the data. False
     * when the space has none yet. See TODO 415.
     */
    bool get(const std::string& space, dictionary_compressor::buffer_type& out);
    /**
     * Put a dictionary back, the other half of a restore. Stored in the space before
     * it's used. Refused when the space already has a different one - every value
     * compressed there needs that one - or when its files need another. The same one
     * again is fine.
     */
    bool set(const dictionary_space& space, art::value_type data, std::string& err);

    /**
     * Read the space's dictionary from its meta key, with no latch held: as the
     * space opens, and after a streaming load. A store from before TODO 527 has it
     * in `barch_dict_<space>.dat`, which is moved into the key here.
     */
    void load(const dictionary_space& space);
    /**
     * The same, with every shard of the space already held - LOAD and RETRIEVE,
     * which replace the files and must have the dictionary back before anything
     * reads through them.
     */
    void load_holding_lock(const dictionary_space& space);
    /**
     * Put `data` in as the space's dictionary with every shard held, and save the
     * shard that has it. For a RETRIEVE from a source older than TODO 527, whose
     * files don't carry the key but which sends the dictionary beside them.
     */
    bool install_holding_lock(const dictionary_space& space, const dictionary_compressor::buffer_type& data,
                              std::string& err);
    /**
     * The space's files are about to be replaced: forget what the old ones
     * needed. Not the dictionaries themselves - a shard that doesn't install
     * still needs its own - TODO 534.
     */
    void files_replaced(const std::string& space);
    /**
     * A shard file of this space carries dictionary `data` - TODO 534. It's kept
     * for reading the values compressed with it; the one in use doesn't change.
     */
    void carried(const std::string& space, const dictionary_compressor::buffer_type& data);
    /** every dictionary this space has, by id, for a shard file to carry - TODO 534 */
    std::shared_ptr<const dictionary_compressor::known_map> to_carry(const std::string& space);
    /** the space was cleared, its dictionary key with it */
    void forget(const std::string& space);
    /** whether a trained dictionary is waiting to be stored */
    bool has_pending(const std::string& space);
    /**
     * Store a dictionary that finished training while a latch was held, and start
     * using it. Called with no latch held - the space's maintenance tick.
     */
    void persist_pending(const dictionary_space& space);

    /**
     * The dictionary id a shard file of this space should record: the space's
     * dictionary if it has one, else whatever its files already needed, else 0.
     * Kept rather than dropped when the dictionary is missing, so a save can't
     * forget that the values need one - TODO 518.
     */
    uint32_t stamp(const std::string& space);
    /**
     * A shard file of this space said its values need dictionary `id`. 0 says
     * nothing. Checked against the space's dictionary once it's loaded; files that
     * disagree with each other block the space from training and compressing -
     * TODO 518.
     */
    void require(const std::string& space, uint32_t id);
}


#endif // DICTIONARY_COMPRESSOR_H