#ifndef DICTIONARY_COMPRESSOR_H
#define DICTIONARY_COMPRESSOR_H

#include <vector>
#include <zstd.h>

#include "configuration.h"
#include "sastam.h"
#include "value_type.h"
// not thread safe on purpose
class dictionary_compressor {
public:
    enum {
        dc_min_samples_size = 512000
    };
    using buffer_type = heap::vector<uint8_t>;

    dictionary_compressor(size_t min_samples_size = dc_min_samples_size);
    ~dictionary_compressor();

    // Compresses data. If dictionary is not ready, adds to training data.
    // Returns compressed data if dictionary is ready, otherwise empty vector.
    const buffer_type& compress(const buffer_type& data);
    const buffer_type& compress(art::value_type data);
    // clear current training data and start training again
    void clear();
    // Decompresses data using the trained dictionary.
    art::value_type decompress(art::value_type compressed_data);

    // Returns the dictionary if ready, otherwise empty.
    buffer_type get_dictionary();

    [[nodiscard]] bool is_dictionary_ready() const;
    void create_from_dictionary(const buffer_type& other);
    void save_dictionary(const std::string& name);
    void load_dictionary(const std::string& name);
    size_t remaining_sample_data_required() const;
private:
    void train_dictionary();

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
 * The space name is required at every call rather than defaulted, so that
 * adding a call site is a compile error until someone has decided which space
 * it belongs to. Take it from the allocator that owns the leaf -
 * `n.logical.get_ap<alloc_pair>().name` - rather than from the caller's idea of
 * context, because that is the one source that cannot be wrong.
 *
 * Getting it wrong fails loudly rather than quietly: zstd records the
 * dictionary id in the frame, so decompressing with the wrong one is an error
 * and `dictionary_compressor::decompress` logs it and answers empty.
 *
 * Each space's dictionary is saved as `barch_dict_<space>.dat`. A space with no
 * such file falls back to the old single `barch_dict.dat` if there is one, so a
 * store written before this change still reads back.
 */
namespace dictionary {
    // decompresses data without blocking by using a thread local
    art::value_type decompress(const std::string& space, const art::value_type& data);
    // compresses data if ready, may return empty if it could not compress or if dictionary is ready
    // function may block if dictionary is training else uses thread local trained dictionary
    art::value_type compress(const std::string& space, art::value_type data);
    // train this space's encoder on given data - the model is saved per space
    size_t train(const std::string& space, art::value_type data);
}


#endif // DICTIONARY_COMPRESSOR_H