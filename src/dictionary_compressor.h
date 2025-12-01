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
namespace dictionary {
    // decompresses data without blocking by using a thread local
    art::value_type decompress(const art::value_type& data);
    // compresses data if ready, may return empty if it could not compress or if dictionary is ready
    // function may block if dictionary is training else uses thread local trained dictionary
    art::value_type compress(art::value_type data);
    // train the current encoder on given data - the model is saved to
    size_t train(art::value_type data);
}


#endif // DICTIONARY_COMPRESSOR_H