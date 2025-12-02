#include "dictionary_compressor.h"

#include <filesystem>
#include <fstream>
#include <zstd.h>
#include <zdict.h>

#include "ioutil.h"

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
    if (!data.empty() && data.size < min_compressed_size) {
        return compressed;
    }
    if (!dict_ready) {
        // Accumulate training data
        if (!data.empty()) {
            training_samples.insert(training_samples.end(), data.begin(), data.end());
            sample_sizes.push_back(data.size);
        }
        // Check if we have enough data to train
        if (data.empty() || training_samples.size() >= min_samples_size) {
            train_dictionary();
            // After training, we proceed to compress the current request
            // Note: Previous data sent while training returned empty and wasn't compressed.
            // Depending on requirements, we could store them and return them now, 
            // but the prompt says "initially add training data... then run dictionary creation".
        } else {
            return compressed; // Return empty vector while gathering training data
        }
    }
    if (data.empty()) {
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

void dictionary_compressor::train_dictionary() {
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
            barch::std_err("sample sizes do match total");
        }
        // Failed to train dictionary (e.g., not enough samples or entropy)
        // Reset to keep collecting? Or fail permanently? 
        // For this implementation, we'll clear and try again later.
        barch::std_err("Error creating dictionary:", ZSTD_getErrorName(dictSize));
        training_samples.clear();
        sample_sizes.clear();
        dictionary_data.clear();
        return;
    }

    // Resize dictionary to actual size generated
    dictionary_data.resize(dictSize);

    create_from_dictionary(dictionary_data);
}

art::value_type dictionary_compressor::decompress(art::value_type compressed_data) {

    if (!dict_ready || !ddict) {
        return {};
    }

    uint64_t rSize = ZSTD_getFrameContentSize(compressed_data.data(), compressed_data.size);
    
    if (ZSTD_isError(rSize)) {
        barch::std_err("decompression frame error",ZSTD_getErrorName(rSize));
        return {};
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
        barch::std_err("decompression error [",ZSTD_getErrorName(rSize),"]");
        return {};
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
        dict_ready = true;
        dictionary_data = other_dictionary_data;
        // Clear training data to free memory
        training_samples.clear();
        training_samples.shrink_to_fit();
        sample_sizes.clear();
        sample_sizes.shrink_to_fit();
    }

}
void dictionary_compressor::clear() {
    dict_ready = false;
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
const std::string& get_dict_file_name() {
    static std::string dict_file_name = "barch_dict.dat";
    return dict_file_name;
}
size_t dictionary_compressor::remaining_sample_data_required() const {
    if (is_dictionary_ready()) return 0;
    if (training_samples.size() < min_samples_size) {
        return min_samples_size - training_samples.size();
    }
    return 0;
}
void dictionary_compressor::save_dictionary(const std::string& name) {
    auto &dict_file_name = name;
    std::remove(dict_file_name.c_str());
    std::ofstream out{dict_file_name, std::ios::out | std::ios::binary}; // the wal file is truncated if it exists
    if (!out) {
        abort_with("could not save dictionary");
    }
    auto dict = get_dictionary();
    size_t size  =dict.size();
    writep(out, size);
    writep(out, dict.data(), dict.size());
    out.flush();
}
void dictionary_compressor::load_dictionary(const std::string &name) {
    std::ifstream f(name, std::ios::in | std::ios::binary);
    if (!f) {
        return ;
    }
    size_t ds = 0;
    readp(f, ds);
    buffer_type buff;
    buff.resize(ds);
    readp(f, buff.data(), buff.size());
    if (!f) {
        return;
    }
    create_from_dictionary(buff);
    if (!is_dictionary_ready()) {
        barch::std_err("ZSTD Dictionary had a format error and could not be loaded from",std::filesystem::current_path().c_str(),"/",name);
        abort_with("dictionary format error");
    }else {
        barch::std_log("loaded zstd dictionary from",std::filesystem::current_path().c_str(),"/",name);
    }
}

std::mutex& get_dc_mut() {
    static std::mutex m;
    return m;
}
dictionary_compressor& get_main(bool load = true) {
    static auto dict_loaded = false;
    static dictionary_compressor dc;
    if (!load) dict_loaded = true;
    if (!dict_loaded) {
        std::lock_guard l(get_dc_mut());
        if (dict_loaded) {
            return dc;
        }
        dc.load_dictionary(get_dict_file_name());
        dict_loaded = true;

    }
    return dc;
}

dictionary_compressor& get_dc() {
    thread_local dictionary_compressor dc;
    return dc;
}
namespace dictionary {

    art::value_type decompress(const art::value_type& data) {
        if (get_main().is_dictionary_ready()) {
            if (!get_dc().is_dictionary_ready()) {
                std::lock_guard l(get_dc_mut());
                get_dc().create_from_dictionary(get_main().get_dictionary());
            }
            return get_dc().decompress(data);
        }
        return {};
    }
    art::value_type compress(art::value_type data) {
        if (!barch::get_compression_enabled()) {
            return {};
        }
        if (get_main().is_dictionary_ready()) {
            if (!get_dc().is_dictionary_ready()) {
                std::lock_guard l(get_dc_mut());
                get_dc().create_from_dictionary(get_main().get_dictionary());
            }
            auto& compressed = get_dc().compress(data);
            return {compressed.data(), compressed.size()};
        }else {
            std::lock_guard l(get_dc_mut());
            // check if dictionary is saved
            bool wasnt_ready = false;
            if (!get_main().is_dictionary_ready()) {
                get_main().compress(data);
                wasnt_ready = true;
            }
            if (get_main().is_dictionary_ready()) {
                get_dc().create_from_dictionary(get_main().get_dictionary());
                // save the dictionary once
                if (wasnt_ready) {
                    get_main().save_dictionary(get_dict_file_name());
                }
            }
            return {};
        }
    }
    size_t train(art::value_type data) {
        std::lock_guard l(get_dc_mut());
        auto& dc = get_main(false);
        // check if dictionary is saved
        if (dc.is_dictionary_ready()) {
            dc.clear();
        }

        dc.compress(data);
        if (dc.is_dictionary_ready()) {
            dc.save_dictionary(get_dict_file_name());
        }

        return dc.remaining_sample_data_required();
    }
};
