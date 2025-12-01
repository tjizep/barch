//
// Created by teejip on 11/30/25.
//

#ifndef BARCH_MERGE_OPTIONS_H
#define BARCH_MERGE_OPTIONS_H
#include <bitset>

struct merge_options {
    enum {
        merge_compressed = 1,
        merge_decompress = 2,
        merge_encrypt = 3
    };
    std::bitset<32> options{0};
    void set_compressed(bool val) {
        options[merge_compressed]  = val;
    }
    bool is_compressed() const {
        return options[merge_compressed];
    }
    bool is_decompress() const {
        return options[merge_decompress];
    }
};
#endif //BARCH_MERGE_OPTIONS_H