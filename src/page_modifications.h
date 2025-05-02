//
// Created by teejip on 3/31/25.
//

#ifndef PAGE_MODIFICATIONS_H
#define PAGE_MODIFICATIONS_H
#include <cstddef>
#include <cstdint>
#include <atomic>

#include "constants.h"

struct page_modifications {
    static std::atomic<uint32_t> flush_ticker[ticker_size];
    static void inc_ticker(size_t page) {
        ++flush_ticker[page & (ticker_size-1)];
    }
    static void inc_all_tickers() {
        for (size_t i = 0; i < ticker_size; ++i) {
            ++flush_ticker[i];
        }

    }
    static uint32_t get_ticker(size_t page) {
        return flush_ticker[page & (ticker_size-1)];
    }
};



#endif //PAGE_MODIFICATIONS_H
