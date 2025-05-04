//
// Created by teejip on 3/31/25.
//

#include "page_modifications.h"

std::atomic<uint32_t> page_modifications::flush_ticker[ticker_size]{};
