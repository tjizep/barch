//
// Created by linuxlite on 2/7/25.
//

#include "compress.h"

uint32_t compress::flush_ticker = 0;
std::shared_mutex compress::mutex{};
