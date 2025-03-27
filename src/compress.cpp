//
// Created by linuxlite on 2/7/25.
//

#include "compress.h"
#include<iostream>
#include<fstream>
#include <logger.h>
#include <bits/fs_fwd.h>

uint32_t compress::flush_ticker = 0;
std::mutex compress::mutex{};
/// prevents other threads from allocating memory while vacuum is taking place
/// it must be entered and left before the allocation mutex to prevent deadlocks
std::shared_mutex compress::vacuum_scope{};
