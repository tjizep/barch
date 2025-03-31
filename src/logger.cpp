//
// Created by linuxlite on 3/24/25.
//
#include <sys/unistd.h>
#include "logger.h"
#include <chrono>
#include <iostream>
#include <string>
#include <string_view>

#if __cplusplus >= 202002L
#include <format>

void art::raw_write_to_log(std::string_view users_fmt, std::format_args&& args)
{
    size_t tid = gettid();
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::steady_clock::now().time_since_epoch().count();
    // %d %b %Y %H:%M:%OS
    std::string logged = std::format("{}:M {:%d %b %Y %H:%M:%OS}.{:03d} * BARCH ",tid, now,millis%1000);
    logged += std::vformat(users_fmt, args);
    std::clog <<  logged << '\n';
}

void art::log(const std::string& message, const std::exception& e)
{

    std_log(message, e.what());
}

void art::log(const std::exception& e, const std::string& file, int line)
{

    std_log(e.what(), file, line);
}
void art::log(const std::string& message)
{
    std_log(message);
}
#else


void art::log(const std::string& message, const std::exception& e)
{
    std::clog << message << " " << e.what() << '\n';
}

void art::log(const std::exception& e, const std::string& file, int line)
{
    std::clog << e.what() << " " <<  file << " " << line << '\n';
}
void art::log(const std::string& message)
{
    std::clog << message << '\n';
}
#endif


