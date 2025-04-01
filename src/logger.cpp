//
// Created by linuxlite on 3/24/25.
//
#include <sys/unistd.h>
#include "logger.h"
#include <chrono>
#include <iostream>
#include <regex>
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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/color.h>
#pragma GCC diagnostic pop

void art::raw_write_to_log(fmt::string_view users_fmt, fmt::format_args&& args)
{
    size_t tid = gettid();
    auto now = std::chrono::system_clock::now();
    // %d %b %Y %H:%M:%OS
    auto header_color = fg(fmt::color::orange) | fmt::emphasis::italic;
    auto text_color = fg(fmt::color::light_blue) | fmt::emphasis::italic;
    std::string logged = fmt::format(header_color,"{}:M {:%d %b %Y %H:%M:%S} * BARCH ",tid, now);
    logged += fmt::vformat(text_color, users_fmt, args);
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
#endif


