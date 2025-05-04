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

void art::raw_start_log(bool err) {
    size_t tid = gettid();
    auto now = std::chrono::system_clock::now();
    // %d %b %Y %H:%M:%OS
    std::string logged;
    fmt::text_style header_color;
    fmt::text_style text_color;
    if (err) {
        text_color = fg(fmt::color::burly_wood) | fmt::emphasis::italic;
        header_color = fg(fmt::color::white_smoke) | fmt::emphasis::italic;
        logged = fmt::format(header_color, "{}:E {:%d %b %Y %H:%M:%S} * BARCH ", tid,
                             std::chrono::floor<std::chrono::milliseconds>(now));
    } else {
        text_color = fg(fmt::color::burly_wood);
        header_color = fg(fmt::color::white_smoke);
        logged = fmt::format(header_color, "{}:M {:%d %b %Y %H:%M:%S} * BARCH ", tid,
                             std::chrono::floor<std::chrono::milliseconds>(now));
    }
    std::clog << logged;
}

void art::raw_continue_log(bool err, fmt::string_view users_fmt, fmt::format_args &&args) {
    fmt::text_style text_color;
    if (err) {
        text_color = fg(fmt::color::burly_wood) | fmt::emphasis::italic;
    } else
        text_color = fg(fmt::color::burly_wood);

    std::clog << fmt::vformat(text_color, users_fmt, args);
}

void art::raw_end_log() {
    std::clog << "\n";
}

void art::raw_write_to_log(bool err, fmt::string_view users_fmt, fmt::format_args &&args) {
    size_t tid = gettid();
    auto now = std::chrono::system_clock::now();
    // %d %b %Y %H:%M:%OS
    std::string logged;
    fmt::text_style header_color;
    fmt::text_style text_color;
    if (err) {
        text_color = fg(fmt::color::burly_wood) | fmt::emphasis::italic;
        header_color = fg(fmt::color::white_smoke) | fmt::emphasis::italic;
        logged = fmt::format(header_color, "{}:E {:%d %b %Y %H:%M:%S} * BARCH ", tid,
                             std::chrono::floor<std::chrono::milliseconds>(now));
    } else {
        text_color = fg(fmt::color::burly_wood);
        header_color = fg(fmt::color::white_smoke);
        logged = fmt::format(header_color, "{}:M {:%d %b %Y %H:%M:%S} * BARCH ", tid,
                             std::chrono::floor<std::chrono::milliseconds>(now));
    }
    logged += fmt::vformat(text_color, users_fmt, args);
    std::clog << logged << '\n';
}

void art::log(const std::string &message, const std::exception &e) {
    std_err(message, e.what());
}

void art::log(const std::exception &e, const std::string &file, int line) {
    std_err(e.what(), file, line);
}

void art::log(const std::string &message) {
    std_log(message);
}
#endif
