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

#include <mutex>
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/color.h>
#include <thread>
static std::mutex& get_lock() {
    static std::mutex m;
    return m;
};
void barch::raw_start_log(bool err) {
    std::unique_lock lock(get_lock());
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

void barch::raw_continue_log(bool err, fmt::string_view users_fmt, fmt::format_args &&args) {
    std::unique_lock lock(get_lock());

    fmt::text_style text_color;
    if (err) {
        text_color = fg(fmt::color::burly_wood) | fmt::emphasis::italic;
    } else
        text_color = fg(fmt::color::burly_wood);

    std::clog << fmt::vformat(text_color, users_fmt, args);
}

void barch::raw_end_log() {
    std::unique_lock lock(get_lock());
    std::clog << "\n";
}

void barch::raw_write_to_log(bool err, fmt::string_view users_fmt, fmt::format_args &&args) {
    std::unique_lock lock(get_lock());
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

void barch::log(const std::string &message, const std::exception &e) {
    std_err(message, e.what());
}

void barch::log(const std::exception &e, const std::string &file, int line) {
    std_err(e.what(), file, line);
}

void barch::log(const std::string &message) {
    std_log(message);
}
