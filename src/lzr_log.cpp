//
// Created by teejip on 8/2/26.
//
// The formatting the old logger did, done once here rather than at every call site.
// This is the only translation unit that needs fmt's chrono and color headers.
//

#include "lzr_log.h"

#include <chrono>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string>
#include <sys/unistd.h>

#include "logger.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Weffc++"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/format.h>
#pragma GCC diagnostic pop

namespace {
    enum class level { info, warning, error };

    /**
     * Render one piece the way the old logger's "{}" would have. fmt does the numbers,
     * so a whole double is "3" and a char is a character, exactly as before.
     */
    void append(std::string& out, const barch::log_value& v) {
        switch (v.k) {
            case barch::log_value::text:
                out.append(v.str.data(), v.str.size());
                break;
            case barch::log_value::character:
                out += v.as.c;
                break;
            case barch::log_value::boolean:
                fmt::format_to(std::back_inserter(out), "{}", v.as.b);
                break;
            case barch::log_value::integer:
                fmt::format_to(std::back_inserter(out), "{}", v.as.i);
                break;
            case barch::log_value::unsigned_integer:
                fmt::format_to(std::back_inserter(out), "{}", v.as.u);
                break;
            case barch::log_value::real:
                fmt::format_to(std::back_inserter(out), "{}", v.as.d);
                break;
        }
    }

    void write_line(level lv, const barch::log_line& parts) {
        std::string body;
        for (const barch::log_value& p : parts) {
            append(body, p);
            body += ' ';
        }

        fmt::text_style header_color;
        fmt::text_style text_color;
        char tag = 'M';
        switch (lv) {
            case level::error:
                tag = 'E';
                text_color = fg(fmt::color::burly_wood) | fmt::emphasis::italic;
                header_color = fg(fmt::color::white_smoke) | fmt::emphasis::italic;
                break;
            case level::warning:
                tag = 'W';
                text_color = fmt::fg(fmt::color::golden_rod);
                header_color = fg(fmt::color::white_smoke);
                break;
            case level::info:
                tag = 'M';
                text_color = fg(fmt::color::burly_wood);
                header_color = fg(fmt::color::white_smoke);
                break;
        }

        size_t tid = gettid();
        auto now = std::chrono::system_clock::now();
        std::string logged = fmt::format(header_color, "{}:{} {:%d %b %Y %H:%M:%S} * BARCH ",
                                         tid, tag,
                                         std::chrono::floor<std::chrono::milliseconds>(now));
        // body is an argument rather than the format string, so a brace in a logged
        // value is written out instead of being read as a placeholder
        logged += fmt::format(text_color, "{}", body);

        // the same lock the old logger takes, so lines from the two do not interleave
        // while call sites are still being moved across
        std::unique_lock lock(barch::log_mutex());
        std::clog << logged << '\n';
    }
}

void barch::log(log_line parts) {
    write_line(level::info, parts);
}

void barch::warn(log_line parts) {
    write_line(level::warning, parts);
}

void barch::err(log_line parts) {
    write_line(level::error, parts);
}
