//
// Created by linuxlite on 3/24/25.
//

#ifndef LOGGER_H
#define LOGGER_H
#include <sys/types.h>
#include <string>
#include <exception>
#include <array>
#include <format>
#include <iostream>
#include <string_view>
#include <chrono>
#include <thread>
namespace art
{
    extern bool is_initialized;
    static void raw_write_to_log(std::string_view users_fmt, std::format_args&& args)
    {

        size_t tid = getpid();
        auto now = std::chrono::system_clock::now();
        // %d %b %Y %H:%M:%OS
        std::string logged = std::format("{}:M {:%d %b %Y %H:%M:%OS} * ",tid, now);
        logged += std::vformat(users_fmt, args);
        std::clog <<  logged << '\n';
    }

    template<typename... Args>
    constexpr void std_log(Args&&... args)
    {
        // Generate formatting string "{} "...
        std::array<char, sizeof...(Args) * 3 + 1> braces{};
        constexpr const char c[4] = "{} ";
        for (size_t i{0}; i != braces.size() - 1; ++i)
            braces[i] = c[i % 3];
        braces.back() = '\0';

        raw_write_to_log(std::string_view{braces.data()}, std::make_format_args(args...));
    }

    static void log(const std::string& message, const std::exception& e)
    {
        std_log(message, e.what());
    }

    static void log(const std::exception& e, const std::string& file, int line)
    {
        std_log(e.what(), file, line);
    }

    static void log(const std::string& message)
    {
        std_log(message);
    }
    static bool logger_init()
    {
        if(is_initialized)
        {
            log("");
            log(std::runtime_error("test"), __FILE__, __LINE__);
            log("error",std::runtime_error("test"));

        }
        return true;
    }
    static bool initialized = logger_init();

}


#endif //LOGGER_H
