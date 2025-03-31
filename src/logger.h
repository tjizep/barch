//
// Created by linuxlite on 3/24/25.
//

#ifndef LOGGER_H
#define LOGGER_H
#include <string>
#include <exception>
#include <array>
#include <chrono>
namespace art
{
    extern void raw_write_to_log(std::string_view users_fmt, std::format_args&& args);
    template<typename... Args>
    static constexpr void std_log(Args&&... args)
    {

        // Generate formatting string "{} "...
        std::array<char, sizeof...(Args) * 3 + 1> braces{};
        constexpr const char c[4] = "{} ";
        for (size_t i{0}; i != braces.size() - 1; ++i)
            braces[i] = c[i % 3];
        braces.back() = '\0';

        raw_write_to_log(std::string_view{braces.data()}, std::make_format_args(args...));
    }
    extern void log(const std::string& message, const std::exception& e);
    extern void log(const std::exception& e, const std::string& file, int line);
    extern void log(const std::string& message);

}


#endif //LOGGER_H
