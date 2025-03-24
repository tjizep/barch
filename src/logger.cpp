//
// Created by linuxlite on 3/24/25.
//

#include "logger.h"
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <sstream>
#include <chrono>
#include <format>
template<typename... Args>
std::string fmtcat(std::string_view rt_fmt_str, Args&&... args)
{
    return std::vformat(rt_fmt_str, std::make_format_args(args...));
}

void art::logger::log(const std::string& message, const std::exception& e)
{
    auto now = std::chrono::system_clock::now();
    std::string time = std::format("{0:%F_%T}", now);
    std::cerr << fmtcat(time, message, e.what()) << std::endl;
}

void art::logger::log(const std::exception& e, const std::string& file, int line)
{
    auto now = std::chrono::system_clock::now();
    std::string time = std::format("{0:%F_%T}", now);
    std::cerr << fmtcat(time, file, line, e.what()) << std::endl;
}

void art::logger::log(const std::string& message)
{
    auto now = std::chrono::system_clock::now();
    std::string time = std::format("{0:%F_%T}", now);
    std::cout << fmtcat(time, message) << std::endl;
}

template<typename... Args>
void log(Args&&... args)
{
    std::string message = fmtcat(args...);
    auto now = std::chrono::system_clock::now();
    std::string time = std::format("{0:%F_%T}", now);
    std::cout << fmtcat(time, message) << std::endl;
}
