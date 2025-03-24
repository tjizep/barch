//
// Created by linuxlite on 3/24/25.
//

#ifndef LOGGER_H
#define LOGGER_H
#include <string>
#include <exception>

namespace art
{
    class logger
    {
    public:
        static void log(const std::string& message, const std::exception& e);
        static void log(const std::exception& e, const std::string& file, int line);
        static void log(const std::string& message);
        template<typename... Args>
        static void log(Args&&... args);
    };
}


#endif //LOGGER_H
