//
// Created by teejip on 10/8/25.
//

#ifndef BARCH_SOURCE_H
#define BARCH_SOURCE_H
#include <string>
class source {
public:
    std::string host {};
    std::string port {};
    size_t shard {};
    virtual ~source() = default;
    virtual bool is_open() const = 0;
    virtual bool fail() const = 0;
    virtual void flush() = 0;
    virtual bool connect(const std::string& host, const std::string& port) = 0;
    virtual bool read(char* data, size_t size) = 0;
    virtual bool write(const char* data, size_t size) = 0;
    virtual bool close() = 0;
};

#endif //BARCH_SOURCE_H