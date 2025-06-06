//
// Created by teejip on 5/20/25.
//

#ifndef SWIG_API_H
#define SWIG_API_H
#include "swig_caller.h"
#include <string>
#include <vector>
void setConfiguration(const std::string& name, const std::string& value);
void load(const std::string& host, const std::string& port);
void ping(const std::string &host, const std::string& port);
void start(const std::string &host, const std::string& port);
void stop();
unsigned long long size();
void save();
void load();
void clear();
class KeyMap {
public:
    KeyMap();
    void set(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    void incr(const std::string& key, double by);
    void decr(const std::string& key, double by);
    void erase(const std::string &key);
    std::vector<std::string>& glob(const std::string& glob, int max_ = 0) const;
    size_t globCount(const std::string& glob) const;
    std::string lowerBound(const std::string& key) const ;
    std::string min() const ;
    std::string max() const ;
private:
    mutable std::vector<std::string_view> params {};
    mutable std::vector<std::string> result{};
    mutable swig_caller sc{};
};
#endif //SWIG_API_H
