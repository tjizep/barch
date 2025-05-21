//
// Created by teejip on 5/20/25.
//

#ifndef SWIG_API_H
#define SWIG_API_H

#include <string>

class KeyMap {
public:
    KeyMap();
    void set(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    void incr(const std::string& key, double by);
    void decr(const std::string& key, double by);
    void erase(const std::string &key);
    std::string min() const ;
    std::string max() const ;
    size_t size() const;
    void save();
    void clear();
};
#endif //SWIG_API_H
