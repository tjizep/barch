//
// Created by teejip on 8/12/25.
//

#ifndef BARCH_TIME_CONVERTSION_H
#define BARCH_TIME_CONVERTSION_H
#include <chrono>
template<typename T>
static uint64_t millis(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(a - b);
    uint64_t count = duration.count();
    return count;
}
template<typename T>
static uint64_t micros(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(a - b);
    uint64_t count = duration.count();
    return count;
}
template<typename T>
static uint64_t secs(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(a - b);
    uint64_t count = duration.count();
    return count;
}
template<typename T>
static uint64_t minutes(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::minutes>(a - b);
    uint64_t count = duration.count();
    return count;
}
template<typename T>
static uint64_t hours(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::hours>(a - b);
    uint64_t count = duration.count();
    return count;
}
template<typename T>
static uint64_t days(std::chrono::time_point<T> a, std::chrono::time_point<T> b) {
    auto duration = std::chrono::duration_cast<std::chrono::hours>(a - b);
    uint64_t count = duration.count()/24;
    return count;
}
template<typename T>
static uint64_t millis(std::chrono::time_point<T> a) {
    return millis(std::chrono::high_resolution_clock::now(), a);
}
#endif //BARCH_TIME_CONVERTSION_H