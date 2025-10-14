//
// Created by linuxlite on 3/26/25.
//

#ifndef IOUTIL_H
#define IOUTIL_H
#include <iostream>
#include <fstream>
#include "constants.h"
#include "logger.h"
#include <arpa/inet.h>
#include "configuration.h"
#include <experimental/type_traits>

template<typename T>
using expires_from_now_t = decltype( std::declval<T&>().expires_from_now(std::chrono::seconds(10)) );

template<typename T>
constexpr bool has_expires_from_now = std::experimental::is_detected_v<expires_from_now_t, T>;

template<typename T>
using nostats_t = decltype( std::declval<T&>().nostats() );

template<typename T>
constexpr bool has_nostats = std::experimental::is_detected_v<nostats_t, T>;


template <typename T>
static void read_expires(T& stream)
{
    if constexpr (has_expires_from_now<T>) {
        stream.expires_from_now(barch::get_rpc_read_to_s());
    }
}

template <typename T>
static void write_expires(T& stream)
{
    if constexpr (has_expires_from_now<T>) {
        stream.expires_from_now(barch::get_rpc_write_to_s());
    }

}

extern thread_local uint64_t stream_write_ctr;
extern thread_local uint64_t stream_read_ctr;

template <typename TStream>
static void incr_write_stat(const TStream&, size_t bytes) {
    if constexpr (has_nostats<TStream>) {
    }else {
        stream_write_ctr += bytes;
    }
}


template <typename TStream>
static void incr_read_stat(const TStream& , size_t bytes) {
    if constexpr (has_nostats<TStream>) {

    }else {
        stream_read_ctr += bytes;
    }

}


template<typename OStream, typename T>
static void writep(OStream &of, const T* data, size_t size) {
    write_expires(of);
    if (log_streams==1) barch::std_log("writing",size,"bytes","at",(uint64_t)stream_write_ctr);
    of.write(reinterpret_cast<const char *>(data), size);
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    incr_write_stat(of,size);
}

template<typename OStream, typename T>
static void writep(OStream &of, const T &data) {
    write_expires(of);
    of.write(reinterpret_cast<const char *>(&data), sizeof(data));
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    incr_write_stat(of,sizeof(data));
}
template<typename OStream>
static void writep(OStream &of, const char* data) {
    write_expires(of);
    size_t size = strlen(data);
    of.write(data, size);
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    incr_write_stat(of,size);
}

template<typename IStream, typename T>
static void readp(IStream &in, T &data) {
    read_expires(in);

    in.read(reinterpret_cast<char *>(&data), sizeof(data));
    if (log_streams==1) barch::std_log("reading",sizeof(data),(uint64_t)data,"at",(uint64_t)stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("read failed");
    }
    incr_read_stat(in,sizeof(data));
}

template<typename IStream, typename T>
static void readp(IStream &in, T *data, size_t size) {
    read_expires(in);
    in.read(reinterpret_cast<char *>(data), size);
    if (log_streams==1) barch::std_log("reading",size,"bytes","at",(uint64_t)stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("read failed");
    }
    incr_read_stat(in,size);
}
#endif //IOUTIL_H
