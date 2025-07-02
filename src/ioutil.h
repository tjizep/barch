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
template <typename Type, typename = void>
struct has_expires_from_now : std::false_type{

};

template <typename Type>
struct has_expires_from_now<Type,
    typename std::enable_if<std::is_member_function_pointer<decltype(&Type::expires_from_now)>::value>::type> : std::true_type
{

};
extern thread_local uint64_t stream_write_ctr;
extern thread_local uint64_t stream_read_ctr;
template<typename OStream, typename T>
static void writep(OStream &of, const T* data, size_t size) {
    if  constexpr (has_expires_from_now<OStream>::value) {
        of.expires_from_now(art::get_rpc_write_to_s());
    }
    if (log_streams==1) art::std_log("writing",size,"bytes","at",(uint64_t)stream_write_ctr);
    of.write(reinterpret_cast<const char *>(data), size);
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    stream_write_ctr += size;
}

template<typename OStream, typename T>
static void writep(OStream &of, const T &data) {
    if  constexpr (has_expires_from_now<OStream>::value) {
        of.expires_from_now(art::get_rpc_write_to_s());
    }

    of.write(reinterpret_cast<const char *>(&data), sizeof(data));
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
}

template<typename IStream, typename T>
static void readp(IStream &in, T &data) {
    if  constexpr (has_expires_from_now<IStream>::value) {
        in.expires_from_now(art::get_rpc_read_to_s());
    }

    in.read(reinterpret_cast<char *>(&data), sizeof(data));
    if (log_streams==1) art::std_log("reading",sizeof(data),(uint64_t)data,"at",(uint64_t)stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("read failed");
    }
    stream_read_ctr+=sizeof(data);
}

template<typename IStream, typename T>
static void readp(IStream &in, T *data, size_t size) {
    if  constexpr (has_expires_from_now<IStream>::value) {
        in.expires_from_now(art::get_rpc_read_to_s());
    }
    in.read(reinterpret_cast<char *>(data), size);
    if (log_streams==1) art::std_log("reading",size,"bytes","at",(uint64_t)stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("read failed");
    }
    stream_read_ctr+=size;
}
#endif //IOUTIL_H
