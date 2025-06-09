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

extern thread_local uint64_t stream_write_ctr;
extern thread_local uint64_t stream_read_ctr;

template<typename T>
static void writep(std::ostream &of, const T &data) {
    if (log_streams==1) art::std_log("writing",sizeof(data),(uint64_t)data,"at",stream_write_ctr);
    of.write(reinterpret_cast<const char *>(&data), sizeof(data));
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    stream_write_ctr += sizeof(data);
}
template<typename T>
static void writep(std::ostream &of, const T* data, size_t size) {
    if (log_streams==1) art::std_log("writing",size,"bytes","at",stream_write_ctr);
    of.write(reinterpret_cast<const char *>(data), size);
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    stream_write_ctr += size;
}

template<typename OStream, typename T>
static void writep(OStream &of, const T &data) {

    of.write(reinterpret_cast<const char *>(&data), sizeof(data));
    if (of.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
}

template<typename IStream, typename T>
static void readp(IStream &in, T &data) {
    in.read(reinterpret_cast<char *>(&data), sizeof(data));
    if (log_streams==1) art::std_log("reading",sizeof(data),(uint64_t)data,"at",stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    stream_read_ctr+=sizeof(data);
}

template<typename T>
static void readp(std::istream &in, T *data, size_t size) {
    in.read(reinterpret_cast<char *>(data), size);
    if (log_streams==1) art::std_log("reading",size,"bytes","at",stream_read_ctr);
    if (in.fail()) {
        throw_exception<std::runtime_error>("write failed");
    }
    stream_read_ctr+=size;
}
#endif //IOUTIL_H
