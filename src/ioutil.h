//
// Created by linuxlite on 3/26/25.
//

#ifndef IOUTIL_H
#define IOUTIL_H
#include <iostream>
#include <fstream>
template <typename T>
static void writep(std::ofstream& of,const T& data)
{
    of.write(reinterpret_cast<const char*>(&data),sizeof(data));
}
template <typename T>
static void writep(std::ostream& of,const T& data)
{
    of.write(reinterpret_cast<const char*>(&data),sizeof(data));
}
template <typename OStream, typename T>
static void writep(OStream& of,const T& data)
{
    of.write(reinterpret_cast<const char*>(&data),sizeof(data));
}
template <typename T>
static void readp(std::ifstream& in,T& data)
{
    in.read(reinterpret_cast<char*>(&data),sizeof(data));
}

template <typename IStream, typename T>
static void readp(IStream& in,T& data)
{
    in.read(reinterpret_cast<char*>(&data),sizeof(data));
}
#endif //IOUTIL_H
