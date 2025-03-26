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
static void readp(std::ifstream& in,T& data)
{
    in.read(reinterpret_cast<char*>(&data),sizeof(data));
}
#endif //IOUTIL_H
