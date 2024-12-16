cmake_minimum_required(VERSION 3.29)
project(artval C)

set(CMAKE_C_STANDARD 11)

add_library(artval SHARED src/art.c src/artval.c)
