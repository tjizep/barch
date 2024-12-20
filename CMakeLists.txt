cmake_minimum_required(VERSION 3.28)
project(cdict)

set(CMAKE_CPP_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Weffc++")
include_directories("module/fast_float/include")

file(GLOB SOURCES
    "src/*.c" "src/*.cpp"
)

add_library(cdict SHARED ${SOURCES})
