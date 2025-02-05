cmake_minimum_required(VERSION 3.9)
project(cdict VERSION 0.0.1 LANGUAGES CXX)
enable_testing()

set(CMAKE_CPP_STANDARD 17)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
include(FetchContent)
FetchContent_Declare(
        fast_float
        GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
        GIT_TAG main
)
FetchContent_MakeAvailable(fast_float)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Weffc++")
include_directories(${fast_float_SOURCE_DIR}/include)

## CMake sub project executables only works if you pretend they are not subprojects
execute_process(COMMAND rm -rf ${CMAKE_BUILD_TYPE} RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
execute_process(COMMAND mkdir ${CMAKE_BUILD_TYPE} RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
execute_process(COMMAND cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ".."  RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})
execute_process(COMMAND make RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

file(GLOB SOURCES
    "src/*.c" "src/*.cpp"
)

add_library(cdict SHARED ${SOURCES})
add_test(NAME TestStarter
        COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

#add_test(
#        NAME start_valkey_server
#        COMMAND valkey-server --loadmodule ${CMAKE_BINARY_DIR}/libcdict.so
#        WORKING_DIRECTORY ${valkey_SOURCE_DIR}/src/)

#add_test(
#        NAME run_lua
#        COMMAND valkey-cli --eval "${CMAKE_SOURCE_DIR}/test/test.lua"
#        WORKING_DIRECTORY)