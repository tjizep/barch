cmake_minimum_required(VERSION 3.22)
project(barch VERSION 0.0.1 LANGUAGES CXX)
enable_testing()
if (NOT ADD_ZSTD_COMPRESSION)
    set(ADD_ZSTD_COMPRESSION OFF)
endif ()
if (NOT ADD_FAST_FLOAT)
    set(ADD_FAST_FLOAT ON)
endif ()

if (NOT ADD_FMT)
    set(ADD_FMT ON)
endif ()

if (NOT TEST_OD)
    set(TEST_OD ON)
endif ()

set(CMAKE_CPP_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(USE_AVX_INSTRUCTIONS ON)
#remove -Werror because fmtlib
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Weffc++")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

include(FetchContent)

if (ADD_ZSTD_COMPRESSION)
    message("Using ZSTD compression")
    set(ZSTD_BUILD_STATIC ON)
    set(ZSTD_BUILD_SHARED OFF)

    FetchContent_Declare(
            zstd
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SOURCE_SUBDIR build/cmake
            URL "https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz"
    )
else ()
    message("ZSTD compression not used use -DADD_ZSTD_COMPRESSION=ON")
endif()
if (ADD_FAST_FLOAT)
    FetchContent_Declare(
            fast_float
            GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
            GIT_TAG main
    )
    FetchContent_MakeAvailable(fast_float)
endif ()
if (ADD_FMT)

    FetchContent_Declare(
            fmt
            GIT_REPOSITORY https://github.com/fmtlib/fmt
            GIT_TAG        123913715afeb8a437e6388b4473fcc4753e1c9a) # 11.4
    SET(BUILD_SHARED_LIBS FALSE)
    SET(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
    FetchContent_MakeAvailable(fmt)
endif ()

if (ADD_ZSTD_COMPRESSION)
    FetchContent_MakeAvailable(zstd)
endif ()

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
#-Weffc++ is removed else fmtlib does not compile - because it's a little ...s


include_directories(src)
include_directories(external/include)
include_directories(${fast_float_SOURCE_DIR}/include)
include_directories(${zstd_SOURCE_DIR}/lib)

if (NOT TEST_OD)
    message("Testing not enabled Use -DTEST_OD=ON")
else()
    ## CMake sub project executables only works if you pretend they are not subprojects
    execute_process(COMMAND rm -rf ${CMAKE_BUILD_TYPE} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND mkdir ${CMAKE_BUILD_TYPE} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ".."  RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})
    execute_process(COMMAND make RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})
endif ()

file(GLOB SOURCES
    "src/*.c" "src/*.cpp"
)

add_library(barch SHARED ${SOURCES})

if (ADD_ZSTD_COMPRESSION)
    target_link_libraries(barch PRIVATE libzstd_static)
endif ()

if (ADD_FMT)
    target_link_libraries(barch PRIVATE fmt::fmt)
endif ()

if (NOT TEST_OD)
    message("Testing not enabled tests not generated :Use -DTEST_OD=ON")
else()

    add_test(NAME TestIntegers
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testintegers.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestDoubles
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testdouble.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME BicycleTest
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testbicycle.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestAbc
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testabc.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME Test123
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} test123.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestRandom
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testrnd.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestBounds
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testbounds.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestKeys
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testkeys.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME LargeKeys
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testgloblarge.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME Test1111111
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} test111111.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestMSG
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testmulti.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestIntArithmetic
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testintarith.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestComposites
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testcomposites.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

    add_test(NAME TestZRank
            COMMAND ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE}/TestStarter ${CMAKE_BINARY_DIR} testzrank.lua
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${CMAKE_BUILD_TYPE})

endif ()
