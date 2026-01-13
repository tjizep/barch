cmake_minimum_required(VERSION 3.22)
project(barch VERSION 0.4.3 LANGUAGES CXX)
enable_testing()
set(TARGETS barch lbarch)
# Get the current git commit hash
execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
)

# Fallback if git is not available
if(NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH "unknown")
endif()

# Generate the header file
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
        ${CMAKE_CURRENT_BINARY_DIR}/generated/version.h
)

# Ensure the generated directory is in the include path
include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)

message("barch version: ${PROJECT_VERSION} (commit: ${GIT_COMMIT_HASH})")

if (NOT ADD_ZSTD_COMPRESSION)
    set(ADD_ZSTD_COMPRESSION ON)
endif ()
if (NOT ADD_FAST_FLOAT)
    set(ADD_FAST_FLOAT ON)
endif ()
if (NOT USE_ASIO)
    set(USE_ASIO ON)
endif ()
if (NOT ADD_SWIG)
    set(ADD_SWIG ON)
endif ()

if (NOT ADD_FMT)
    set(ADD_FMT ON)
endif ()

if (NOT TEST_OD)
    set(TEST_OD ON)
endif ()
if (NOT USE_LIBURING)
    set(USE_LIBURING ON)
endif ()
if (NOT USE_OPENSSL)
    set(USE_OPENSSL ON)
endif ()

option(COVERAGE "Enable code coverage reporting" OFF)

set(CMAKE_CPP_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(USE_AVX_INSTRUCTIONS ON)

#remove -Werror because fmtlib
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -march=native -Wanalyzer-malloc-leak -Wanalyzer-double-free -Wall -Wextra -Wsign-compare -Wundef -Wno-format-zero-length -Wpointer-arith -Wno-missing-braces -Wno-missing-field-initializers -Wno-missing-attributes")

if (COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
    message("Code coverage enabled")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    #set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
endif ()

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
        "-O2 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
        "-O3 -funroll-all-loops")

if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    message("Using debuggable optimizations")
endif ()

set(PYTHON3_EXEC /usr/bin/python3)

set(TEST_BUILD_DIR build)

if (CMAKE_BUILD_TYPE STREQUAL "Release")

else ()
    set(TEST_BUILD_DIR ${CMAKE_BUILD_TYPE})
endif ()

include(FetchContent)
include(ExternalProject)

if (USE_OPENSSL)
    message("Using OpenSSL")
    if (BUILD_OPENSSL)
        FetchContent_Declare(
            openssl
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/openssl/openssl/releases/download/openssl-3.6.0/openssl-3.6.0.tar.gz"
        )

        FetchContent_MakeAvailable(openssl)

        execute_process(COMMAND ./Configure
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
        execute_process(COMMAND make -j4
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
        include_directories(${openssl_SOURCE_DIR}/src/include)
        set(OPENSSL_LIB_PATH ${openssl_SOURCE_DIR}/libssl.a ${openssl_SOURCE_DIR}/libcrypto.a)
        message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
    else()
        set(OPENSSL_USE_STATIC_LIBS on)
        find_package(OpenSSL COMPONENTS Crypto SSL)
        # Avoid a rare circumstance of not finding all components but the end-user did their
        # own call for OpenSSL, which might trick us into thinking we'd otherwise have what we wanted
        if(OPENSSL_FOUND)
            include_directories(${OPENSSL_INCLUDE_DIR})
            message(STATUS "OpenSSL libs: " ${OPENSSL_LIBRARIES})
            set(OPENSSL_LIB_PATH ${OPENSSL_LIBRARIES})
            message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
        else()
            message(STATUS "Did not find OpenSSL libs use -DBUILD_OPENSSL to build it")
        endif()


    endif ()

endif ()

if (USE_LIBURING)
    message("Using liburing")
    FetchContent_Declare(
            liburing
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/axboe/liburing/archive/refs/tags/liburing-2.11.tar.gz"
    )
    FetchContent_MakeAvailable(liburing)
    execute_process(COMMAND ./configure --cc=gcc --cxx=g++
            WORKING_DIRECTORY ${liburing_SOURCE_DIR})
    execute_process(COMMAND make -j4
            WORKING_DIRECTORY ${liburing_SOURCE_DIR})
    include_directories(${liburing_SOURCE_DIR}/src/include)
    set(LIBURING_LIB_PATH ${liburing_SOURCE_DIR}/src/liburing.a)
else ()
    message("Liburing not used")
endif()

if (USE_ASIO)
    message("Using std alone ASIO")
    FetchContent_Declare(
            asio
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            #SOURCE_SUBDIR build/cmake
            URL "https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-36-0.tar.gz"
    )
    FetchContent_MakeAvailable(asio)
    include_directories(${asio_SOURCE_DIR}/asio/include)
else ()
    message("asio not used use -USE_ASIO=ON")
endif()
if (ADD_ZSTD_COMPRESSION)
    message("Using ZSTD compression")
    set(ZSTD_BUILD_STATIC ON)
    set(ZSTD_BUILD_SHARED OFF)

    FetchContent_Declare(
            zstd
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SOURCE_SUBDIR build/cmake
            URL "https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz"
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
    set(EXTRA_TEST_ARGS "")
    if (COVERAGE)
        set(EXTRA_TEST_ARGS "-DCOVERAGE=ON")
    endif ()
    ## CMake sub project executables only works if you pretend they are not subprojects
    execute_process(COMMAND rm -rf ${TEST_BUILD_DIR} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND mkdir ${TEST_BUILD_DIR} RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)
    execute_process(COMMAND cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ".."  RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${TEST_BUILD_DIR})
    execute_process(COMMAND make RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test/${TEST_BUILD_DIR})

endif ()

file(GLOB SOURCES
    "src/*.c" "src/*.cpp" "src/*.cxx" "src/rpc/*.cpp" "src/art/*.cpp" "src/io/*.cpp" "src/keys/*.cpp" "src/shard/*.cpp" "src/parsers/*.cpp"
)
if (ADD_SWIG)
    find_package (Python3 COMPONENTS Interpreter Development)
    find_package (SWIG)
    include (UseSWIG)
    set_property(SOURCE src/barch.i PROPERTY CPLUSPLUS ON)
    if (${Python3_FOUND})
        message("Python includes:${Python3_INCLUDE_DIRS}")
        message("Python libraries:${Python3_LIBRARIES} '${PYTHON_LIBRARIES}'")

        include_directories(${Python3_INCLUDE_DIRS})
        #add_library(barch SHARED ${SOURCES})
        swig_add_library(barch LANGUAGE python SOURCES src/barch.i ${SOURCES})
        target_link_libraries(barch PRIVATE ${Python3_LIBRARIES})
        set(PYTHON_INSTALL_FILES
                /home/barch/setup/${PROJECT_NAME}.py
                /home/barch/setup/_${PROJECT_NAME}.so)

        set(SETUP_PY_IN ${CMAKE_CURRENT_SOURCE_DIR}/src/python/setup.py.in)
        set(SETUP_PY_OUT ${CMAKE_BINARY_DIR}/setup.py)
        configure_file(${SETUP_PY_IN} ${SETUP_PY_OUT})

        add_custom_command(TARGET barch POST_BUILD
            COMMAND mkdir -p ${CMAKE_BINARY_DIR}/b
            BYPRODUCTS ${CMAKE_BINARY_DIR}/b
        )
        set(PYBARCH_TEST ON)
    else ()
        message("SWIG python interface generation skipped - python not found")
        message("building non SWIG library")
    endif ()
else ()
    message("SWIG local interface not used use -DADD_SWIG=ON")
    message("building non SWIG library")

endif ()

add_library(lbarch SHARED ${SOURCES})

set(TEST_SOURCE_PATH ${CMAKE_SOURCE_DIR}/test)
set(TEST_BUILD_PATH ${TEST_SOURCE_PATH}/${TEST_BUILD_DIR})

if (COVERAGE)
    add_custom_target(coverage_report
            COMMAND lcov --capture --directory . --output-file coverage.info
            COMMAND lcov --remove coverage.info '${CMAKE_BINARY_DIR}/_deps/*' '/usr/*' 'external/*' '*/CMakeFiles/*' --output-file coverage.info
            COMMAND genhtml coverage.info --output-directory coverage_html
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Generating HTML coverage report"
    )
    set(LCOV_REPORT "${CMAKE_BINARY_DIR}/coverage.info")
    set(BADGE_OUTPUT "${CMAKE_SOURCE_DIR}/coverage.svg")

    add_custom_target(coverage_badge
            DEPENDS coverage_report
            COMMAND bash "${CMAKE_SOURCE_DIR}/generate_badge.sh" "${LCOV_REPORT}" "${BADGE_OUTPUT}"
            COMMENT "Generating coverage badge from ${LCOV_REPORT} into ${BADGE_OUTPUT}"
            VERBATIM
    )
endif ()

if (ADD_ZSTD_COMPRESSION)
    target_link_libraries(barch PRIVATE libzstd_static)
    target_link_libraries(lbarch PRIVATE libzstd_static)
endif ()

if (USE_LIBURING)
    target_link_libraries(barch PRIVATE ${LIBURING_LIB_PATH})
    target_link_libraries(lbarch PRIVATE ${LIBURING_LIB_PATH})
endif ()

if (USE_OPENSSL)
    message("OpenSSL static Linking against ${OPENSSL_LIB_PATH}")
    target_link_libraries(barch PRIVATE ${OPENSSL_LIB_PATH})
    target_link_libraries(lbarch PRIVATE ${OPENSSL_LIB_PATH})
endif ()

if (ADD_FMT)
    target_link_libraries(barch PRIVATE fmt::fmt)
    target_link_libraries(lbarch PRIVATE fmt::fmt)
endif ()

if (NOT TEST_OD)
    message("Testing not enabled tests not generated :Use -DTEST_OD=ON")
else()


    add_test(NAME TestIntegers
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testintegers.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestDoubles
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testdouble.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME BicycleTest
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testbicycle.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestAbc
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testabc.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME Test123
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/test123.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestRandom
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testrnd.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestBounds
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testbounds.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestKeys
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testkeys.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME LargeKeys
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testgloblarge.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME Test1111111
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/test111111.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestMSG
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testmulti.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestIntArithmetic
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testintarith.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestComposites
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testcomposites.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestZRank
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/testzrank.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestBenchy
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/zbenchy.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestHashBenchy
            COMMAND ${TEST_BUILD_PATH}/TestStarter ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}/zwbenchy.lua ${TEST_BUILD_PATH}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    add_test(NAME TestClean
            COMMAND sh -c "rm -f *.dat"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    if (PYBARCH_TEST)
        message("Python test: WORKING_DIRECTORY ${CMAKE_BINARY_DIR}")

        add_test(NAME TestInstallRedisPy
                COMMAND "pip3" install redis
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

        add_test(NAME TestBarchInstallPy
                COMMAND "pip3" install .
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchPy
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/testbarch.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchReplicationPy2
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/repltest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchLru
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/lrutest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchExpireMany
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/expiretest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchList
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/listtest.py ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}
                WORKING_DIRECTORY ${TEST_BUILD_PATH})

        add_test(NAME TestBarchRPC
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/remotetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBarchSimpleClusterRPC
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/routetest.py ${CMAKE_BINARY_DIR} ${TEST_SOURCE_PATH}
                WORKING_DIRECTORY ${TEST_BUILD_PATH})

        add_test(NAME TestRespClientLocal
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/redispytest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestMerge
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/mergetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestSpaceThread
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/spacethreadtest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestBpopThread
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/bstartest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestLargeDataSave
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/largetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestLargeDataLoad
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/largetest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

        add_test(NAME TestCompression
                COMMAND ${PYTHON3_EXEC} ${TEST_SOURCE_PATH}/compresstest.py
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

    endif ()
endif ()

