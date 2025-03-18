#!/bin/bash

TOOLCHAIN="$1"

mkdir build
cd build

if [ "$TOOLCHAIN" != "" ] ; then
    cmake -DTEST_OD=ON  .. -DCMAKE_TOOLCHAIN_FILE=/toolchains/"$TOOLCHAIN".cmake
else
    cmake -DTEST_OD=ON  ..
fi
make -j 2
if [ "$TOOLCHAIN" != "" ] ; then
  qemu-"$TOOLCHAIN" tests/basictest
else
  ctest --output-on-failure
fi
