Building for coverage
```
mkdir build && cd build
cmake .. -DCOVERAGE=ON -DTEST_OD=ON -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)
```