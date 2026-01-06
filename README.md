# Introducing *BARCH*
[![Ubuntu 24.04 CI (GCC 13)](https://github.com/tjizep/barch/actions/workflows/ubuntu24.yml/badge.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu24.yml)
[![Ubuntu 22.04 CI (GCC 11)](https://github.com/tjizep/barch/actions/workflows/ubuntu22.yml/badge.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu22.yml)
[![coverage](https://tjizep.github.io/barch/coverage.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu24-sanitize.yml)

`BARCH` is a low-memory, dynamically configurable, constant access time ordered cache similar to [valkey](https://valkey.io/) and redis

Additionally, it has an embedded server+client for python which can reduce read latency to micro-seconds.
It implements the Z* (OrderedSet), H* (HashSet) and key value API's available in redis.
A demo ubuntu 22.04 docker image is available at teejip/barch:v0.4.2.6b


### Advantages of the embedded L1 server

- far lower read latency (sub micro-second instead of milli-seconds)
- automatic scaling (to a point)
- concurrency through sharding
- optionally serverless (L1 only or L1 + L2)
- future work may add object stores such as S3 or dynamo as L2 or even L3 layers

The embedded server can also run in a multi-writer replication configuration in conjunction with a standalone server.
An embedded server can also be quickly synchronized with a standalone server using the block load api.
   - See [python example](https://tjizep.github.io/barch/PYTHONSERVEREXAMPLE)

### [Some Benchmarks](https://tjizep.github.io/barch/BENCHMARKS)

# Features
1. Ordered: minimum, lower-bound and maximum operations are constant time
2. Low memory footprint: Half that of standard hash map while providing similar single threaded latency 
   - Also see [Compression](https://tjizep.github.io/barch/COMPRESSION)
3. Dynamic config: All options can be configured at runtime without restarts or reload
4. Scalability: All reads are concurrent and scales linearly with core count

# Use Cases
1. [Fast Ranged Queries, Prefix Queries and Aggregates](https://tjizep.github.io/barch/USECASE)
2. Prefix-cache for fast loading of hints  
3. Z-Order/Morton code range queries for quick spatial bounds checking
4. Fast and accurate counting of small ranges (age, population etc) See [ZfastRank](https://tjizep.github.io/barch/ZFASTRANK)
5. Many small key value pairs in small footprint with minimal overhead
6. Constant time access priority queue

# Installation

1. Checkout and build on (linux only) with gcc 10 or above
   ```
   cmake -B build -DTEST_OD=ON` -  add `-DCMAKE_BUILD_TYPE=Debug  #for debug modes
   cmake --build build --target barch --parallel #builds `_barch.so  #with python dependencies
   cmake --build build --target lbarch --parallel #builds `liblbarch.so #without python dependencies
   ```
   Test: `ctest`
   start `valkey-server valkey.conf --loadmodule {[src code]/build/_barch.so}`
2. [Via Docker image](https://tjizep.github.io/barch/DOCKER)

# Docs
1. [Documentation for API's](https://tjizep.github.io/barch/APIS)

# Configuration
See [Configuration](https://tjizep.github.io/barch/DOCKER) docs

# Hardware Compatibility
1. amd64+sse
2. arm+neon

# Toolchain Compatibility
1. Debian + gcc 11+
2. any compiler that supports C++ 20

# Server Environment
1. Valkey v. 8.0+
2. any python 3.10 program


