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

# Installation and Running

Note: BARCH builds to multiple shared libraries for different environments.

Checkout and build on (currently linux only) with gcc 10 or above...

1. Install python development tools
      ```
      sudo apt update && sudo apt upgrade -y
      sudo apt install -y build-essential make curl ca-certificates libssl-dev libffi-dev
      sudo apt install -y python3-pip python3-venv python3-full
      sudo apt install cmake
      ```
2. REQUIRED: install swig (to generate luajit, python and java bindings)
      ```
      sudo apt install swig
      ```
3. OPTIONAL: install jdk and maven - for barchjni, barchj and running java examples.
   Examples for java are located in examples/java/barchj.
      ```
      sudo apt install default-jdk
      sudo apt install maven
      ```
4. OPTIONAL: install openresty for barchlua (luajit) - 
   Follow instructions for openresty at https://openresty.org/en/linux-packages.html#ubuntu 
    - after building barch for lua should be located in the /var/lib/lua/5.1/barch.so
    - To install:
      ```
      sudo cmake --install . --component barchlua
      ```
5. NOTES:
   - The build will download valkey sources and build lbarch which is the barch valkey runtime for the `B.*` api's.
   - Barch can run without valkey as a python library in which case you can connect to the barch resp port of your choice using redis-cli or valkey-cli. See the examples/flask/server.py for more information on how to do this.

6. FINALLY: 
   - Build with cmake (barchj and lbarch will be built if the environments from above is detected)
         
- 
   ```
   cmake -B build -DTEST_OD=ON
   cmake --build build --target barch --parallel 
   cmake --build build --target lbarch --parallel
   ```
     
   Test: `ctest`
   start `valkey-server valkey.conf --loadmodule {src code location}/build/_barch.so`

  - install barch for lua
    ```
     sudo cmake --install . --component barchlua 
    ```
Or alternatively [Via Docker image](https://tjizep.github.io/barch/DOCKER)

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


