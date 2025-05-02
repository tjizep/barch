# Introducing *BARCH*

`BARCH` is a low memory, dynamically configurable, constant access time ordered key value store for [valkey](https://valkey.io/)

[![Ubuntu 24.04 CI (GCC 13)](https://github.com/tjizep/barch/actions/workflows/ubuntu24.yml/badge.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu22.yml)

## Whats New (2 May 2025)
### Ordered Set API's

All are O(1) (+N where applicable) time except ZRANK which is O(m) ZFASTRANK is a O(1) replacement.
Not all flags on ZADD are supported yet except XX and NX.

B.ZPOPMIN,B.ZPOPMAX, B.ZADD, B.ZREM, B.ZCOUNT, B.ZCARD, B.ZDIFF, B.ZDIFFSTORE, B.ZINTERSTORE, B.ZINCRBY, B.ZREMRANGEBYLEX, B.ZINTERCARD, B.ZINTER, B.ZRANGE, B.ZREVRANGE, B.ZRANGEBYSCORE, B.ZREVRANGEBYSCORE, B.ZREVRANGEBYLEX, B.ZRANGEBYLEX, B.ZRANK, B.ZFASTRANK

### Hash Set Functions added

All are O(1) time

B.HSET, B.HGETDEL, B.HGETEX, B.HMSET, B.HEXPIRE, B.HDEL, B.HINCRBY, B.HINCRBYFLOAT, B.HGET, B.HTTL, B.HLEN, B.HEXPIRETIME, B.HMGET, B.HGETALL, B.HKEYS, B.HEXISTS
Other functions

BSIZE is replaced by B.SIZE
B.STATS is used for statistics about the internal data structures

### Notes on ZFASTRANK

ZFastRank employs a newly developed constant time algorithm for determining rank in a sorted or ordered set.
The algorithm makes use of trace paths and cumulative nodes to calculate the node count between arbitrary boundaries in constant time on ART's (Adaptive Radix Trees).
This algorithm is currently unknown to academia or other entities.

### Benchmarks
Barch ZADD benchmark
```
./valkey-benchmark -t b.zadd -r 10000000 -n 40000000 -P 16 -q
B.ZADD: 280117.94 requests per second, p50=2.935 msec
```
Valkey ZADD benchmark
```
./valkey-benchmark -t zadd -r 10000000 -n 40000000 -P 16 -q
ZADD: 176270.47 requests per second, p50=4.647 msec
```
Barch has on average 80% less latency the difference increases with key count and requests




# Features
1. Ordered: minimum, lower-bound and maximum operations are constant time
2. Low memory footprint: Half that of standard hash map while providing similar single threaded latency 
3. Compression: Memory use can be lowered even further using zstd compression at the expense of latency
4. Dynamic config: All options can be configured at runtime without restarts or reload
5. Scalability: All reads are concurrent and scales linearly with core count

# Use Cases
1. [Fast Ranged Queries, Prefix Queries and Aggregates](https://github.com/tjizep/barch/blob/main/USECASE.md)
2. Prefix-cache for fast loading of hints  
3. Z-Order/Morton code range queries for quick spatial bounds checking
4. Fast and accurate counting of small ranges (age, population etc)
5. Many small key value pairs in small footprint with minimal overhead
6. Constant time access priority queue

# Commands and API
1. `B.ADD K V` add a key and value [more](https://github.com/tjizep/barch/blob/main/ADD.md)
2. `B.SET K V` set a key overriding existing value [more](https://github.com/tjizep/barch/blob/main/SET.md)
3. `B.GET K` retrieve a key, returns nil if no key exists [more](https://github.com/tjizep/barch/blob/main/GET.md)
4. `B.REM K` removes a key, returns value if success [more](https://github.com/tjizep/barch/blob/main/REM.md)
5. `B.MAX` returns largest key  (or nil) - numbers are less than text
6. `B.MIN` returns smallest key (or nil) if there are no keys
7. `B.RANGE K1 K2 count` returns an array of maximum `count` containing the keys [see example](https://github.com/tjizep/barch/blob/main/USECASE.md)
8. `B.COUNT K1 K2` returns count of keys within a range [see example](https://github.com/tjizep/barch/blob/main/USECASE.md) 
9. `B.LB K1` lower bound: first key not less than K1 
10. `B.SIZE` returns keys held by `BARCH` this may include expired or evicted volatile keys
11. `B.HEAPBYTES` bytes allocated by `BARCH` alone
12. `B.STATS` lots of statistics on internal datastructures, node count, defrag, pages etc.
13. `B.OPS` operation counts of various `BARCH` api commands
14. `B.KEYS` scan keys with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/KEYS.md)
15. `B.VALUES` scan values with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/KEYS.md)

# Installation
1. Checkout and build on (linux only) with gcc 10 or above
2. `mkdir build`
3. `cd build`
4. `cmake .. -DTEST_OD=ON` -  add `-DCMAKE_BUILD_TYPE=Debug` for debug modes
5. Test: `ctest`
6. `start valkey-server valkey.conf --loadmodule {src code/Release/cdict.so}`

# Hardware Compatibility
1. amd64+sse
2. arm+neon

# Toolchain Compatibility
1. Debian + gcc 13
2. any compiler that supports C++ 20

# Server Environment
1. Valkey v. 8.0

# Configuration
1. Set max memory use to any value lower than valkey maxmemory if it's higher it will be ignored. postfix with k,m or g for kilo,mega or giga -bytes
```redis
CONFIG SET B.max_memory_bytes 41m
```
2. Enable active defragmentation [on,off,yes,no]
````redis
CONFIG SET B.active_defrag on
````
3. Set eviction policy [allkeys-lru,allkeys-lfu,allkeys-random,volatile-lru]
```redis
CONFIG SET B.eviction_policy allkeys-lru
```
4. Set compression [zstd,off,none]
```redis
CONFIG SET B.compression zstd
```

# Statistics 
An example of statistics returned
```
 1) "'B.STATS'"
    2)  1) 1) heap_bytes_allocated
           2) (integer) 1562240
        2) 1) page_bytes_compressed
           2) (integer) 0
        3) 1) max_page_bytes_uncompressed
           2) (integer) 0
        4) 1) last_vacuum_time
           2) (integer) 0
        5) 1) vacuum_count
           2) (integer) 0
        6) 1) page_bytes_uncompressed
           2) (integer) 0
        7) 1) bytes_addressable
           2) (integer) 0
        8) 1) interior_bytes_addressable
           2) (integer) 0
        9) 1) leaf_nodes
           2) (integer) 0
       10) 1) size_4_nodes
           2) (integer) 0
       11) 1) size_16_nodes
           2) (integer) 0
       12) 1) size_48_nodes
           2) (integer) 0
       13) 1) size_256_nodes
           2) (integer) 0
       14) 1) size_256_occupancy
           2) (integer) 0
       15) 1) leaf_nodes_replaced
           2) (integer) 0
       16) 1) pages_uncompressed
           2) (integer) 0
       17) 1) pages_compressed
           2) (integer) 0
       18) 1) pages_evicted
           2) (integer) 0
       19) 1) keys_evicted
           2) (integer) 0
       20) 1) pages_defragged
           2) (integer) 3508
       21) 1) exceptions_raised
           2) (integer) 0
60) 1) "'B.SIZE'"
    2) (integer) 0
61) 1) "'B.OPS'"
    2)  1) 1) delete_ops
           2) (integer) 1648923
        2) 1) retrieve_ops
           2) (integer) 1000000
        3) 1) insert_ops
           2) (integer) 1648923
        4) 1) iterations
           2) (integer) 0
        5) 1) range_iterations
           2) (integer) 0
        6) 1) lower_bound_ops
           2) (integer) 0
        7) 1) maximum_ops
           2) (integer) 5
        8) 1) minimum_ops
           2) (integer) 5
        9) 1) range_ops
           2) (integer) 5
       10) 1) set_ops
           2) (integer) 0
       11) 1) size_ops
           2) (integer) 1000010
           ```