# Introducing *BARCH*

`BARCH` is a low memory, dynamically configurable, constant access time ordered key value store for [valkey](https://valkey.io/)

# Features
1. Ordered: minimum, lower-bound and maximum operations are constant time
2. Low memory foot print: Half that of standard hash map while providing similar single threaded latency 
3. Compression: Memory use can be lowered even further using zstd compression at the expense of latency
4. Dynamic config: All options can be configured at runtime without restarts or reload
5. Scalability: All reads are concurrent and scales linearly with core count

# Use Cases
1. Prefix-cache for fast loading of hints  
2. Z-Order/Morton code range queries for quick spatial bounds checking
3. Fast and accurate counting of small ranges (age, population etc)
4. Many small key value pairs in small footprint with minimal overhead
5. Constant time access priority queue

# Commands and API
1. `B.ADD K V` add a key and value
2. `B.SET K V` set a key overriding existing value
3. `B.GET K` retrieve a key, returns nil if no key exists
4. `B.REM K` removes a key, returns value if success
5. `B.MAX` returns largest key - numbers are less than text
6. `B.MIN` returns smallest key
7. `B.KEYRANGE K1 K2 count` returns an array of maximum count 
8. `B.LB K1` lower bound: first key not less than K1
9. `B.SIZE` returns keys held by `BARCH` this may include expired or evicted volatile keys
10. `B.HEAPBYTES` bytes allocated by `BARCH` alone
11. `B.STATS` lots of statistics on internal datastructures, node count, defrag, pages etc.
12. `B.OPS` operation counts of various `BARCH` api commands

# Installation
1. Checkout and build on (linux only) with gcc 10 or above
2. `mkdir build`
3. `cd build`
4. `cmake .. -DTEST_OD=ON` you can add `-DCMAKE_BUILD_TYPE=Debug` for debug modes
5. Test: `ctest`
6. `start valkey-server valkey.conf --loadmodule {src code/Release/cdict.so}`

# Hardware Compatibility
1. amd64+sse
2. arm+neon

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
4. Set compression [zstd,off]
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