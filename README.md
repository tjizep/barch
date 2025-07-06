# Introducing *BARCH*
[![Ubuntu 24.04 CI (GCC 13)](https://github.com/tjizep/barch/actions/workflows/ubuntu24.yml/badge.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu24.yml)
[![Ubuntu 22.04 CI (GCC 11)](https://github.com/tjizep/barch/actions/workflows/ubuntu22.yml/badge.svg)](https://github.com/tjizep/barch/actions/workflows/ubuntu22.yml)

`BARCH` is a low memory, dynamically configurable, constant access time ordered cache similar to [valkey](https://valkey.io/) and redis

Additionally it has an embedded server+client for python which can reduce read latency to micro-seconds.
It implements the Z* (OrderedSet), H* (HashSet) and key value API's available in redis.
A demo ubuntu 22.04 docker image is available at teejip/barch:apis

Run below to expose an example flask application to see the API's in action
```
sudo docker run -p 127.0.0.1:8000:8000 barch:apis
```
binaries are located under `/home/barch/setup` within the docker image

It's also usable as a valkey module and can be started as
```
valkey-server --loadmodule _barch.so
```

### Advantages of the embedded L1 server

- far lower read latency (sub micro-second instead of milli-seconds)
- automatic scaling (to a point)
- concurrency through sharding
- optionally serverless (L1 only or L1 + L2)
- future work may add object stores such as S3 or dynamo as L2 or even L3 layers

The embedded server can also run in a multi-writer replication configuration in conjunction with a standalone server.
An embedded server can also be quickly synchronized with a standalone server using the block load api.

The python code whould look something like this
Client/Server A
```python
import barch
barch.start("127.0.0.1","13000")
barch.publish("w.x.y.b","13000")
h = barch.HashSet()
h.add("ka",["field1","value1","field2","value2"]) # gets sent to b
...
```

Client/Server B
```python
import barch
barch.start("127.0.0.1","13000")
barch.publish("w.x.y.a","13000")
h = barch.HashSet() 
h.add("kb",["field1","value1","field2","value2"]) # gets sent to a
...
```


## Whats New (17 June 2025)
### Replication to Facilitate micro second local level api's

Barch contains two modes of replication now
- Block retrieve using barch.load("ip","port")
- Publish to another instance of barch barch.publish("ip","port")
- replication and local server example at /test/repltest.py

These can be used in conjunction to facilitate ultra low latency read caching. 

### Python API

Barch has a new python api - these mostly follows redis api's but runs on the local in process barch db created by the client. This facilitates very low read latency
- Api examples are under examples/flask/example.py showing how barch can be used as a low latency cache while being replicated to from an existing 
- a docker demo image is located at docker hub teejip/barch:main
- Further examples can be found at test/testbarch.py

### Docker Demo Image
The `Dockerfile` contains a python and flask build to demonstrate some of barch's features

### Fixes
- fixed lowerbound and iterators 
- fixed large page support

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
Barch has on average 50% less latency the difference increases with key count and requests



# Features
1. Ordered: minimum, lower-bound and maximum operations are constant time
2. Low memory footprint: Half that of standard hash map while providing similar single threaded latency 
3. Compression: Memory use can be lowered even further using zstd compression at the expense of latency
4. Dynamic config: All options can be configured at runtime without restarts or reload
5. Scalability: All reads are concurrent and scales linearly with core count

# Use Cases
1. [Fast Ranged Queries, Prefix Queries and Aggregates](https://github.com/tjizep/barch/blob/main/docs/USECASE.md)
2. Prefix-cache for fast loading of hints  
3. Z-Order/Morton code range queries for quick spatial bounds checking
4. Fast and accurate counting of small ranges (age, population etc)
5. Many small key value pairs in small footprint with minimal overhead
6. Constant time access priority queue

# Commands and API
1. `B.ADD K V` add a key and value [more](https://github.com/tjizep/barch/blob/main/docs/ADD.md)
2. `B.SET K V` set a key overriding existing value [more](https://github.com/tjizep/barch/blob/main/docs/SET.md)
3. `B.GET K` retrieve a key, returns nil if no key exists [more](https://github.com/tjizep/barch/blob/main/docs/GET.md)
4. `B.REM K` removes a key, returns value if success [more](https://github.com/tjizep/barch/blob/main/docs/REM.md)
5. `B.MAX` returns largest key  (or nil) - numbers are less than text
6. `B.MIN` returns smallest key (or nil) if there are no keys
7. `B.RANGE K1 K2 count` returns an array of maximum `count` containing the keys [see example](https://github.com/tjizep/barch/blob/main/docs/USECASE.md)
8. `B.COUNT K1 K2` returns count of keys within a range [see example](https://github.com/tjizep/barch/blob/main/USECASE.md) 
9. `B.LB K1` lower bound: first key not less than K1 
10. `B.SIZE` returns keys held by `BARCH` this may include expired or evicted volatile keys
11. `B.HEAPBYTES` bytes allocated by `BARCH` alone
12. `B.STATS` lots of statistics on internal datastructures, node count, defrag, pages etc.
13. `B.OPS` operation counts of various `BARCH` api commands
14. `B.KEYS` scan keys with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/KEYS.md)
15. `B.VALUES` scan values with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/KEYS.md)

# Other VALKEY API's implemented 
```
SET
KEYS
INCR
INCRBY
DECR
DECRBY
MSET
ADD
GET
MGET
MIN
MAX
LB
REM
SIZE
SAVE
PUBLISH
LOAD
START
STOP
RETRIEVE
PING
```
## Hash Set API's Implemented
```
HSET
HEXPIREAT
HEXPIRE
HMGET
HINCRBY
HINCRBYFLOAT
HDEL
HGETDEL
HTTL
HGET
HLEN
HEXPIRETIME
HGETALL
HKEYS
HEXISTS
```
## ZOrder API's Implemented
```
ZADD
ZREM
ZINCRBY
ZRANGE
ZCARD
ZDIFF
ZDIFFSTORE
ZINTERSTORE
ZINTERCARD
ZINTER
ZPOPMIN
ZPOPMAX
ZREVRANGE
ZRANGEBYSCORE
ZREVRANGEBYSCORE
ZREMRANGEBYLEX
ZRANGEBYLEX
ZREVRANGEBYLEX
ZRANK

``` 

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
4. Set compression [zstd,fsst,off,none]
```redis
CONFIG SET B.compression zstd
```
