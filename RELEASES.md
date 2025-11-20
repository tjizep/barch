# Release v0.3.4.15b 2025-11-20

## New Features
1. Add `BLPOP` and `BRPOP` for lists (v0.3.4.3b 2025-11-11)
2. Add lock timeouts (for potential deadlocks) (v0.3.4.7b 2025-11-13)
3. Add `RPUSH` and `RPOP` (Release v0.3.4.8b 2025-11-13)
4. Add `DEL`(v0.3.4.10b 2025-11-14)
5. Allow changing the order during a merge (v0.3.4.11b 2025-11-14)
6. improve hash resize (v0.3.4.11b 2025-11-14)

## Fixes

1. Add a smidgen of static analysis to builds
2. refactor `caller` vm
3. improve test build (v0.3.4.3b 2025-11-11)
4. fix intermittent crash when module gets unloaded (v0.3.4.5b 2025-11-12)
5. fix potential race condition in b[r,l]pop (v0.3.4.7b 2025-11-12)
6. refactor locking (v0.3.4.7b 2025-11-13)
7. fix file extension init bug, causing data to not load (v0.3.4.9b 2025-11-14)
8. Fix timeout on B*POP (v0.3.4.10b 2025-11-14)
9. config initializers  (v0.3.4.11b,v0.3.4.12b,v0.3.4.14b 2025-11-14,2025-11-15)
10. move to C++ 20  (v0.3.4.12b 2025-11-15)
11. fix leaf size bug when key >= 256 bytes (v0.3.4.15b 2025-11-17)
12. update readme ((v0.3.4.15b 2025-11-20)

# Release v0.3.4.2b 2025-11-06 (v0.3.4.5b 2025-11-12)

## New Features

1. Added server_port configuration, 0 disables server if required (v0.3.4.1b 2025-10-01)
2. Improve client replication io (v0.3.4.1b 2025-10-07)
3. Make server replication asynch and faster (v0.3.4.1b 2025-10-08)
4. server replication can be called multiple times per connection (v0.3.4.1b 2025-10-08)
5. add shard info `INFO SHARD [key]|[#(shard number)]` (v0.3.4.2b 2025-11-05)
   - `INFO SHARD 1` returns shard info for key `1`
   - `INFO SHARD #1` returns shard info for shard number 1
   - `INFO SHARD ##1` or `#ABC` returns shard info for key `##1` or `#ABC`
6. add key space support with these new api's key space name's should match `[0-9,A-Z,a-z,_]+` 
   - `SPACES USE ksname` the named keyspace is made the current store for all other API's, a new keyspace will be empty
   - `SPACES UNLOAD ksname` all memory used by named key space is cleared except if it's in use
   - `SPACES` returns a list of loaded key spaces and their sizes
   - total count in the entire db of all shards in all key spaces
     `SIZEALL`
   - save and clear all key spaces
     `CLEARALL`
     `SAVEALL`
   - Key space operators:
   - `SPACES DEPENDS {depend[e|a]nt key space} ON {source key space name} [STATIC]`
     Let a key space depend on a list of one or more source key spaces (dependant missing keys are resolved in source)
     keys are added to the dependent and not propagated to the source
   - `SPACES RELEASE {depend[e|a]nt key space} FROM {source key space name}`
     release a source from a dependent
   - `SPACES DEPENDANTS {key space name}`
     list the dependants
   - `SPACES DROP {key space name}`
       delete keyspace files when there's no more references to it or on program exit
   - `SPACES MERGE {depend[e|a]nt key space} [INTO {source key space name}]`
      Merge a dependent named key space to its sources or any other random key space 
   - `SPACES OPTION [SET|GET] ORDERED [ON|OFF]` sets the current key space to ordered or unordered, option is saved in key space shards
   - `SPACES OPTION [SET|GET] LRU [ON|OFF|VOLATILE]` sets the current key space to evict lru
   - `SPACES OPTION [SET|GET] RANDOM [ON|OFF|VOLATILE]` sets the current key space to evict randomly
   - Key Space random access i.e. `myspace:SET a spaced` or `:GET a` -> `(nil)` or `myspace:GET a` -> `"spaced"`
   - `KSPACE EXIST {key space name} return `1` if space exists else `0`
8. add optional extra part to storage for future backwards compatibility (v0.3.4.2b 2025-11-05)
9. Prevent the `KEYS` command from bringing down a server by allowing only one match using the reserved glob thread group (v0.3.4.2b 2025-11-05)
   So keys is still fast but can not bring down a server (v0.3.4.2b 2025-11-05)
10. `VALUES` match all values in the db against a glob pattern while (v0.3.4.2b 2025-11-05)
11. Added `CLIENT INFO` command - for resp interface only (v0.3.4.2b 2025-11-05)
12. Added `MULTI` command - for resp interface only (transaction support in redis style) (v0.3.4.2b 2025-11-05)
13. Key Space access i.e. `myspace:SET a spaced` (v0.3.4.2b 2025-11-05)
14. Reduce thread count

## Fixes

1. Fix bugs in sharding and replication logic (v0.3.4.1b 2025-10-08)
2. Fix potential network stats bugs (v0.3.4.1b 2025-10-08)
3. RPC Cleanup (v0.3.4.1b 2025-10-09)
4. Shard and Tree Cleanup (v0.3.4.2b 2025-10-14)
5. Thread and memory cleanup (v0.3.4.2b 2025-10-24)
6. Add key space name to rpc and repl (v0.3.4.2b 2025-10-24)
7. fix route test (v0.3.4.2b 2025-10-27)
8. parser case insensitivity (v0.3.4.2b 2025-10-28)

# Release v0.3.3.32b 2025-09-24

## New Features

1. Added `UINCR`, `UINCRBY`, `APPEND` and `PREPEND` (0.3.3.1b 2025-08-01)
2. Unsigned 64-bit integer support (64-bit signed already there) (0.3.3.1b 2025-08-01)
3. Improve parser and call performance (0.3.3.1b 2025-08-01)
4. Added io_context per server thread to try and reduce locking (0.3.3.3b 2025-08-02)
5. improve concurrency (0.3.3.3b 2025-08-02)
6. increase shards to improve write concurrency and performance (0.3.3.4b 2025-08-04)
7. use io_uring for resp socket operations (0.3.3.9b 2025-08-14)
8. general performance improvements (0.3.3.9b 2025-08-14)
9. jump cache to improve read performance (0.3.3.9b 2025-08-16)
10. reduce memory use by 10-15% (0.3.3.9b 2025-08-16)
11. improve update performance 8-15% (0.3.3.14b 2025-08-20)
12. add unordered only mode (unordered_keys config) (0.3.3.15b 2025-08-23)
13. add overflow hash to reduce memory consumption (unordered_keys config) (0.3.3.16b 2025-08-24)
14. buffer some statistics to improve concurrency (0.3.3.16b 2025-08-27)
15. improve write throughput by 80% using lockfree queue (moodycamel) (v0.3.3.19b 2025-09-01)
16. support up to 256 kb key and value size (v0.3.3.25b 2025-09-8)
17. rework and improve lru algorithm (v0.3.3.28b 2025-09-13)
18. add queue statistics (v0.3.3.29b 2025-09-15)
19. add `H` flag to insert into hash (`GET` works without a flag) (v0.3.3.29b 2025-09-16)

## Fixes

1. Readme instructions for docker
2. expose valkey and flask simultaneously
3. fix potential crash in resp parser due to temporary
4. improve code
5. fix stats not saving anymore (0.3.3.6b 2025-08-06)
6. remove performance bug in lowerbound (0.3.3.7b 2025-08-08)
7. fix resp reply string type for keys (not simple string anymore) (0.3.3.8b 2025-08-10)
8. fix auth read race condition (0.3.3.10b 2025-08-17)
9. improve jump cache retention (0.3.3.11b 2025-08-18)
10. fix jump cache race condition (0.3.3.12b & 0.3.3.13b 2025-08-19)
11. fix use after free issue introduced by jump table during updates (0.3.3.16b 2025-08-26)
12. fix release date doc (0.3.3.16b 2025-08-27)
13. fix hash size reporting (0.3.3.16b 2025-08-27)
14. remove mixed hash art - it crashes - art and hash operates separately (0.3.3.17b 2025-08-29)
15. fix loading issue (0.3.3.18b 2025-08-30)
16. fix insert thread ap bug (0.3.3.20b 2025-09-02)
17. improve eviction tests (0.3.3.21b 2025-09-02)
18. improve io uring throughput and latency (0.3.3.22b 2025-09-02)
19. potential fix modify flag in allocator (0.3.3.23b 2025-09-04)
20. hash->queue server rename and modularize. (0.3.3.23b 2025-09-04)
21.  remove thread_local's (0.3.3.23b 2025-09-06)
22.  fix stats vars (0.3.3.23b 2025-09-06)
23. use asio for resp again (0.3.3.24b 2025-09-07)
24. improve hash perf a bit (0.3.3.24b 2025-09-07)
25. fix the issue with last page eviction (0.3.3.25b 2025-09-08)
26. handle dequeue issue with empty keys (also add test) (0.3.3.26b 2025-09-09)
27. switch off memory overflow check in auth db which can lead to unexpected failures when memory is too low(0.3.3.27b 2025-09-10)
28. fix low memory check and test (0.3.3.27b 2025-09-10)
29. fix oom checks (v0.3.3.28b 2025-09-13)
30. fix lru bug with hash table (v0.3.3.28b 2025-09-14)
31. fix rpc bug with hash table (v0.3.3.28b 2025-09-14)
32. fix rpc error handling (v0.3.3.30b 2025-09-17)
33. improve rpc code (v0.3.3.30b 2025-09-18)
34. fix route/cluster test (v0.3.3.30b 2025-09-18)
35. fix lock-free queue order and test (2x yay) (v0.3.3.30b 2025-09-19)
36. disable update queue for now (v0.3.3.30b 2025-09-21)
37. Use simpler circular queue  (v0.3.3.31b 2025-09-22)
38. finalize circular queue  (v0.3.3.32b 2025-09-24)

# Release v0.3.3.7b 2025-08-08

## New Features

1. Added `UINCR`, `UINCRBY`, `APPEND` and `PREPEND` (0.3.3.1b 2025-08-01)
2. Unsigned 64-bit integer support (64-bit signed already there) (0.3.3.1b 2025-08-01)
3. Improve parser and call performance (0.3.3.1b 2025-08-01)
4. Added io_context per server thread to try and reduce locking (0.3.3.3b 2025-08-02)
5. improve concurrency (0.3.3.3b 2025-08-02)
6. increase shards to improve write concurrency and performance (0.3.3.4b 2025-08-04)

## Fixes

1. Readme instructions for docker
2. expose valkey and flask simultaneously
3. fix potential crash in resp parser due to temporary
4. improve code
5. fix stats not saving anymore (0.3.3.6b 2025-08-06)
6. remove performance bug in lowerbound (0.3.3.7b 2025-08-08)

# Release v0.3.3.5b 2025-08-04

## New Features

1. Added `UINCR`, `UINCRBY`, `APPEND` and `PREPEND`
2. Unsigned 64-bit integer support (64-bit signed already there)
3. Improve parser and call performance
4. Added io_context per server thread to try and reduce locking
5. improve concurrency
6. increase shards to improve write concurrency

## Fixes

1. Readme instructions for docker
2. expose valkey and flask simultaneously
3. fix potential crash in resp parser due to temporary

# Release v0.3.3.1b 2025-08-01

## New Features

1. Added `UINCR`, `UINCRBY`, `APPEND` and `PREPEND`
2. Unsigned 64-bit integer support (64-bit signed already there)

## Fixes

1. Readme instructions for docker
2. expose valkey and flask simultaneously
3. tweak docker a little

# Release v0.3.3.1b 2025-08-01

## New Features

1. Added `UINCR`, `UINCRBY`, `APPEND` and `PREPEND`
2. Unsigned 64-bit integer support (64-bit signed already there)

## Fixes

1. Readme instructions for docker
2. expose valkey and flask simultaneously

# Release v0.3.2.3b 2025-07-31

## New Features

1. Added `AUTH`, `ACL` with persistent db
2. Add alias for LB->FIRST and UB->NEXT

## Fixes

1. a grievous update reallocation bug was fixed
2. cleanup expired keys and release memory - adds test (also test issue 1)
3. lower bound can sometimes take very long. now returns quickly
4. update did not really work, works now
5. expiry time not added back during active defrag., now adds it back
6. do not add expired keys back during defrag.

# Release v0.3.1b 2025-07-25

## New Features

1. Added `TTL`, `COUNT`, `EXPIRE` and `RANGE` commands
2. `COUNT a b` command uses the cumulative index
3. Performance Improvements (3X throughput and 3.5x less latency than valkey)
   After 10 Million Keys where added
   Machine is 8 core AMD Zen 4 32 GiB Ram, Ubuntu 20.04
   CPU Usage 32% for Barch, 10% for Valkey and 22% for Benchmark
   BARCH Throughput
   ```
   ./valkey-benchmark -t get -r 10000000 -n 40000000 -P 12 -q --threads 3 -p 14000
   GET: rps=3314784.0 (overall: 3291016.0) avg_msec=0.118 (overall: 0.127)
   ```
   Valkey Throughput
   ```
   ./valkey-benchmark -t get -r 10000000 -n 40000000 -P 12 -q --threads 3
   GET: 1066126.62 requests per second, p50=0.519 msec
   ```
   
# Release v0.3.0b 2025-07-21 

## New Features

1. RESP interface
   Barch provides a RESP (Redis .. Protocol) interface created by the START function or barch.start(interface,port)
   This is a multi threaded asynchronous server provided by the ASIO C++ library
   Performance characteristics are excellent and latency can be 4 times less while throughput at least 2x more than valkey and redis
   (for the API's exposed i.e. SET GET HSET HGET ZADD etc)
   This allows Barch to be used by standard redis clients
   Example performance using standard valkey benchmark
```
./valkey-benchmark -t set -r 10000000 -n 40000000 -P 16 -q --threads 4 -p 14000
WARNING: Could not fetch server CONFIG
SET: 1926411.12 requests per second, p50=0.399 msec  
```
Benchmark latency can go as low as 0.07 ms using these benchmark settings (not possible with valkey or redis)
```
./valkey-benchmark -t set -r 10000000 -n 40000000 -P 1 -q --threads 4 -p 14000
WARNING: Could not fetch server CONFIG
SET: 428908.41 requests per second, p50=0.071 msec 
```
2. ASIO based rpc server on the same port as above
   This is used by the new clustering mechanisms described in the `routetest.py`

3. ROUTE, ROUTEADD, REMROUTE
   To define routes to internal shards in other servers and clients in a cluster.

4. Transparent Failure Recovery
   Cluster network or node failures recover transparently

## Fixes

1. Crash when lru enabled
2. Race condition on max page variable when data is saved
3. Missing and unsorted data in RANGE function
4. 

## Release v0.2.9b Whats New (17 June 2025)
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

