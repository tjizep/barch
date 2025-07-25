# Release v0.3.1b 2025-07-25

## New Features

1. Added `TTL`, `COUNT`, `EXPIRE` and `RANGE` commands
2. `COUNT a b` command uses cumulative index
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

