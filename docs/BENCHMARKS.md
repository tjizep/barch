### Memtier and Valkey Benchmarks of Ordered index (ART)
- [More extensive benchmarks](https://tjizep.github.io/barch_benchmark/index_memtier_1_10.html)
- The Unordered index has even better random point query performance at the expense of `MIN`, `MAX`, `RANGE` and `COUNT` functions
- Use `CONFIG SET ordered_keys off` using a redis client on port 14000
- or `CONFIG SET B.ordered_keys off` within the valkey server (if Barch is loaded as a module)

### Some more benchmark(s)

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
