
# Commands and API

All api's are also available on the built in RESP interface without the `B.` prefix.
Api's with the same signature as VALKEY or REDIS works the same

1. `B.ADD K V` add a key and value [more](https://github.com/tjizep/barch/blob/main/docs/ADD.md)
2. `B.SET K V` set a key overriding existing value [more](https://github.com/tjizep/barch/blob/main/docs/SET.md)
3. `B.GET K` retrieve a key, returns nil if no key exists [more](https://github.com/tjizep/barch/blob/main/docs/GET.md)
4. `B.REM K` removes a key, returns value if success [more](https://github.com/tjizep/barch/blob/main/docs/REM.md)
5. `B.MAX` returns largest key  (or nil) - numbers are less than text
6. `B.MIN` returns smallest key (or nil) if there are no keys
7. `B.RANGE K1 K2 count` returns an array of maximum `count` containing the keys [see example](https://github.com/tjizep/barch/blob/main/docs/USECASE.md)
8. `B.COUNT K1 K2` returns count of keys within a range [see example](https://github.com/tjizep/barch/blob/main/docs/USECASE.md)
9. `B.LB K1` lower bound: first key not less than K1
10. `B.SIZE` returns keys held by `BARCH` this may include expired or evicted volatile keys
11. `B.HEAPBYTES` bytes allocated by `BARCH` alone
12. `B.STATS` lots of statistics on internal datastructures, node count, defrag, pages etc.
13. `B.OPS` operation counts of various `BARCH` api commands
14. `B.KEYS` scan keys with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/docs/KEYS.md)
15. `B.VALUES` scan values with a glob pattern without blocking other calls [more](https://github.com/tjizep/barch/blob/main/docs/KEYS.md)
15. `ACL` and `AUTH` [more](https://github.com/tjizep/barch/blob/main/docs/ACL.md)
16. `B.FIRST` or `B.LB` the lower bound or first key not less than the input
17. `B.NEXT` or `B.UB` the upper bound or next key larger than the input

# ZSTD Dictionary Compression
    - Use `SET CONFIG compression zstd` (port 14000) or `SET CONFIG B.compression zstd` on the valkey port
    - You can use `valkey-server --port 7777 --loadmodule _barch.so` which will put valkey on port 7777 and barch on port 14000
    - Use `TRAIN "SOME DATA"` to add samples for training the dictionary, should be at least five (5)
    - See test/test_data.py for generating some training data - training isn't required though you can just enable compression
    - Use `TRAIN` (no parameters) to complete training and save the trained dictionary - this is used to compress all data
    - If training data does not exist barch will use the values as training data and save a dictionary automatically

# BARCH specific API's implemented
```
MIN                        MAX
SIZE                       SAVE
PUBLISH                    LB, FIRST  
START                      STOP   
RETRIEVE                   LOAD  
ADDROUTE                   ROUTE
REMROUTE                   OPS
STATS                      UB, NEXT
COUNT                      SPACE
USE
```

# Other VALKEY API's implemented (Mostly Key Value + TTL)
```
AUTH                       ACL
SET                        KEYS
INCR                       INCRBY
UINCRBY                    UDECRBY
DECR                       DECRBY
MSET                       DEL
GET                        MGET
REM                        FLUSHDB/FLUSHALL                   
PING                       TTL
EXPIRE                     EXISTS
DBSIZE                     MULTI
EXEC                       CLIENT INFO
INFO                       APPEND
KEYS                       VALUES
SELECT

```
## Hash Set API's Implemented
```
HSET                       HEXPIREAT
HEXPIRE                    HMGET
HINCRBY                    HINCRBYFLOAT
HDEL                       HGETDEL
HTTL                       HGET
HLEN                       HEXPIRETIME
HGETALL                    HKEYS
HEXISTS
```
## Ordered Set API's Implemented
```
ZADD                       ZREM
ZINCRBY                    ZRANGE
ZCARD                      ZDIFF
ZDIFFSTORE                 ZINTERSTORE
ZINTERCARD                 ZINTER
ZPOPMIN                    ZPOPMAX
ZREVRANGE                  ZRANGEBYSCORE
ZREVRANGEBYSCORE           ZREMRANGEBYLEX
ZRANGEBYLEX                ZREVRANGEBYLEX
ZRANK
 
``` 
# List API's Implemented
```
LLEN                        LPUSH
LPOP                        BLPOP
BRPOP                       LFRONT
LBACK
```
