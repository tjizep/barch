## KEYS

### Syntax
```redis
KEYS pattern [COUNT count the keys matching instead of returning them] | [MAX integer to limit results]
```
#### Complexity O(n)

Returns all keys matching pattern in an array.

While the time complexity for this operation is O(N), the constant times are fairly low. 
For example, *BARCH* running on an older laptop can scan a 1 million key database in 5 milliseconds (as opposed to the valkey hash table that blocks and takes 20 ms).

*Note*: Unlike valkey, KEYS and VALUES do not block other access. They only
wait on each other: one KEYS or VALUES at a time, while GET, SET and the rest
keep running. The scan uses a worker pool; its size is
`iteration_worker_count` (`CONFIG SET iteration_worker_count n`, default 4).

KEYS and VALUES do not cause OOM. Matching keys are written to the socket as
they are found, and a walk that would cross `max_memory_bytes` stops and
returns what it has. Use COUNT to return only a count, or MAX n to cap the
reply.

#### Options
- Use the COUNT option to return a count only.
- Use MAX n to limit the total replies

#### Supported glob-style patterns:

- `h?llo` matches hello, hallo and hxllo
- `h*llo` matches hllo and heeeello
- `h[ae]llo` matches hello and hallo, but not hillo
- `h[^e]llo` matches hallo, hbllo, ... but not hello
- `h[a-b]llo` matches hallo and hbllo
