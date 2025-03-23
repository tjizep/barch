## KEYS

### Syntax
```redis
KEYS pattern [COUNT count the keys matching instead of returning them] | [MAX integer to limit results]
```
#### Complexity O(n)

Returns all keys matching pattern in an array.

While the time complexity for this operation is O(N), the constant times are fairly low. 
For example, *BARCH* running on an older laptop can scan a 1 million key database in 5 milliseconds.

*Note*: Unlike valkey this command will not block any other access to the database, it will run in it's own thread. 
However, each reply does take some memory so care should be taken to not choose a pattern to return to many results. 
The COUNT argument can be added to return only a count

#### Options
- Use the COUNT option to return a count only.
- Use MAX n to limit the total replies

#### Supported glob-style patterns:

`h?llo` matches hello, hallo and hxllo
`h*llo` matches hllo and heeeello
`h[ae]llo` matches hello and hallo, but not hillo
`h[^e]llo` matches hallo, hbllo, ... but not hello
`h[a-b]llo` matches hallo and hbllo