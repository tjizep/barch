## Add Command

Add a key in floating point, integer or string format. If the key exists no action is taken and the database 
remains the same.

## Example

```redis
B.ADD 10 a
B.GET 10
-> a
```
### TTL

Add the ex parameter and another number indicating the time to live. Internally a 64-bit value is added to the key and 
value pair indicating the time at which the key expires. The key will not be available anymore and will be garbage 
collected by the automatic page defragmentation process.

```redis
B.ADD 10 a ex 10
B.GET 10
-> a
...
B.GET 10
-> (nil)

```

## Key Types
Key type will be auto-detected. The storage format of a integer or floating point type will be 64-bits.
The endian-ess of the key is determined by the platform and hardware. 

## Key Order
Different types will not mix in the overall order, however, 
keys of different types can be added in the same database.

Each key contains a 1 byte prefix determining the type.
See [more](https://tjizep.github.io/barch/SET)