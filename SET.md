## SET Command
```redis
SET key value [[NX|nx] | [XX|xx]] [GET|get] [[EX|ex] seconds | [PX|px] milliseconds |
  [EXAT|exat] unix-time-seconds | [PXAT|pxat] unix-time-milliseconds | [KEEPTTL|keepttl]]
```

#### Time Complexity O(1) (O(k) where k represents keylength)

Set a key in floating point, integer or string format. If the key exists the value is updated. 
If the key does not it is added.

#### Options 
The SET command supports a set of options that modify its behavior:

- EX seconds -- Set the specified expire time, in seconds (a positive integer).
- PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
- EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
- PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
- NX -- Only set the key if it does not already exist.
- XX -- Only set the key if it already exists.
- KEEPTTL - Keep the time to live associated with the key.
- GET -- Return the old string stored at key, or nil if key did not exist. The key is converted to a string if it is not.
### Example

```redis
B.SET 10 a
B.GET 10
-> a
```

### Key Types
Key type will be auto-detected. The storage format of a integer or floating point type will be 64-bits.
The endian-ess of the key is determined by the platform and hardware.

### Key Order
Different types will not mix in the overall order, however,
keys of different types can be added in the same database.

Each key contains a 1 byte prefix determining the type.

The type order is

1. Integers (64-bits) [prefix 0x00]
2. Double Precision Floating Point (64-bits) [prefix 0x01]
3. Strings 8-bit per char [prefix 0x02]
4. Other [prefix >= 0x03]

