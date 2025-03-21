## SET Command

Set a key in floating point, integer or string format. If the key exists the value is updated. 
If the key does not it is added.

## Example

```redis
B.SET 10 a
B.GET 10
-> a
```

## Key Types
Key type will be auto-detected. The storage format of a integer or floating point type will be 64-bits.
The endian-ess of the key is determined by the platform and hardware.

## Key Order
Different types will not mix in the overall order, however,
keys of different types can be added in the same database.

Each key contains a 1 byte prefix determining the type.

The type order is

1. Integers (64-bits) [prefix 0x00]
2. Double Precision Floating Point (64-bits) [prefix 0x01]
3. Strings 8-bit per char [prefix 0x02]
4. Other [prefix >= 0x03]

