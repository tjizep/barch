## GET Command
Retrieves the value associated with a specific key. If the key does not exist (nil) is returned.
#### Time Complexity O(1) (O(k) where k represents keylength)

## Example

```redis
B.SET 10 a
B.GET 10
-> a
```

## Key Types
Key type will be auto-detected on input. 

## Key Order
See (Order)[./SET.MD] for more information on key order