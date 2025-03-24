#### Fast Ranged Queries and Aggregates

`B.ARCH` is adept at providing fast (O(k)) ranged queries based on the underlying order of keys.

Lets consider a simple example to illustrate:

First add some data:

```redis
B.SET bicycle:yours 1
B.SET bicycle:mine 1
B.SET bicycle:theirs 1

B.SET car:yours 1
B.SET car:mine 1
B.SET car:theirs 1

B.SET cat:yours 1
B.SET cat:mine 1
B.SET cat:theirs 1

B.SET eyes:yours 1
B.SET eyes:mine 1
B.SET eyes:theirs 1
```

