#### Fast Ranged Queries and Aggregates

`B.ARCH` is adept at providing fast (O(k)) ranged queries based on the underlying order of keys.

Lets consider a simple example to illustrate:

First add some data:

```redis
B.SET bicycle:mountain 1
B.SET bicycle:racing 1
B.SET bicycle:tricycle 1

B.SET car:manufacturer:bmw 1
B.SET car:manufacturer:honda 1
B.SET car:manufacturer:toyota 1

B.SET cat:callycoe 1
B.SET cat:persion 1
B.SET cat:torty 1

B.SET eyes:blue 1
B.SET eyes:brown 1
B.SET eyes:green 1
```
We can now determine which `car` manufacturers there are:

```redis
B.COUNT car:manufacturer: car:~

```
returns 
```redis
(integer) 3
```

And 
```redis
 B.RANGE car:manufacturer:b car:manufacturer:~ 1000
```

Returns
```redis
1) "car:manufacturer:bmw"
2) "car:manufacturer:honda"
3) "car:manufacturer:toyota"
```

While
```redis
B.RANGE bicycle: bicycle:~ 1000
```

Returns
```redis
1) "bicycle:mountain"
2) "bicycle:racing"
3) "bicycle:tricycle"
```

Another example

```redis
B.RANGE eyes: eyes:~ 1000
```

produces
```redis
1) "eyes:blue"
2) "eyes:brown"
3) "eyes:green"
```

Just for funzies

```redis
B.RANGE car: ca~ 1000 
```

```redis
1) "car:manufacturer:bmw"
2) "car:manufacturer:honda"
3) "car:manufacturer:toyota"
4) "cat:callycoe"
5) "cat:persion"
6) "cat:torty"
```