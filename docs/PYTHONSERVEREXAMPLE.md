The python code whould look something like this
Client/Server A
```python
import barch
barch.start("127.0.0.1","13000")
barch.publish("w.x.y.b","13000")
h = barch.HashSet()
h.add("ka",["field1","value1","field2","value2"]) # gets sent to b
...
```

Client/Server B
```python
import barch
barch.start("127.0.0.1","13000")
barch.publish("w.x.y.a","13000")
h = barch.HashSet() 
h.add("kb",["field1","value1","field2","value2"]) # gets sent to a
...
```
