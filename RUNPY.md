### Thanks for trying BARCH

This is the ubuntu 22.04 x86_64 barch python module.

1. To install on ubuntu 22.04 x86_64 (may work on later versions)
   
```
tar -xzvf barch-py-ubuntu22-x86_64.tar.gz 
pip install .
```
2. Example Python
```
import barch
k = barch.KeyValue()
k.set("hello", "world")
print(k.get("hello"))
```