### Thanks for trying BARCH

This is the ubuntu 22.04 x86_64 barch valkey module archive.
It requires valkey to run

1. to install valkey
```
sudo apt-get install valkey
```
3. Running  (with liblbarch.so contained along with this file)
```
valkey-server --loadmodule liblbarch.so
```
4. a Barch RESP interface is created on port 14000 on the local machine (when started) 
