# Docker Image from dockerhub

Run below to expose an example flask application to see the API's in action
```
sudo docker ps -a -q | sudo xargs docker stop 
sudo docker run --ulimit memlock=-1 --network=host teejip/barch:v0.4.2.3b

```
binaries are located under `/home/barch/setup` within the docker image

It's also usable as a valkey module and can be started as
```
valkey-server --loadmodule _barch.so
```

# Docker Examples

Barch can run in a docker image called barch:api
to build it use

```
sudo docker build --progress=plain -t barch:api .
sudo docker run --ulimit memlock=-1 --network=host barch:api
```
- This will publish a small flask website running barch on port 8000
- The docker image is updated from github each time the api branch is pushed
- to stop and clean previous containers (on ubuntu)
```bash
sudo docker ps -a -q | sudo xargs docker stop | sudo xargs docker rm
```