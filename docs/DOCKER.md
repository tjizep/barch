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