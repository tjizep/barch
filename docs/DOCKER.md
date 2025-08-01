# Docker Examples

Barch can run in a docker image called barch:api
to build it use

```
sudo docker build --progress=plain -t barch:api .
sudo docker run -p 14000:14000 -p 8000:8000 -p 6379:6379 barch:api
```
- This will publish a small flask website running barch on port 8000
- The docker image is updated from github each time the api branch is pushed
- to stop and clean previous containers (on ubuntu)
```bash
sudo docker ps -a -q | sudo xargs docker stop | sudo xargs docker rm
```