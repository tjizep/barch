# Docker Examples

Barch can run in a docker image called barch:api
to build it use

```
sudo docker build --progress=plain -t barch:api .
sudo docker run -p 127.0.0.1:8000:8000 barch:api
```
- This will publish a small flask website running barch on port 8000
- The docker image is updated from github each time the api branch is pushed