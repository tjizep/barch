### Installation
- install openresty for various operating systems and distributions
  https://openresty.org/en/linux-packages.html#ubuntu
- if you have downloaded `barch.so` (note: not `_barch.so`) copy the file to `/usr/local/lib/lua/5.1`
- you can also determine the path running
  ```
  luajit -e 'print(package.cpath)'
  ```
- else if you have built barch using the normal cmake build process you can run 
  ```
  sudo cmake --install . --component barchlua
  ```
  in the build directory

- to prepare nginx run these commands when the current directory the same as the readme
- below we go to the `posts` example first (assuming you're in the same directory as this readme file)
  ```
  cd posts
  PATH=/usr/local/openresty/nginx/sbin:$PATH
  export PATH
  nginx -p `pwd`/ -c conf/nginx.conf
  curl http://localhost:8000/
  ```

- stopping nginx (run from the `posts` subdirectory)
  ```
  nginx -p `pwd`/ -c conf/nginx.conf -s quit
  ```
  