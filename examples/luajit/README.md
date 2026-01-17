### Installation for luajit or openresty

- usually luajit can be installed using `sudo apt install luajit` on ubuntu systems
- this may not install the lua header files, however.
- follow header file installation instructions here: https://luajit.org/install.html
- or follow the openresty examples at https://openresty.org/en/linux-packages.html#ubuntu
- after barchlua is built using `cmake build .` run `sudo cmake --install . --component barchlua`