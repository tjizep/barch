# The shop

A storefront over 7,344 Amazon products, served by barchd: a grid with categories
and search, product pages, a basket, a checkout that writes an order, and accounts.
It lives in a repository of its own now, with its product data:

**https://github.com/tjizep/barch-shop**

barchd can install it straight from there:

```
mkdir -p data
barchd --port 14000 --dir data -g https://github.com/tjizep/barch-shop user=default
# open http://127.0.0.1:18090/shop
```

Its `package.luau` sets up the key spaces, loads the code and pages, pulls in
[barch-accounts](https://github.com/tjizep/barch-accounts) as a dependency, and
starts the HTTP server. The catalog itself is a Python step against the running
server, and a clone's `setup.sh` does everything at once; the shop's README has
both.

To see what it keeps, add the key space viewer,
[barch-spaces](https://github.com/tjizep/barch-spaces), with a second `-g`.

It's the biggest worked example of what barch does outside a cache: Luau HTTP
routes, the chunked file store, several key spaces with code living beside its
data, `require` across spaces, an `fs_source` that fetches images on demand, and
installing all of it from git.
