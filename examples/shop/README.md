# A shop, out of a key space

A storefront over 992 Amazon products: a grid with categories and search, a product
page, a basket, and a checkout that writes an order. The catalog is a file tree in
barch whose directories are the category tree; the product images are not shipped at
all, and arrive from Amazon the first time somebody looks at one.

```
barchd --port 14000 &
PORT=14000 HTTP_PORT=18090 ./setup.sh
# open http://127.0.0.1:18090/shop
```

`prepare.py` turns `../shopping/amazon-products.csv` into `build/`, and `setup.sh`
loads it. Nothing else is needed; the images arrive as you browse.

## What is where

One key space, `shop`:

| path | what |
|---|---|
| `/catalog/<top>/<sub>/<asin>.json` | the full record. **The directories are the category tree** |
| `/meta/index.json` | one compact row per product, so the front page is one read |
| `/meta/names.json` | slug to display name |
| `/modules/catalog.luau` | the lookup both routes share, loaded with `require` |
| `/app/index.html` | the storefront |
| `order:<id>` | what checkout writes |

and four stored functions: `CONF` (the http key), `SHOPUI` (`kind = "files"`, the
page), `SHOPAPI` (`/api/*`), `SHOPIMG` (`/img/{asin}`).

`GET /api/categories` has no index behind it: it is `fs.list("/catalog")` and then
`fs.list` on each of those. The tree on disk *is* the answer.

## Images arrive as they are asked for

The catalog holds urls. `/img/<asin>` looks the product up, asks the image space
for that url, and serves what comes back. Measured: **374ms cold, 0.5ms warm**, and
the whole set is about 10MB if every product is eventually looked at.

The fetch waits inline and holds a VM slot while it does, so a cold gallery is
bounded by the pool size. That is the trade for not shipping 10MB of pictures.

The cache is the `shopimg` key space, configured `foreign = luau`: a key that is
not there is filled by `imgspace/imgfetch.luau`, which fetches the url the key
names. So the same cache fills two ways and both are the real one:

```
redis-cli -p 14000
> USE shopimg
> GET https://m.media-amazon.com/images/I/71QeGmahUnL._AC_UX500_.jpg
```

is 280ms the first time and 0ms after, and a page view of the same product is
served out of what that left behind.

The route reads it with `fetch` rather than `get`, and the difference is the point:
`get` returns what is cached and nothing else, while `fetch` is allowed to go and
get it. A read that can take the source's latency is not the same operation as one
that cannot, and in a handler it holds a VM slot while it waits - so it says so at
the call rather than being a property of the key space that the reader cannot see.
That is TODO 259, which also wired `barch.space` into handlers: a route could not
reach another key space at all before it.

## Both ways of holding a catalog

The products are files here, which is what makes `fs.list` the category tree. The
same records work as plain keys - `prepare.py` writes one JSON per product and
`LOADKEYS build/catalog` would import them as `catalog:<top>:<sub>:<asin>.json`
instead. What that loses is the directory listing: a key namespace is walked with
`DIR LS ... SEP :`, which gives the same shape by a different route.

## Things it ran into

**A key with a space in it was invisible to a range scan** (TODO 260, since
fixed). The first version used category names as directory names, so
`/catalog/Home & Kitchen/...` stored 992 files of which a listing found three - the
key had a space, which made it a composite in another region of the tree, and the
scan only asked one region. A range bounds both now and `FS LS` on those same
directories returns all 28. The paths stay slugged anyway, because
`home-kitchen` is a better thing to put in a url than `Home & Kitchen`.

**`barch.call` returned a bulk string with its RESP type byte attached** (TODO 261,
since fixed) - `$hello`, not `hello`. The record came back beginning `${"asin":` and
`simdjson.parse` was quite right to refuse it. `barch.call("CALLF", ...)` works now;
the shared lookup stays a `require`d module because one copy of it in a file both
routes read is the better arrangement anyway.

**A `require` at the top of a file fails at install.** Storing a function runs the
chunk, and at that moment there is no store for `require` to read a module out of.
So the requires are inside the handlers, where they cost one lookup per session
because `require` caches.

**Prices are server side.** The basket posts asins and quantities, never prices;
`/api/order` looks each one up and totals it. A basket that arrives with its own
totals is a basket that can arrive with any totals it likes.
