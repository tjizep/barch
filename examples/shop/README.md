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
| `/modules/catalog.luau` | the lookup the API and the source share, loaded with `require` |
| `/app/index.html` | the storefront |
| `order:<id>` | what checkout writes |

and four stored functions: `CONF` (the http key), `SHOPUI` (`kind = "files"`, the
page), `SHOPAPI` (`/api/*`), and `SHOPIMG` (`/img/*`, a files route with a source).

`GET /api/categories` has no index behind it: it is `fs.list("/catalog")` and then
`fs.list` on each of those. The tree on disk *is* the answer.

## Images arrive as they are asked for

The catalog holds urls, not pictures - about 10MB across 992 products, and no reason
to hold any of it until somebody looks at one. `/img/<asin>` is a **plain files
route**: no luau runs per request, and the space fetches what it does not have.

That is `fs_source`, a stored function the space names, which is given a path and
produces the file:

```
USE configuration
SET shop.fs_source imgsource
SET shop.missing_ttl 60000
```

`imgsource.luau` turns `/img/<asin>` into a catalog lookup and one `http.request`,
and returns `{body, type}` - a list rather than a string, because `/img/<asin>` has
no extension to guess a content type from. Measured: **220ms cold, 0.3ms warm**, and
a product nobody sells is a 404 that is remembered for `missing_ttl` rather than a
round trip every time.

`FS LS /img SOURCE` lists the whole catalog rather than the handful of pictures
somebody has looked at - 992 entries, one of them a `file` and the rest `remote`,
meaning the source knows the name and nothing has fetched it. That is
`fs_source_list`, and it is a separate setting from `fs_source` because a file
source answers `{body, type}` and a listing answers names, and both are lists.

Nothing here sets `fs_cache_bytes`, so the whole catalog's worth of images - about
10MB if every product is viewed - stays once fetched. A space that wanted a ceiling
would set one and the oldest fetches would go; what was loaded by `LOADFS` is never
a candidate, because the source cannot produce it again.

The route opts in with `source = true`, and that is deliberate rather than a
property of the key space: a fetch waits inline and holds the route's place in the
VM pool while it does, and whoever reads the route ought to see that.

This used to be a luau handler here doing the fetching by hand, and before that a
fill script on a separate foreign key space - which cannot work at all. A fill is
handed a key, and of the three keys a file is made of only the name record carries a
path; the inode and the chunks are keyed by an id with no way back. See TODO 263.

## Both ways of holding a catalog

The products are files here, which is what makes `fs.list` the category tree. The
same records work as plain keys - `prepare.py` writes one JSON per product and
`LOADKEYS build/catalog` would import them as `catalog:<top>:<sub>:<asin>.json`
instead. What that loses is the directory listing: a key namespace is walked with
`DIR LS ... SEP :`, which gives the same shape by a different route.

## Accounts, in a space of their own

`POST /api/register`, `POST /api/signon`, `POST /api/signout` and `GET /api/me`
add registration and sign-on, with a `/shop/register.html` and
`/shop/signon.html` page over them. What they store is deliberately **not**
in `shop`: a customer record is not catalog data, and this space has already
been dropped and reloaded more than once while getting the catalog right (see
the arena and re-import work in the history above) - an account must survive
that.

So accounts live in a second key space, `users`, reached cross-space with
`barch.space.users.<key>` from the handlers in `shopapi.luau`:

| path | what |
|---|---|
| `user:<email>` | `{email, name, salt, hash}` - the account |
| `sess:<sid>` | the email a signed-in cookie belongs to |

`barch.space.NAME` looks a space up; it does not create one, so `setup.sh`
runs a bare `USE users` before the HTTP server starts - the same way `USE`
already brings `shop` and `configuration` into being.

There is no crypto library in the Luau sandbox (`open_safe` in
`luau_driver.cpp` opens base, math, string, table, bit32, and the rest - no
hashing), so `modules/sha256.luau` is a pure-Luau SHA-256 over `bit32`, and a
password is stored as `sha256(salt .. password)` with an 8 byte random salt
per account. It is not a KDF and it is not constant time - fine for an
example storefront, not a reason to reuse it anywhere that has to resist a
real attacker.

Signing in sets an `sid` cookie the same way `examples/http/luau/session.luau`
does, except the session points into `users` rather than into `barch.store`
on `shop` - a session is data about an account, and belongs with it.

`POST /api/send-email` is the shape a real deployment would want for a
welcome mail or a reset link, in `luau/sendemail.luau`: it validates
`{to, subject, ...}` and answers `{ok, queued}` without contacting any
provider. Wiring SMTP or a provider API in is future work; the point here is
the endpoint, not the delivery.

## Ratings, kept apart from the catalog import

`p.rating`/`p.reviews` on a product record are the Amazon numbers `prepare.py`
imported once and never change. What a shopper actually says on this
storefront is a separate thing, `POST /api/ratings` and `GET
/api/ratings/<asin>`, and it lives in `shop` next to `order:<id>` rather than
in `users` - a rating is data about a product, not about an account, the same
distinction that put accounts in a space of their own.

| path | what |
|---|---|
| `rating:<asin>:<email>` | `{stars, comment, seq}` - one per account per product, replaced on a second submit |
| `ratingsum:<asin>` / `ratingcount:<asin>` | running totals, so the average is not recomputed from every review on every read |

Posting again on a product you already rated overwrites that one document
rather than adding a second line, and the two counters move by the
difference (`INCRBYFLOAT`) rather than being resummed - the same shape as
`order:seq`'s `INCRBY` a few lines up. The list of reviews for a product is
`DIR LS rating:<asin> SEP :`, the composite-key walk from "Both ways of
holding a catalog" above, applied to a key namespace this time instead of a
file tree. There is no clock in the Luau sandbox (`open_safe` opens no `os`
library), so "newest first" is a per-product `ratingseq:<asin>` counter
handed out at write time instead of a timestamp. Rating a product requires
being signed in, the same `sid` cookie `/api/me` reads.

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
