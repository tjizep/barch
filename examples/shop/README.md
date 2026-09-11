# A shop, out of a key space

A storefront over 7,344 Amazon products: a grid with categories and search, a product
page, a basket, and a checkout that writes an order. The catalog is a file tree in
barch whose directories are the category tree; the product images are not shipped at
all, and arrive from Amazon the first time somebody looks at one.

```
mkdir -p data
barchd --port 14000 --dir data &
PORT=14000 HTTP_PORT=18090 ./setup.sh
# open http://127.0.0.1:18090/shop
```

`prepare.py` turns `../shopping/amazon-products.csv` into `build/`, and `setup.sh`
loads it, and the images arrive as you browse. The checkout's address lookup wants
one more step, because that data is pulled rather than shipped:

```
pip install overturemaps h3 redis shapely
python3 geo.py --port 14000
```

About six minutes. Skip it and the checkout says so rather than offering a picker
that finds nothing.

`--dir` is what keeps `data/` in that first line worth typing. barch writes its
shards to the working directory, so a bare `barchd --port 14000` started here
leaves about 700 `leaves_*.dat` and `nodes_*.dat` files next to `setup.sh` - 350MB
of them once the catalog is in - and the example becomes hard to read. `--dir`
chdirs first, so they all land under `data/`. It does not create the directory,
hence the `mkdir`. Both `data/` and `build/` are gitignored; deleting `data/` with
the server stopped is how you start the shop over from nothing.

## The files

```
examples/shop/
  prepare.py            the CSV to build/, run once
  setup.sh              loads it all and starts the HTTP server
  app/                  the pages and their stylesheet -> shop /app
    index.html            the storefront
    register.html         create an account
    signon.html           sign in
    spaces.html           the key space viewer
    shop.css              the looks, shared by all of them
  luau/                 the stored functions -> shop, as keys
    conf.luau             CONF, the http key: port, user, which routes
    shopui.luau           SHOPUI, the pages
    shopapi.luau          SHOPAPI, /api/*
    spacesapi.luau        SPACESAPI, /api/admin/* - what the viewer reads
    shopimg.luau          SHOPIMG, /img/*
    imgsource.luau        the fs_source behind /img
    imglist.luau          the fs_source_list behind FS LS /img
    sendemail.luau        SENDEMAIL, /api/send-email
  modules/              -> shop /modules
    catalog.luau          find a product by asin
  users/modules/        -> users /modules
    accounts.luau         register, signon, signout, the sid cookie
    sha256.luau           the password hash, in pure Luau
  ratings/modules/      -> ratings /modules
    ratings.luau          GET and POST /api/ratings
  geo/modules/          -> geo /modules
    geo.luau              street and suburb lookup for the checkout
  orders/modules/       -> orders /modules
    orders.luau           placing an order, and reading your own back
  geo.py                loads South African places into geo, run once
  bm25.luau             a scoring module nothing loads yet
  build/                what prepare.py writes, loaded by setup.sh
  data/                 where barchd keeps its shards
```

The five module directories are the shape of the thing: each one is loaded into
the key space whose data it reads, so the code for an account is in `users` beside
the accounts, the code for a review is in `ratings` beside the reviews, the street
lookup is in `geo` beside the streets and the order code is in `orders` beside the
orders. Only `modules/` belongs to `shop`, because the catalog does.

`shop.css` is one stylesheet for all three pages rather than a `<style>` block in
each, which is what they had. It takes its colours and type from `docs/index.html`
- ink and paper greys, cobalt for the thing you click, copper for the accent, IBM
Plex Sans with Archivo for display and JetBrains Mono for the small uppercase
labels - so the shop and the documentation read as one project. The layout is an
ordinary storefront: a white two row header over a grey ground, a sidebar card of
categories, flat product cards with a hairline instead of a shadow, and a dark
full width button at the bottom of each one. The `kind = "files"` route serves it
with the content type read off the extension, so nothing had to be configured for
a second file type.

## What is where

Five key spaces. `shop` holds the catalog:

| path | what |
|---|---|
| `/catalog/<top>/<sub>/<asin>.json` | the full record. **The directories are the category tree** |
| `/meta/index.json` | one compact row per product, so the front page is one read |
| `/meta/names.json` | slug to display name |
| `/modules/catalog.luau` | the lookup the API and the source share, loaded with `require` |
| `/app/index.html` | the storefront, with `register.html` and `signon.html` beside it |
| `where:<asin>` | which catalog path an asin was found at, remembered after the first walk |

and four stored functions: `CONF` (the http key), `SHOPUI` (`kind = "files"`, the
page), `SHOPAPI` (`/api/*`), and `SHOPIMG` (`/img/*`, a files route with a source).

`users` holds the accounts:

| path | what |
|---|---|
| `user:<email>` | `{email, name, salt, hash}` - the account |
| `sess:<sid>` | the email a signed-in cookie belongs to |
| `/modules/accounts.luau` | register, signon, signout, and reading the sid cookie |
| `/modules/sha256.luau` | the password hash, since the sandbox has no crypto |

and `ratings` holds what shoppers said:

| path | what |
|---|---|
| `rating:<asin>:<email>` | `{asin, email, name, stars, comment, seq}` - one per account per product |
| `ratingcount:<asin>` / `ratingsum:<asin>` | the totals, so an average is two reads |
| `/modules/ratings.luau` | `GET` and `POST /api/ratings` |

and `geo` holds the places the checkout's address step looks up:

| path | what |
|---|---|
| `street:<NAME>:<METRO>` | `{name, metro, ways, lat, lon}` - one per distinct road name per metro |
| `sub:<NAME>:<KIND>:<id>` | `{name, kind, lat, lon}` - suburbs, cities, municipalities |
| `prov:<NAME>` | the nine provinces |
| `meta:loaded` | what `geo.py` loaded, and when |
| `/modules/geo.luau` | the lookup behind `GET /api/places` |

and `orders` holds what checkout wrote:

| path | what |
|---|---|
| `order:<id>` | the document, priced on the server |
| `byuser:<email>:<seq>` | the id, so one customer's orders are one walk |
| `/modules/orders.luau` | placing them, and `GET /api/orders` |

None of those four has a stored function or a route of its own. Every route is
in `shop`, because that is the space the HTTP server runs in, and it reaches the
others with `require("users:/modules/accounts.luau")` for the code and
`barch.space.users` for the data. What moved is where the code and the data are
kept, not where either runs - the sections below are about why, and about what it
costs.

`GET /api/categories` has no index behind it: it is `fs.list("/catalog")` and then
`fs.list` on each of those. The tree on disk *is* the answer.

## Images arrive as they are asked for

The catalog holds urls, not pictures - a 40 image sample averages 33kB, so the whole
of it is somewhere around 250MB, and there is no reason to hold any of that until
somebody looks at one. `/img/<asin>` is a **plain files
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
somebody has looked at - 7,344 entries, the ones somebody has opened `file` and all
the rest `remote`, meaning the source knows the name and nothing has fetched it. On
the machine this was written on, after a browse: 168 `file`, 7,176 `remote`. That is
`fs_source_list`, and it is a separate setting from `fs_source` because a file
source answers `{body, type}` and a listing answers names, and both are lists.

Nothing here sets `fs_cache_bytes`, so every image stays once fetched - and at
roughly 250MB if somebody views the whole catalog, this is an example that ought to
set one rather than a demonstration that you need not. A space that wanted a ceiling
would set it and the oldest fetches would go; what was loaded by `LOADFS` is never a
candidate, because the source cannot produce it again.

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

So accounts live in a second key space, `users`, and **so does the code that
reads them**: `users/modules/accounts.luau` is loaded into that space's file
store and `shopapi.luau` reaches it with
`require("users:/modules/accounts.luau")`. The four things in it are in
"What is where" above.

What moving the code does *not* move is where it runs. An HTTP route is a
stored function in the server's space and stays one, so a module required out
of `users` still has `barch.store` and `barch.call` pointing at `shop`.
Everything in `accounts.luau` therefore goes through the `barch.space.users`
handle, and a sibling require has to name the space again -
`require("users:/modules/sha256.luau")`, because a bare `:/...` resolves
against the running space rather than the space the file came out of.

Neither `require("users:...")` nor `barch.space.users` creates a space, so
`setup.sh` runs a bare `USE users` before the HTTP server starts - the same
way `USE` already brings `shop` and `configuration` into being - and loads the
modules with `LOADFS` right after.

There is no crypto library in the Luau sandbox (`open_safe` in
`luau_driver.cpp` opens base, math, string, table, bit32, and the rest - no
hashing), so `users/modules/sha256.luau` is a pure-Luau SHA-256 over `bit32`,
and a password is stored as `sha256(salt .. password)` with an 8 byte random
salt per account. It is not a KDF and it is not constant time - fine for an
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

## Ratings, in a third space

`p.rating`/`p.reviews` on a product record are the Amazon numbers `prepare.py`
imported once and never change. What a shopper actually says on this
storefront is a separate thing, `POST /api/ratings` and `GET
/api/ratings/<asin>`, and it gets a key space of its own, `ratings` - a rating
is data about a product, not about an account, and it should not be dropped
with the catalog either. The code goes with it, the same way the accounts code
went to `users`. The three keys it holds are in "What is where" above.

Posting again on a product you already rated overwrites that one document
rather than adding a second line and keeps the sequence number it had, so an
edit does not jump to the top of the list.

**Two things had to change when this moved out of `shop`.** A space handle has
`get`, `set` and `range` and no `INCRBY`, and `barch.call` goes to the space
the *route* runs in - `shop` - so the counters cannot be bumped the way
`order:seq` a few lines up still is. Faking an increment with a get and a set
loses a vote when two people rate the same product at the same moment and then
drifts for good, so the totals are recomputed from the reviews that are
actually there and written back as a cache: one product's worth of walk, on a
write, never on a read, and a wrong total is corrected by the next one. The
per-product `ratingseq:<asin>` counter went the same way - the next sequence
number is the highest one in the walk plus one - so there is no counter key
left at all. There is still no clock in the Luau sandbox (`open_safe` opens no
`os` library), which is why "newest first" is a sequence number rather than a
timestamp.

And the review list was `DIR LS rating:<asin> SEP :`, which is a command and
so would have walked `shop`. It is the handle's `range` now, bounded
`rating:<asin>:` to `rating:<asin>;` - ':' is 0x3a and ';' is 0x3b, so the half
open bound picks up exactly this product's keys. Underneath it is
`text_range`, which reads the composite region as well as the plain one, so
the TODO 260 problem - a key with a space in it living in another region of
the tree - does not come back.

Rating a product requires being signed in, the same `sid` cookie `/api/me`
reads. `shopapi.luau` resolves the shopper through the accounts module and
hands the ratings module `{email, name}` plus a "does this asin exist"
function, because the ratings space holds opinions about asins and no catalog
to check them against.

Building this turned up a real bug in `barch.space`, TODO 273: the handle held
a pointer into a dense map, so opening the second space moved the first one's
entry and the next read through it died with `bad_function_call`. Three lines
reproduce it and the fix is to hold those entries by pointer.

## Checkout, in three steps

`POST /api/order` used to take a name, an email and an address as one line of
free text, out of a form at the bottom of the basket. It is a flow now - basket,
delivery, payment - in the one panel, because the basket has to stay visible
while somebody is correcting an address.

The step is state and not a route: nothing in it is worth a URL. What does
outlive a reload is the address and the payment choice, in `localStorage`; the
step itself does not, because coming back to a payment screen with no memory of
why is worse than starting again.

**The delivery step looks streets and suburbs up as you type**, out of `geo`,
debounced at 160ms. What it does not do is ask you to pick a house number,
because that data does not exist - see below.

The apartment number comes first and on its own, and the street line is written
the way people write it: `12 Long Street`. There is no street called "12 Long", so
a leading run of digits is split off and only what follows is searched - and while
the line is still just a number there is nothing to look for, so nothing is asked
for. Picking from the list keeps the number that was already typed, which is why
the split is a parse rather than a second field. The order keeps both: `line` as
typed for anything printing a label, `house` and `street` beside it for anything
counting streets, because neither is recoverable from the other for free.

**The payment step offers methods and never asks for a card number.** Cash on
delivery, card on delivery, EFT. An example storefront has no business
collecting a card number, and a demo that puts a PAN field on the screen teaches
the wrong thing to whoever copies it. The order records `{method, state}` and
there is nothing else to store or to leak. The server checks the method against
the same three it offers, for the same reason it prices the basket itself: a
request that arrives with its own payment method is a request that can arrive
with any of them.

## The account screen, and where orders live

Signing in used to get you a button that signed you back out. There is an account
behind it now - who you are, your orders newest first with their state, and sign
out - and the header carries a count of what is still pending.

`GET /api/orders` reads the email off the session and never off the query string,
so there is no version of it that lists somebody else's. `GET /api/orders/<id>`
checks the id against the caller's own index rather than trusting it, because
knowing an order id should not be the same as being allowed to read it. Asked for
one that is not yours, the answer is the same 404 as one that does not exist.

**Orders moved out of `shop` while this was built, and that is the interesting
part.** They were sitting next to the catalog, which is exactly what this README
spends two sections arguing accounts should not do - and dropping the catalog to
clear a stale import during this work took every order with it while `users` and
`ratings` came through. An order has to outlive the catalog for the same reason an
account does, so it lives in `orders` with the code that reads it.

The id allocator had to move too, and could not move as it was. It was
`INCRBY order:seq`, and `barch.call` runs against the space the *route* is in -
`shop` - so the counter would have stayed with the catalog and a reload would
reset it and start handing out ids that already existed. It is sixteen hex
characters out of `math.random` now, which needs nothing kept anywhere. The
per-customer sequence in `byuser:<email>:<seq>` is the same trick the ratings use:
no INCRBY to reach, so the next one is the highest already there plus one, over
the handful of orders one account has.

Checked the way the argument says to check it: place three orders, `FLUSHDB` the
`shop` space down to zero keys, reload the catalog with `setup.sh`, and the three
orders and the session are still there with `shop` holding no `order:*` at all.

Nothing in this example fulfils an order, so every order is `pending`. The field
is there because an order without a state is not an order, and because the
account screen has to show something true.

## Places, and the address data South Africa does not have

`examples/flask/overture.py` loads Overture's `address` theme for Canada - full
addresses, house numbers, postcodes - across nine key spaces. Neither half of
that carries over.

**Overture has no South African addresses.** The `address` theme is
OpenAddresses derived and the country is not in it: Cape Town, Johannesburg,
Pretoria and Durban bounding boxes each answer zero rows. What does have
coverage, both out of OpenStreetMap, is `division` - suburbs, cities, provinces -
and `segment`, which carries named roads. So `geo.py` loads those two, and the
checkout asks for the house number and the postal code as typed text. A picker
that pretended to know South African house numbers would be worse than a field
that admits it does not.

**One key space, not nine.** Nine is what you do when a point lookup and a prefix
walk want different structures - `streets` for the text, `spatial_data` for the
records, `tokey` to get between them, `overflows` because an appended list fills
up. Hybrid keys remove the reason: the ART owns the leaves and a hash indexes
them, so one ordered space answers both. The only design left is that the text
you search by has to lead the key, which is why they read
`street:<NAME>:<METRO>` and not `street:<METRO>:<NAME>`.

Measured on the loaded space - 88,009 keys, `INFO SHARD` reporting
`index_physical:ART+HASH`:

| read | |
|---|---|
| exact key, the hash hit | 32us |
| three letter prefix, 25 results | 1.2ms |
| one letter prefix, 25 results | 1.7ms |

Those are round trips over RESP, so the 32us is mostly the trip rather than the
read. The point is the shape: the same space does both, and neither one needed a
second copy of the data.

What `geo.py` pulls by default is every division in the country and the named
roads of four metros - Cape Town, Johannesburg, Pretoria, Durban - which is
12,138 places and 75,865 street names in about six minutes. `--all-roads` is the
national version and takes hours. The roads are folded to one key per name per
metro as they load, since a road is many segments and the picker wants a name:
238,525 Cape Town segments become 24,472 streets.

## Looking at the key spaces

`/shop/spaces.html`, behind the account button. Every space the server knows
about down the side with its key count, and four tabs: the keys in one with their
values, the file store as a tree where a space has one, the configuration both
per space and global, and the statistics.

**It reads every space the HTTP user can see.** It is read-only and it is behind
a sign-in, but a sign-in on an example storefront is not an admin boundary. Do
not put this on a public port.

Three things had to be worked out rather than assumed, and each one shaped the
code:

**`KEYS` is asynchronous, so a script may not call it.** Keys come off the
`barch.space[name]` handle's range walk instead. Nor does the `space:COMMAND`
prefix help: it works on a RESP connection but not through `barch.call`, because
the runner looks the whole name up in the command table and there is no command
called `GEO:DBSIZE`. So anything per-space goes through a handle and only global
commands - `SPACES`, `CONFIG`, `INFO`, `STATS`, `OPS` - go through `barch.call`.

**The file tree is the key walk, done carefully.** A file is `fs:n:<path>` holding
`{id,size,type,version}` and a directory is `fs:n:<path>/` holding `{dir:true}`,
so one level is a range. The first version walked everything under the prefix and
discarded anything with a slash left in it, which works until a directory is big:
`/catalog` is 7,344 files, the walk ran out inside it, and `/meta` and `/modules`
were simply missing from the listing of `/`. It steps over subtrees now - past a
directory to the first byte that cannot be inside it, which is `/` plus one.

**`CONFIG` is one command for GET and SET.** It carries `read`, `write` and
`config` together, so there is no way to let a route read the settings without
also letting one change them. `setup.sh` grants `+config` and says so; drop it and
the viewer reports the missing grant instead of showing an empty list as though
the server had no settings.

One limit it is honest about rather than hiding: `INFO SHARD` reads the
connection's key space and the route runs in `shop`, so the shard figures describe
`shop` whichever space is selected. The page says which space they are about.

## Things it ran into

**Nine categories with nothing in them** (TODO 279, and not a bug). `LOADFS`
adds and overwrites and never deletes, so a space that was loaded from a larger
catalog keeps the directories the new one does not have - and `GET
/api/categories` is `fs.list`, so it reported them honestly. The empty entries
were real entries. Nothing about the route or the import is wrong: an import is
additive on purpose, because a flag that deleted files because a directory did
not have them would take a tree with it the first time somebody typed the wrong
root. To make this space match `build/` again, empty it and load it: `FLUSHDB`
on `shop`, or stop the server and remove `data/*shop*.dat`, then run `setup.sh`.
Doing that leaves `users` and `ratings` alone, which is the argument for them
being separate spaces, made by accident.

**A key with a space in it was invisible to a range scan** (TODO 260, since
fixed). The first version used category names as directory names, so
`/catalog/Home & Kitchen/...` stored 992 files - the catalog was smaller then - of
which a listing found three. The
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
