# A git checkout in the file store, browsed in a browser

`setup.sh` points barch at `github.com/svar-widgets/vue-filemanager`, has it clone
the repository into a key space's file store, and serves the SVAR file manager at
`/browser` so you can walk around inside it.

It exists to exercise the whole path at once: a git repository, the `fs:` store,
the C++ files route, a luau route doing JSON, and a key space holding all of it.

```
barchd --port 14000 &
PORT=14000 HTTP_PORT=18080 ./setup.sh
# open http://127.0.0.1:18080/browser
```

## What is where

Everything lives in one key space, `browser` by default:

| path | what |
|---|---|
| `/app/*` in the fs | the page and its two bundles, from `LOADFS` |
| `/repo/*` in the fs | the checkout, put there by the git sync |
| `BROWSER` | `kind = "files"`, `/browser/*` → `/app`, `index = "index.html"` |
| `REPOFILES` | `kind = "files"`, `/files/*` → `/repo` |
| `FMAPI` | `kind = "resource"`, `/api/*`, the REST the widget speaks |
| `CONF` | `kind = "http"`, the three keys above |

The git half is six settings in the configuration space:

```
USE configuration
SET git/repositories/browser/url https://github.com/svar-widgets/vue-filemanager
SET git/repositories/browser/space browser
SET git/repositories/browser/as fs
SET git/repositories/browser/fs_root /repo
SET git/repositories/browser/pull on
SET git/repositories/browser/ms 300000
```

`as = fs` is the part worth noticing. A repository is normally imported as keys and
stored functions - `.luau` becomes a function, everything else a key - which is
right for a checkout of scripts and wrong for anything else. `as = fs` puts the
checkout in the chunked file store instead, so what comes out has a size, a content
type, chunking and Range requests behind it. `FUNCTIONS SYNC browser` applies it,
and every five minutes after that; a file deleted upstream leaves the store on the
next sync.

## The application is not the repository

The repository is source: `.vue` and `.js` under `src/`, built by vite. A browser
cannot run any of it, so this does not serve the checkout as a web site. The
checkout is the *content*; the file manager browsing it is three files that
`setup.sh` fetches once and `LOADFS` puts in beside it - `vue.js`, `filemanager.js`
and `filemanager.css`, pre-bundled, plus our own `index.html` with an import map.

Nothing is fetched from a CDN at run time. Every byte the browser loads comes out
of the barch file store, which is the point of the example.

## The REST endpoints

Read out of `@svar-ui/filemanager-data-provider` 2.6.0 rather than guessed:

```
GET    files            the root
GET    files/<id>       one folder, the id url encoded
GET    info/<id>        one entry
POST   files/<parent>   create a file or a folder
POST   upload?id=<p>    multipart - refused, see below
PUT    files/<id>       rename
PUT    files            move or copy
DELETE files            delete
```

`fmapi.luau` answers all of them except `upload`, which is multipart and nothing
here parses it. Every one is a `barch.fs` call: a listing is `fs.list`, a rename is
`fs.rename`, a delete is `fs.remove` or a recursive `fs.rmdir`.

**A move costs what its names cost, not what its data costs.** Chunks hang off a
file id rather than off its path, so moving a folder of a thousand files rewrites a
thousand small name records and touches no content. A copy is the other way round -
one name has one file, with no reference counting - so it duplicates for real and
costs what the data costs.

There is no date column - the metadata carries size, chunk count, content type and
a version, but not a modification time, so the widget's date column stays empty.

## Things it shows that are easy to miss

**Downloads do not touch luau.** `/files/*` is `kind = "files"`, answered in C++.
A luau handler holds a VM slot for the whole call and the pool is 2-8, so a handful
of concurrent downloads of a 120KB bundle would starve every other route on the
server. `test/fstest.py` measures exactly that.

**`/browser` and `/browser/` both work.** A files route can declare an `index`,
which is what a url naming a directory gets. Crow registers the rule with the
trailing slash and redirects the bare form onto it, so `/browser` is a 301 to
`/browser/`.

**One key space, one server.** The routes are stored functions in the `browser`
space and `HTTP START` serves that space's routes. Loading the assets into one
space and declaring the routes in another gets you a working route that 404s
everything.
