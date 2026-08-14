# API documentation standard

This describes how BARCH's reference documentation is written.

Most of it is not really an invention. The HTML reference site had already settled into
a consistent shape across all thirteen of its reference articles before anyone wrote the
rules down, and the older markdown files in this directory had settled into a second,
simpler shape of their own. What follows is mostly those two shapes made explicit, so
that the next page written matches the ones already here. Where something genuinely new
has been added, it is because an audit found a defect that the existing habits would not
have caught, and those places are called out as they come up.

## 1. Scope

This governs `docs/index.html`, which is the reference site and its front page, and it governs the
per-command markdown files that sit beside it - `SET.md`, `GET.md`, `KEYS.md`, `ACL.md`
and the others. The two are written to different templates, because they are doing
different jobs, and sections 2 and 4 describe them separately.

It also governs doc comments on public headers, but only to the extent of section 5, the
part about citing sources. If you want to see the house voice done well, `range_index.h`
and `key_space.h` are the best examples in the tree. Neither is required to carry any
article structure, since that is a site-level concern rather than a source-level one.

`README.md` is a landing page rather than a reference, so it is out of scope, as are
`RELEASES.md`, `TODO.md`, `DONE.md` and `BENCHMARKS.md`.

The site splits on audience, not on how technical the page is. A caller of barch is a
user even when they write Python, speak RESP, or read lock notes. A developer is
someone who works on the tree.

- **User** — the four narrative articles (`overview`, `quickstart`, `configuration`,
  `errors`). How to get in, what the options are, how failures look.
- **Reference** — a surface a caller can invoke. Still user documentation. Section 2's
  spine applies here. The crumb is `Reference / {Topic}`.
- **Developer** — how a surface is built and bound: `#resp`, `#swig-python`,
  `#swig-lua`. These keep the spine, because they still document a surface, but they
  sit after the user material. A caller looking up a command should not have to pass
  through them. The crumb is `Developer / {Topic}`.

User crumbs are `User / {Topic}`. Do not use `Platform` or `Interfaces`; those names
mixed the two audiences.

## 2. The shape of a reference article on the site

A reference article documents a surface that a caller can invoke, and every one of them
follows the same three-part spine:

```html
<article id="ref-{topic}">
<div class="crumb">Reference / {Topic}</div>
<h1>{Topic}</h1>
<p class="lede">...</p>
<div class="chips">...</div>

<div class="block">
  <header><span class="tag ctx">Design context</span><h2>{specific title}</h2></header>
  <div class="bd">...</div>
</div>

<div class="block">
  <header><span class="tag ref">Endpoint reference</span><h2>{specific title}</h2></header>
  <div class="bd">...</div>
</div>

<div class="block">
  <header><span class="tag code">Code matrix</span><h2>{specific title}</h2></header>
  <div class="bd"><div class="tabs">...</div></div>
</div>
</article>
```

All three blocks are required and they always appear in that order, because each one
answers a different question and readers learn to expect them in that sequence. The first
says why the thing is built the way it is, the second says exactly what you may call, and
the third shows it working. Reference documentation tends to become unreadable precisely
when those three get mixed together in one long passage, which is the failure this spine
is meant to prevent.

The `<h2>` inside each block header should be a real title describing that particular
page's material, something like "Routing model" or "Options, commands, counters", rather
than a repeat of the generic block name. The coloured tag beside it already carries the
generic name, so repeating it wastes the only line of the header that could have told the
reader something.

Four articles - `overview`, `quickstart`, `configuration` and `errors` - are narrative
rather than reference, and they are exempt from the spine, though they still take a
crumb (`User / {Topic}`), an `h1` and a lede. They are a closed set. Adding a fifth
should be a deliberate decision that someone argues for, rather than a convenient way
of avoiding the structure. The three Developer articles (`resp`, `swig-python`,
`swig-lua`) are not a fifth narrative page: they keep this spine, and they sit in
their own nav group because they explain how a surface is bound, not what a caller
invokes.

## 3. What belongs in each of the three blocks

### Design context

This block explains why the surface behaves the way it does: the model behind it, the
invariants it maintains, what it costs, and any trade-off a caller can actually feel from
the outside. It is the block that earns the documentation its keep, since the reference
tables below it could in principle be generated but this cannot.

The register to aim for is mechanism rather than adjective. Saying that a route costs one
atomic load plus a binary search over a couple of cache lines tells a reader something
they can plan around, whereas saying it is blazing fast tells them nothing at all, and
quietly costs you their trust for the rest of the page.

This is also the right home for a constraint that a caller cannot design their way out
of. The write-amplification warning on range sharding is the model to follow, because it
says plainly what the cost is, explains why it is not avoidable, and then tells you what
to prefer instead.

One thing to watch: if a limit has an observable failure attached to it, describing it
here is not sufficient on its own, and it needs an error tree entry as well. Section 6
covers that.

### Endpoint reference

This block says what may be called, exactly, and it is where the page becomes tabular.
Every reference block carries at least one `<table>`, since prose alone is not a
reference no matter how carefully it is written. The usual columns are:

| Column | Contents |
|---|---|
| Surface / Name | The command, option, or function, in `<code>` |
| Arguments | Argument names and forms, or `—` |
| Requirement | A `req` marker, described in section 7 |
| Returns / Behaviour | What comes back, and any qualifications on it |

Some surfaces genuinely want different columns, and an options table that needs Type and
Bounds should have them, but the Requirement column stays in every case.

The calling form has to be given precisely enough that a reader can reproduce it without
guessing. Writing `INFO SHARD #n` is not enough if it matters that those are three
separate protocol arguments rather than one string, and in that particular case it does
matter, because sending it as one string reaches a different code path entirely. A
reference that a reader can plausibly misread into a call that does not work has failed
at the one job it had.

### Code matrix

This is a `.tabs` block holding working examples, one pane per environment. Every pane
has to run as written against the surface described above it, with no pseudocode and no
elisions that would stop it executing.

The panes appear in a fixed order so that readers can navigate by position once they have
seen a couple of pages: **python-swig**, then **redis-cli**, then **redis-python**, then
**lua-swig**, and anything else after those. Leave out whichever do not apply, but do not
reorder the ones that remain.

Tab labels come from a controlled vocabulary - `python-swig`, `lua-swig`, `redis-cli`,
`redis-python`, `go-redis` and `rust-async`. Where a pane plays a particular role in a
multi-part example you can qualify the label with a middle dot and a lowercase role, as in
`python-swig · node A` or `redis-python · balance probe`, but the base name itself should
never vary for its own sake. Having both `redis-cli` and `RESP (redis-cli)` on different
pages is the kind of small inconsistency that makes a site feel unmaintained.

## 4. The shape of a RESP command reference

The markdown files in this directory document individual RESP commands, and `SET.md` is
the template most of this is drawn from, since it already arrives at the things a reader
actually needs in roughly the order they need them.

Two of the eight parts below came instead from reading how redis documents its own
commands, and both earn their place. Redis separates a command's required arguments from
its optional modifiers, which `SET.md` does not, and it states the reply type explicitly
rather than leaving it to be inferred from the description. Nothing else from those pages
is wanted here. The presentation stays as it is in this repository and on the site: no
graphical or railroad syntax diagrams, no disclosure widgets, no metadata blocks, no
compatibility matrices, and no version-added lines.

### 4.1 The syntax specification

Open with the full syntax in a fenced `redis` block, with optional parts in square
brackets and alternatives separated by pipes, exactly as redis documents its own
commands. Where a command accepts both upper and lower case forms of a keyword, show
both, since that is a real property of the parser and readers will want to know.

```redis
SET key value [[NX|nx] | [XX|xx]] [GET|get] [[EX|ex] seconds | [PX|px] milliseconds |
  [EXAT|exat] unix-time-seconds | [PXAT|pxat] unix-time-milliseconds | [KEEPTTL|keepttl]]
```

Long specifications should wrap with an indented continuation line rather than running
off the side of the page. Plain text in a fenced block is the whole of it - the bracket
and pipe notation is already precise, and a drawn diagram of the same grammar would only
be a second thing to keep in step with the parser.

### 4.2 The time complexity

State it immediately after the syntax, as a fourth-level heading, and qualify it where the
honest answer has more than one term:

```
#### Time Complexity O(1) (O(k) where k represents keylength)
```

If the constant factors are unusually good or unusually bad, that is worth a sentence of
its own somewhere below. `KEYS.md` does this well: it admits the operation is O(n) and
then explains that a million keys scan in about five milliseconds, and that it does not
block, which turns a number that looks alarming into one a reader can plan around.

### 4.3 What is stored or retrieved, and what happens when it cannot be

A short paragraph, two or three sentences, saying what the command actually does to the
data.

The second half of this is the part most command documentation forgets, and it is
required here: say what happens when the syntax is perfectly valid but the request still
cannot be fulfilled. A `GET` on a key that is not there, a `SET NX` on a key that already
exists, a range whose bounds are the wrong way round - these are not errors in the sense
of a malformed command, and the caller gets a normal reply, but which normal reply is
exactly what they need to know. Say whether they get nil, an empty array, a zero count or
no operation at all, and say it here rather than leaving it to be inferred from the
options list.

### 4.4 The required arguments

The syntax line gives the argument names but not their meaning, so where a command takes
more than a key there is an `#### Arguments` list naming each one, giving its type, and
saying what it does. Offsets, ranges and limits especially need this, since the two
questions a reader always has are whether the bound is inclusive and whether a negative
value means anything.

```
- key (string) -- the key to read.
- start (int) -- the start offset, zero based, counting from the end if negative.
- end (int) -- the end offset, zero based and inclusive.
```

Keep this separate from the options list below it. Required arguments and optional
modifiers are different things to a reader, and running them together in one list means
they have to reconstruct the syntax line from the prose to tell which is which.

A command whose only argument is a key does not need this section, because the syntax
line has already said everything there is to say.

### 4.5 The options list

Under an `#### Options` heading, one bullet per option, each giving the option, its data
type, and a short explanation of what it does. Types are named plainly as **int**,
**double** or **string**, and any constraint on the value is stated with it:

```
- EX seconds (int) -- set the expire time in seconds, must be positive.
- NX -- only set the key if it does not already exist.
- GET -- return the old value stored at key, or nil if the key did not exist.
```

A flag that takes no argument has no type and simply says what it does. Where two options
cannot be combined, say so on both of them rather than only on the first, because a reader
scanning for `XX` should not have to have read `NX` to discover the conflict.

### 4.6 The reply, and where RESP2 and RESP3 differ

Under a `#### Reply` heading, say what type comes back, not merely what it contains. A
reader writing a client needs to know whether they are getting a bulk string, an integer,
an array, a map or a null, because that is what their client library will hand them.

Where the reply is the same on both protocol versions, one line covers it. Where it is
not, both get stated:

```
#### Reply
RESP2: a flat array of field, value, field, value.
RESP3: a map.
```

This matters more here than it does in redis's own documentation, where most pages end up
saying the same thing twice. BARCH really does shape several reply types by the version
the connection negotiated with `HELLO`, and `src/caller.h` is explicit about which: a map
and a set are written as arrays carrying their own RESP3 wire type, but flatten to an
ordinary array on RESP2, and a verbatim string carries a format tag on RESP3 while
arriving as a plain bulk string on RESP2. A connection is on RESP2 unless it asked for 3,
and it is always RESP2 through the valkey module, where the protocol is not ours to
choose.

So any command replying with a map, a set or a verbatim string has two answers and needs
both written down. Commands returning a string, an integer or an array have one, and
should say so in a single line rather than padding it out into two identical ones.

### 4.7 The example

An `### Example` section showing the command actually being run, with its replies. Real
transcript style, the way `USECASE.md` and `ACL.md` both do it:

```redis
B.SET 10 a
B.GET 10
-> a
```

Show the reply for every command that returns something interesting, and prefer an
example that illustrates the option or the edge case the page spent its time on over one
that merely repeats the simplest possible call.

### 4.8 The SWIG synonyms

Every command reachable from the embedded bindings closes with its Python and Lua
equivalents, each under its own active heading. The headings are written as things you
do, not as labels naming a language:

```markdown
### Setting a key from Python

    import barch
    kv = barch.KeyValue("node")
    kv.set("10", "a")

### Setting a key from Lua

    require("barch")
    local kv = barch.KeyValue("node")
    kv:set("10", "a")
```

Separate headings rather than a table or a combined block, because the two bindings differ
in more than punctuation - Lua uses colon-call syntax and one-based indexing, Python
returns its own `Value` variant - and a reader working in one language should be able to
read their section and stop, without picking their way around the other.

Where a command has no binding equivalent, say so in a single line under a heading of the
same form, so that the absence reads as known rather than forgotten.

## 5. Claims should cite their source

Every factual claim in an Endpoint reference table row, and every error tree entry,
carries a source citation as an HTML comment on the line above it:

```html
<!-- src: src/info_api.cpp:98 -->
<tr><td><code>INFO SHARD #n</code></td>...</tr>
```

Readers never see these, and they are not really for readers. They exist so that a claim
can be checked again later against the line it came from, by a reviewer or by a script,
without anyone having to rediscover where in the source the behaviour was decided.

Cite the line that decides the behaviour rather than one that merely mentions it, which in
practice means the assignment for a default value and the site that produces it for an
error. A claim drawn from several places can cite them comma-separated, with the most
decisive first.

Two things are deliberately exempt. Design context prose does not need citations, because
it is argument rather than fact, it changes far less often, and in the audit that produced
this document it was the part that was already correct. Code matrix panes do not need them
either, since the way you verify an example is by running it.

A citation that no longer points at the behaviour it claims is itself a defect, whether or
not the surrounding prose happens to still be true. That is the intended behaviour rather
than a weakness: the citation is there to go stale loudly, at a moment when someone is
looking at it.

The reason this rule exists at all is worth recording. Three defects were found in the
range-sharding article in August 2026, and all three were in exactly the material this
rule covers - one reference table row, one error tree entry, and one example that had
never been run. One of them documented a client-visible error that the code does not
actually produce. None of them were in the design context prose, which is what suggested
that prose was not where the effort needed to go.

## 6. The error tree

A reference article carries an error tree, meaning an `<h3>Error tree</h3>` and a list
inside the Endpoint reference block, whenever any surface on the page can fail in a way
the global `#errors` article does not already cover. Each entry names the trigger, the
outcome and the class, in that order:

```
<trigger> → <what happens> → <class or message>
```

Three things about these are easy to get wrong, and all three were got wrong at least
once before this was written down.

The first is that a silent failure still belongs in the error tree, and if anything it
belongs there more urgently than a returned error does. Range sharding requested on an
unordered space produces nothing visible to the client at all; the server writes a log
line and quietly comes up hash-sharded instead. Because nothing on the connection tells
the caller, the documentation is the only place they can find out, so say explicitly that
it is not client-visible and say how to detect it.

The second is not to promise an error class that the code does not return. Write what the
caller actually observes, and if what they observe is a log line and a changed behaviour,
then write that.

The third is that things which look like errors but are not should also be listed, marked
as such, since a reader who sees per-shard sizes failing to sum during a rebalance sweep
will otherwise go looking for a bug that is not there.

## 7. The class vocabulary

These are the classes the site uses, and new ones should be added here before they are
used anywhere else.

The three block tags are `tag ctx` for Design context, `tag ref` for Endpoint reference
and `tag code` for Code matrix.

Requirement markers appear in the Requirement column and mean:

| Marker | Meaning |
|---|---|
| `req y` | Required; the call is malformed without it. |
| `req n` | Optional, or not applicable, in which case the cell reads `—`. |
| `req c` | Conditional, meaning required in some forms but not others, or required subject to a stated qualification. Always accompanied by text saying which. |

For notices, `notice warning` is for something that will cost the reader if they skip it,
and `notice note` is for context and status. Neither one substitutes for an error tree
entry, however prominently it is placed.

Chips form the one-line capability summary under the lede, with `chip l1` and `chip l2`
for tier availability, `chip lock` for locking behaviour, and `chip o` for everything else,
such as complexity, defaults and ordering guarantees.

## 8. Voice

These are descriptions of what the better pages in this tree already do, rather than
rules invented from nothing.

Write in the present tense and let the system be the subject, since it does these things
now rather than being something that will do them. Prefer the mechanism to the claim, and
give the number, the ordering or the lock rather than an adjective standing in for one.
Where something has a cost, say what it costs in the same breath as what it buys, because
a reader who finds the cost later on their own will reasonably wonder what else was left
out.

Let the sentences breathe. Short declarative fragments stacked one after another read as
anxious rather than authoritative, and a page written entirely in them is exhausting well
before it is informative. Vary the length, join clauses where they belong together, and
allow an explanation to unfold across a paragraph instead of landing as a series of
verdicts.

Never document an intention. Document behaviour that exists at the stated version, and
where something is not yet on `main`, mark it with a `notice note` naming the branch, as
the range-sharding article does.

There is no place here for marketing register, exclamation marks, or exhortation aimed at
the reader.

## 9. Versioning and branch status

An article documenting behaviour that is not yet on `main` opens with a `notice note`
saying which branch and version it describes and what `main` does instead. When the branch
merges, that notice comes out as part of the same change rather than being left for
somebody to notice later.

## 10. Where things do not yet conform

Recorded here so that a missing section reads as a known gap rather than as somebody's
judgment call.

Error trees are missing from `ref-hash`, `ref-ordered`, `ref-list`, `ref-acl`,
`ref-compression`, `ref-memory`, `ref-stats`, `swig-python` and `swig-lua`. Under section
6 each of those needs one wherever the surface can fail beyond the generic cases, and
working out what each should say means reading the failure modes out of the source first.

`swig-lua` is the only Endpoint reference block on the site with no table in it, which
section 3 requires.

No citations exist anywhere yet, so section 5 currently describes an intention rather than
a practice, and retrofitting them is per-article work.

Tab labels have not been normalised to the vocabulary in section 3, and `RESP (redis-cli)`,
`Go`, `Go (go-redis)` and `Rust (async)` all still appear alongside the base names.

Code matrix panes have never been executed as a gate, which is how the broken `INFO SHARD`
call survived long enough to be found by reading.

None of the markdown command references carry the eight-part shape in section 4 in full.
`SET.md` comes closest, which is why it was the template, but even it has no SWIG synonyms
section and does not say what happens on the unfulfillable cases beyond what the options
list implies. The shape is however now carried in full by all hundred and ten commands in
the `#ref-commands` detail panels, which is the first place in the tree that section 4
describes rather than aspires to.

## 11. Migrating the older markdown files

The per-command files here predate the site and overlap it. They are not being brought up
to the site's three-block spine, partly because markdown cannot really carry it, and
mostly because maintaining a second parallel reference is a liability whatever shape it
takes. Instead each file gets resolved one of two ways.

A file is **retired** when its content already exists in `docs/index.html` at equal or
better depth, in which case the body is replaced with a link to the relevant article
anchor so that any published URL keeps working. A file is **absorbed** when it holds
material the site lacks, in which case that material moves into the appropriate article
first and the file is then retired as above.

Which applies to what, and whether the file is currently reachable from `README.md`, since
only linked files need the redirect stub:

| File | Linked from README | Disposition |
|---|---|---|
| `SET.md`, `GET.md`, `KEYS.md`, `REM.md`, `ADD.md` | no | Retire into `#ref-keyvalue`; no stub needed, nothing links to them |
| `ZFASTRANK.md` | yes | Retire into `#ref-ordered`, stub required |
| `APIS.md` | yes | Audit first; it is an index of surfaces and may be wholly superseded by the site's own navigation |
| `ACL.md` | no | Retire into `#ref-acl` |
| `COMPRESSION.md` | yes | Retire into `#ref-compression`, stub required |
| `USECASE.md` | yes | Absorb; it is narrative material the site does not carry |
| `CONFIGURATION.md` | no | Absorb into the `configuration` narrative article |
| `PYTHONSERVEREXAMPLE.md` | yes | Absorb into the `#swig-python` Code matrix, stub required |

There is a real tension here worth naming rather than papering over. Section 4 defines a
shape for RESP command references, and this section proposes retiring most of the
files that would carry it. Both can be true at once, but only if the decision is made
deliberately: either the command references stay as markdown and get brought up to section
4, or they move into the site and section 4 becomes the shape of the per-command entries
inside `#ref-keyvalue`. That choice has not been made yet, and section 4 is written to
survive either outcome.

`COVERAGE.md`, `DOCKER.md`, `BENCHMARKS.md` and `SWIG.md` here, along with `RUNPY.md` and
`RUNVALKEY.md` in the repository root, describe operational and build concerns rather than
API surfaces, so they are out of scope and stay as they are.

No migration is performed by this document. It only describes the target.
