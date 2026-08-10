# How a command is dispatched

Developer notes on the machinery behind the RESP surface: how a name becomes a handler,
what the ACL categories on an entry actually do, and the behaviours that are decided at
registration rather than inside the command.

None of this is needed to *use* the commands. The list of them, with syntax and replies,
is the RESP Command Index in `barch-docs.html`.

## What the index is built from

Every command the RESP dispatcher will answer to comes from the registration tables in
`src/*_api.cpp`. One hundred and fifty two names are registered over one hundred and
forty nine handlers - the difference is aliases, where two names share one function, as
`GETRANGE` and `SUBSTR` do, and `FIRST` and `LB`.

Three of a command's behaviours are decided at registration rather than inside the command
body: whether it is an alias, whether it runs on its own thread, and whether it replicates.
The sections below are what each of those means.

## The registration table

There is one map from command name to handler, built once on first use and shared from
then on. Each of the ten API files contributes to it through its own `register_*_api`
function, and `barch_apis.cpp` does nothing more than call them in turn, which is why
adding a command means touching exactly one file.

An entry carries three things beyond the function pointer: the ACL categories the command
demands, a flag saying whether it runs asynchronously, and a call counter that
`INFO commandstats` reads back out.

The valkey module registers separately, through `ValkeyModule_CreateCommand`, and that is
a different table with different failure behaviour. A duplicate name there fails module
initialisation outright - the server refuses to start with "Can't load module" - while the
RESP table simply overwrites. A command added to one and not the other is not an error
anywhere; it is a command that answers on one transport and reports "unknown command" on
the other.

## What the categories do

Authorisation is a subset test rather than a lookup. For every category bit the command
sets, the connected user has to have the same bit, and a user missing any one of them gets
`not authorized` before the handler is reached at all.

The practical consequence is worth stating plainly: a command that declares fewer
categories is *easier* to reach, not harder, so a category accidentally left off a command
silently widens access to it.

Which is the danger in how the categories are written. An entry lists them as strings, and
a name that is not one of the fourteen defined in `barch_apis.cpp` is discarded when the
entry is built rather than rejected - the compiler has no opinion about it. Since
authorisation only tests the bits a command actually declares, a discarded name does not
fail closed. Every `Z*` command carried `ordered` for exactly this reason until it was
corrected to `orderedset`.

## Replication and threading

Two behaviours follow from the entry rather than from the command body.

A command whose categories include both `write` and `data` is replicated to any configured
routes, which is how `SET` travels and `SAVE` does not.

And three commands - `KEYS`, `VALUES` and `RANGE` - are marked asynchronous, meaning they
run on their own thread and do not hold the store still while they scan, which is what
lets a million key pattern scan proceed without blocking other traffic.
