# Accounts

Customer accounts and sign-in sessions for a barch app (register, sign on, sign
out, and the `sid` cookie) live in a repository of their own:

**https://github.com/tjizep/barch-accounts**

[The shop](https://github.com/tjizep/barch-shop) uses it, and other apps can too.
It used to sit in this repository, in the shop's `users/modules`; the code is only on
GitHub now, so there's one copy to change.

It has to be installed into the `users` key space's file store under `/modules`,
because the code reaches its data with `barch.space.users` and its hash with
`require("users:/modules/sha256.luau")`. Two ways to get it there:

From a package, which is what barch-shop's `package.luau` does:

```lua
depends = {
    { name = "accounts", url = "https://github.com/tjizep/barch-accounts",
      space = "users", as = "fs", fs_root = "/modules", pull = true },
},
```

By hand against a running server, which is what barch-shop's `setup.sh` does:

```
redis-cli -3
USE configuration
SET git/repositories/accounts/url https://github.com/tjizep/barch-accounts
SET git/repositories/accounts/space users
SET git/repositories/accounts/as fs
SET git/repositories/accounts/fs_root /modules
FUNCTIONS SYNC accounts
```

Either way, list or `USE` the `users` space first, so it exists with the settings
you want before accounts is imported into it. Then reach it with
`require("users:/modules/accounts.luau")`.
