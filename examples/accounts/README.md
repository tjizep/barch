# Accounts, as a repository of its own

Customer accounts and sign-in sessions for a barch app: register, sign on, sign out,
and the `sid` cookie that says who you are. The shop uses it, but nothing here
knows about the shop. It's split out so other apps can depend on it too.

```
accounts.luau   register, signon, signout, the session cookie
sha256.luau     the password hash, in pure Luau over bit32
```

It expects to live in the `users` key space's file store under `/modules`. The code
reaches its data with `barch.space.users` and its hash with
`require("users:/modules/sha256.luau")`, so that's where it has to go. An app pulls
it in with a `depends` entry in its own `package.luau`:

```lua
depends = {
    { name = "accounts", url = "https://github.com/tjizep/barch-accounts.git",
      space = "users", as = "fs", fs_root = "/modules", pull = true },
},
```

and reaches it with `require("users:/modules/accounts.luau")`. The app's package
should list `users` under `spaces` too, so the space exists with the settings it
wants before accounts is imported into it.

To publish it, make this folder the root of a git repository and push it wherever
the app's `url` points. Without git, `LOADFS <this folder> /modules` in the `users`
space does the same thing, which is what the shop's `setup.sh` does.

Each account is `user:<email>` holding `{email, name, salt, hash}`, and each session
is `sess:<sid>` holding the email it belongs to, both in `users`.
