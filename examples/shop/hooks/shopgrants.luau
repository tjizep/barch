-- package.luau's after hook - TODO 582.
--
-- The one thing setup.sh does that a package can't say for itself: the routes run
-- as `web`, and they call a stored function and write orders, so `web` needs
-- `function` and `data` on top of what the built-in one has. `config` is for the
-- space viewer's settings page - see setup.sh before copying that grant.
--
-- A hook runs as the repository's `user`, which whoever runs barchd picks
-- (`barchd -g URL user=default`), so this grant is theirs to allow, not the
-- repository's.
function call(repo, from, to)
    barch.call("ACL", "SETUSER", "web", "on", "+read", "+write", "+data", "+keys",
               "+function", "+config")
    return "ok"
end
