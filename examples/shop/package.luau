-- How the shop installs itself from git - TODO 582.
--
--     barchd -g https://.../shop.git user=default
--
-- The user matters twice: hooks run as it, and so does pulling in accounts, the
-- one dependency. Without it neither happens.
--
-- The same steps as setup.sh, said as data: barch applies them on every sync, in
-- this order. What setup.sh still does that this can't is the catalog itself -
-- prepare.py and load_inventory.py are Python, so run those against the server
-- once it is up.
function setup()
    return {
        -- 7 shards, not the default 347: see the note in setup.sh. These are
        -- written before a space first opens, and a space that already exists
        -- keeps the count it was made with.
        spaces = {
            shop      = { shards = 7 },
            inventory = { shards = 7 },
            users     = { shards = 7 },
            ratings   = { shards = 7 },
            orders    = { shards = 7 },
            geo       = { shards = 7, ordered = true },
            images    = { shards = 7, fs_source = "imgsource",
                          fs_source_list = "imglist", missing_ttl = 60000 },
        },
        -- accounts is a repository of its own (examples/accounts), so other apps
        -- can use it. It lands in users:/modules, where shopapi.luau requires it,
        -- and users is listed above so it exists with 7 shards before it does.
        -- Point url at wherever accounts is published.
        depends = {
            { name = "accounts", url = "https://github.com/tjizep/barch-accounts.git",
              space = "users", as = "fs", fs_root = "/modules", pull = true },
        },
        load = {
            { path = "luau",            space = "shop" },
            { path = "hooks",           space = "shop" },
            { path = "images/luau",     space = "images" },
            { path = "app",             space = "shop",    as = "fs", fs_root = "/app" },
            { path = "modules",         space = "shop",    as = "fs", fs_root = "/modules" },
            { path = "ratings/modules", space = "ratings", as = "fs", fs_root = "/modules" },
            { path = "geo/modules",     space = "geo",     as = "fs", fs_root = "/modules" },
            { path = "orders/modules",  space = "orders",  as = "fs", fs_root = "/modules" },
        },
        http = { space = "shop", key = "CONF", port = 18090, bind = "127.0.0.1" },
        hooks = {
            after = { space = "shop", call = "SHOPGRANTS" },
        },
    }
end
