-- Product images, out of the `images` key space's file store.
--
-- A files route answered in C++ with no luau per request - and `space = "images"`,
-- so it serves that space's files rather than the server's own, and `source = true`,
-- so a picture the space does not have is asked for once (imgsource.luau, named by
-- `images.fs_source`) and stored as a file. Pictures are files there, not keys,
-- because a key tops out at 512 kB and a picture may not.
--
-- Opt in rather than automatic because a fetch waits inline and holds this route's
-- place in the VM pool while it does, which the reader of a route ought to see.
--
-- Needs a barchd built after 18-09-2026: an older one ignores `space` and serves
-- the route from the server's own space, so every picture is a 404. `HTTP STATUS`
-- ends this route's line with `space=images` when it is understood.

function call()
    return "shopimg"
end

function transport()
    return {
        kind = "files",
        route = "/img/*",
        root = "/img",
        space = "images",
        source = true,
        cors = "*",
    }
end
