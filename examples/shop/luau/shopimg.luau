-- Product images.
--
-- A files route, answered in C++ with no luau per request - and `source = true`, so
-- a file this space does not have is asked for once (imgsource.luau) and stored.
-- That is the whole of it: the fetching used to be a handler here, and is now a
-- property of the space with an opt in on the route. See TODO 263.
--
-- Opt in rather than automatic because a fetch waits inline and holds this route's
-- place in the VM pool while it does, which the reader of a route ought to see.

function call()
    return "shopimg"
end

function transport()
    return {
        kind = "files",
        route = "/img/*",
        root = "/img",
        source = true,
        cors = "*",
    }
end
