-- The http key for the browser example: which routes this space serves.
--
-- Everything here is one key space. The repository lands in the file store of that
-- space, the assets land beside it, and the routes below are stored functions in it,
-- so the whole example is "one key space, served".

function call()
    return "browser conf"
end

function transport()
    return {
        kind = "http",
        bind = "127.0.0.1",
        user = "web",
        keys = {"BROWSER", "REPOFILES", "FMAPI"},
    }
end
