-- kind=http: bind options plus the resource keys.
-- HTTP START CONF uses this. Port can still come from the command.
-- user is the default for every request that has no sid and no route pin.

function call()
    return "http"
end

function transport()
    return {
        kind = "http",
        bind = "127.0.0.1",
        user = "web",
        keys = {"ECHO", "PAGE", "SESSION", "HITS", "HITSSTR", "JSON",
                "HEALTH", "STATS", "LOGIN", "WHO"},
    }
end
