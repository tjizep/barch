-- The content being browsed: whatever the git repository put under /repo.
--
-- The same C++ files route as /browser, pointed at the other root. This is what
-- the file manager downloads through, and what makes a click on a file in the
-- browser a Range-capable GET with an ETag rather than a script reading a blob.

function call()
    return "repofiles"
end

function transport()
    return {
        kind = "files",
        route = "/files/*",
        root = "/repo",
        cors = "*",
    }
end
