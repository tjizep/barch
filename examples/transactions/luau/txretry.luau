--[[
The helper the other functions here use: run a function as one pessimistic
transaction with barch.tx.run, which runs it again from the top when another
transaction got in the way, and give up on purpose with refuse().

    local tx = require("txretry")

    local function transfer(source, target, cents)
        ...barch.store reads and writes...
    end

    function call(source, target, cents)
        return tx.in_transaction(transfer, source, target, cents)
    end

It's the Luau twin of ../txretry.py's in_transaction, with one difference. A stored
function can't sleep, so it can't back off between tries the way a client does.
barch.tx.run tries a few times straight away, then gives up with TXRETRY, and the
client calls it again after a pause (in_luau in ../txretry.py). Read what you'll
write with barch.store.get(key, true), in the same key order everywhere, and that's
rare: with no deadlock to meet, a transaction just waits its turn.

Money is whole cents. Luau numbers are doubles, which hold every whole number up to
2^53 exactly, so cents stay exact as long as you only add, subtract, multiply and use
`//`. barch.store holds strings, so turn what it reads into a number with `whole`.
]]

local REFUSED = {}      -- what refuse() raises, told apart from every real error

-- give up on purpose (not enough money, the seat's taken): nothing written is kept
function refuse()
    error(REFUSED)
end

-- a value read (a string, nil) as a whole number, nil as `default`
function whole(v, default)
    if v == nil then
        return default
    end
    return tonumber(v)
end

-- a key read for update as a whole number: locked for the write that follows
function for_update(key, default)
    return whole(barch.store.get(key, true), default)
end

-- a key read as a whole number
function read(key, default)
    return whole(barch.store.get(key), default)
end

-- a whole number written as digits, never as 1e+15
function put(key, n)
    barch.store.set(key, string.format("%d", n))
end

-- `by` added to the number at `key` (nothing there is 0), and the new number
function add(key, by)
    local n = for_update(key, 0) + by
    put(key, n)
    return n
end

--[[
`work(...)` as one pessimistic transaction: what it returns once committed, nil if it
refused. Any other error is raised as it came, after the transaction is aborted.
]]
function in_transaction(work, ...)
    local ok, result = pcall(barch.tx.run, work, true, ...)
    if ok then
        return result
    end
    if result == REFUSED then
        return nil
    end
    error(result, 0)
end

function call()
    return "txretry is a module: require('txretry') from another function"
end
