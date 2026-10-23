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

Money is whole cents, kept as 64 bit integers: every number the examples store is the
8 bytes setInt64At writes (little endian, signed), read back with getInt64At. Luau's
`integer` type holds them exactly, where a Luau number is a double and stops being
exact past 2^53. The catch is that integers don't take + - * < at all. Use the
library (integer.lt, integer.min and so on), and plus, minus and times here, which
raise on overflow where integer.add and friends would quietly wrap.
]]

local REFUSED = {}      -- what refuse() raises, told apart from every real error
local ZERO = integer.create(0)

-- give up on purpose (not enough money, the seat's taken): nothing written is kept
function refuse()
    error(REFUSED)
end

-- digits (an argument, a field of a record) as an integer. Anything else, or a
-- number that doesn't fit in 64 bits, is an error: integer.fromstring would clamp it.
function whole(v)
    local n = integer.fromstring(v)
    if n == nil or tostring(n) ~= v then
        error("not a whole number that fits in 64 bits: " .. tostring(v), 0)
    end
    return n
end

-- a + b, a - b and a * b, as integers, raising rather than wrapping on overflow
function plus(a, b)
    local n = integer.add(a, b)
    if integer.lt(b, ZERO) ~= integer.lt(n, a) then
        error("a sum went past 64 bits", 0)
    end
    return n
end

function minus(a, b)
    local n = integer.sub(a, b)
    if integer.lt(b, ZERO) ~= integer.lt(a, n) then
        error("a difference went past 64 bits", 0)
    end
    return n
end

function times(a, b)
    if a == ZERO or b == ZERO then
        return ZERO
    end
    local n = integer.mul(a, b)
    -- -1 times the smallest one is the one product whose check would divide by -1 and trap
    if (a == integer.create(-1) and b == integer.minsigned) or integer.div(n, a) ~= b then
        error("a product went past 64 bits", 0)
    end
    return n
end

-- a key read for update: locked for the write that follows. nil as `default`.
function for_update(key, default)
    barch.store.get(key, true)              -- the lock: getInt64At on its own would share it
    return barch.store.getInt64At(key) or default
end

-- a key read, nil as `default`
function read(key, default)
    return barch.store.getInt64At(key) or default
end

-- an integer written as the key's 8 bytes
function put(key, n)
    barch.store.setInt64At(key, n)
end

-- `by` added to the integer at `key` (nothing there is 0), and the new integer.
-- incrInt64ByAt locks the key for writing, and raises on overflow.
function add(key, by)
    return barch.store.incrInt64ByAt(key, by, by)
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
