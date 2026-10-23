--[[
Double entry bookkeeping as a stored function. ../ledger.py has the job and its
rules; `ledger.py --luau` runs it through this.

    CALLF ledger transfer <source> <target> <cents>   the journal entry's number, nil if refused
    CALLF ledger audit <account> [account ...]        every balance added up, in cents

Balances are int64 cents (see txretry.luau), so they stay exact all the way to 2^63.
]]
local tx = require("txretry")

local ONE = integer.create(1)

local function transfer(source, target, cents)
    cents = tx.whole(cents)
    -- both read for update, since both are written next, and locked in the same order
    -- by every transfer, so A to B and B to A can't each hold one
    local first, second = source, target
    if second < first then
        first, second = second, first
    end
    local balance = {}
    balance[first] = tx.for_update(first)
    balance[second] = tx.for_update(second)
    if integer.lt(balance[source], cents) then
        tx.refuse()                             -- no overdrafts
    end
    -- an overflow raises, and the transaction is aborted with nothing kept
    tx.put(source, tx.minus(balance[source], cents))
    tx.put(target, tx.plus(balance[target], cents))
    -- both legs in one journal entry, numbered with no gaps
    local entry = tx.add("journal:next", ONE)
    barch.store.set("journal:" .. tostring(entry), source .. " " .. target .. " " .. tostring(cents))
    return entry
end

local function audit(...)
    local total = integer.create(0)
    for _, account in {...} do
        total = tx.plus(total, tx.read(account))
    end
    return total
end

function call(action, ...)
    if action == "transfer" then
        return tx.in_transaction(transfer, ...)
    elseif action == "audit" then
        return tx.in_transaction(audit, ...)
    end
    error("ledger takes transfer or audit", 0)
end
