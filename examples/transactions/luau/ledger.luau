--[[
Double entry bookkeeping as a stored function. ../ledger.py has the job and its
rules; `ledger.py --luau` runs it through this.

    CALLF ledger transfer <source> <target> <cents>   the journal entry's number, nil if refused
    CALLF ledger audit <account> [account ...]        every balance added up, in cents
]]
local tx = require("txretry")

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
    if balance[source] < cents then
        tx.refuse()                             -- no overdrafts
    end
    tx.put(source, balance[source] - cents)
    tx.put(target, balance[target] + cents)
    -- both legs in one journal entry, numbered with no gaps
    local entry = string.format("%d", tx.add("journal:next", 1))
    barch.store.set("journal:" .. entry, source .. " " .. target .. " " .. string.format("%d", cents))
    return entry
end

local function audit(...)
    local total = 0
    for _, account in {...} do
        total += tx.read(account)
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
