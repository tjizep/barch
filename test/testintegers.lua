local vk
vk = redis

local count = 1000
local result = {}
local i = 1
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local keylen = 16
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0

local inc = function()
    index = index + 1
    return index
end

local tocharsnum = function(num)
    if num % 2 == 0 then
        return (num + 1634556)
    else
        return -(num + 1634556)
    end
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}
    result[inc()] = vk.call('ODLB',"abaachcd")

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('ODSET',k,v)
    end
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('ODGET',k) ~= v then
            result[inc()] = {k, v, vk.call('ODGET',k)} --vk.call('cdict.lb',k)
        else
            successes = successes + 1
        end

    end
    result[inc()] = {"'ODRANGE',convert(2), convert(count-2), 10", vk.call('ODRANGE',convert(2), convert(count-2), 4)}
    result[inc()] = {[['ODMIN']], vk.call('ODMIN')}
    result[inc()] = {[['ODMAX']], vk.call('ODMAX')}
    result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], vk.call('ODSIZE')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end


local clear = function()
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('ODGET',k) == nil then
            result[inc()] = {"Failed get before remove",k, v, vk.call('ODGET',k)}
            failures = failures + 1
        end
        if vk.call('ODREM',k) == v then
            successes = successes + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('ODGET',k)}
        end

        if vk.call('ODGET',k) then
            result[inc()] = {"Failed remove",k, v, vk.call('ODGET',k)}
            failures = failures + 1
        end
    end

    result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], vk.call('ODSIZE')}
    result[inc()] = {[['ODOPS']], vk.call('ODOPS')}
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}

end

--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()
clear()
assert(successes==2000, "test failures")
assert(failures==0, "test failures")

return result