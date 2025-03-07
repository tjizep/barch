local vk
vk = redis

local count = 10000
local result = {}
local chars = {'a','b','c','e','f','g','h'}
local radix = #chars
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0
local logperiod = 10000
local inc = function()
    index = index + 1
    return index
end

local tochars123 = function(num)
    return 'a'..chars[math.mod(num, radix)+1]..(90 + num)
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}
    result[inc()] = vk.call('ODLB',"abaachcd")

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('ODSET',k,v)
        vk.call('ODREM',k)
        vk.call('ODSET',k,v)
        if math.mod(i,logperiod) == 0 then
            vk.log(vk.LOG_NOTICE, "Adding "..i)
        end
    end
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        if vk.call('ODGET',k) ~= v then
            result[inc()] = {k, v, vk.call('ODGET',k)} --vk.call('cdict.lb',k)
        else
            successes = successes + 1
        end
        if math.mod(i,logperiod) == 0 then
            vk.log(vk.LOG_NOTICE, "Checking "..i)
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
        if math.mod(i,logperiod) == 0 then
            vk.log(vk.LOG_NOTICE, "Removing "..i)
        end

    end

    result[inc()] = {[['ODSTATS']], vk.call('ODSTATS')}
    result[inc()] = {[['ODSIZE']], vk.call('ODSIZE')}
    result[inc()] = {[['ODOPS']], vk.call('ODOPS')}
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}

end

--[[ Testing num hash string key types]]

convert = tochars123
test()
clear()
--assert(successes==2*count, "test failures")
--assert(failures==0, "test failures")

return result