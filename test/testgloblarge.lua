local vk
vk = redis

local count = 1000000
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

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('B.SET',k,v)
        if math.mod(i,logperiod) == 0 then
            vk.log(vk.LOG_NOTICE, "Adding "..i)
        end
    end
    result[inc()] = {'B.VACUUM',vk.call('B.VACUUM')}
    result[inc()] = {'B.HEAPBYTES', vk.call('B.HEAPBYTES')}
    vk.call('B.SET','akeyofmineb',1)
    vk.call('B.SET','zkeyofminea',2)
    local q = '?key*'
    result[inc()] = {'QUERY', q}
    local t = vk.call('B.MILLIS')
    local cnt = vk.call('B.KEYS',q,'COUNT')
    successes = cnt
    result[inc()] ={'B.KEYS '..q..' COUNT', cnt}
    result[inc()] = {'TIME', vk.call('B.MILLIS')-t}
    result[inc()] = {'HEAP', vk.call('B.HEAPBYTES')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end

local clear = function()
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i


        if vk.call('B.REM',k) == v then
            successes = successes + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('B.GET',k)}
        end

        if math.mod(i,logperiod) == 0 then
            vk.log(vk.LOG_NOTICE, "Removed "..i.." "..failures)
        end

    end

    result[inc()] = {'COUNT', count}
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}

end

--[[ Testing num hash string key types]]
result[inc()] = {"running test "..tests}
result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "60m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "off")
result[inc()] = vk.call("B.CONFIG", "SET","compression", "zstd")

convert = tochars123
test()
--clear()
assert(successes==2, "test failures")
assert(failures==0, "test failures")

return result