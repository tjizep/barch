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
local isnew = false
local inc = function()
    index = index + 1
    return index
end

local tochars123 = function(num)
    return 'a'..chars[math.mod(num, radix)+1]..(90 + num)
end

local test = function()

    tests = tests + 1

    if vk.call('B.SIZE') < count then
        isnew = true
        for i = 1, count do
            local k = convert(i-1)
            local v = '#'..i
            vk.call('B.SET',k,v)
            if math.mod(i,logperiod) == 0 then
                vk.log(vk.LOG_NOTICE, "Adding "..i)
            end
        end

        vk.call('B.SET','akeyofmineb',1)
        vk.call('B.SET','zkeyofminea',2)
    end

    result[inc()] = {'B.HEAPBYTES', vk.call('B.HEAPBYTES')}

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

--[[ Testing num hash string key types]]
result[inc()] = {"running test "..tests}
vk.call('B.CLEAR')

result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "60m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "on")
result[inc()] = vk.call("B.CONFIG", "SET","compression", "none")
local cfg = vk.call("B.CONFIG", "SET","iteration_worker_count", 4)
if cfg then
    successes = successes + 1
end
result[inc()] = cfg

convert = tochars123
test()
result[inc()] = {"'B.CONFIG', 'SET','compression', 'zstd'", vk.call('B.CONFIG', 'SET','compression', 'zstd')}
local before = vk.call('B.HEAPBYTES')
--clear()
assert(successes==2, "test failures")
assert(failures==0, "test failures")

return result