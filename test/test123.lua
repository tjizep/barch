local vk
vk = redis

local count = 100000
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
    vk.call('B.BEGIN')
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('B.SET',k,v)
        vk.call('B.REM',k)
        vk.call('B.SET',k,v)
        if math.mod(i,logperiod) == 0 then
            --vk.log(vk.LOG_NOTICE, "Adding "..i)
        end
    end
    vk.call('B.COMMIT')
    vk.call('B.BEGIN')
    for i = 1, count do

        local k = convert(i-1)
        local v = '#'..i
        local actual = vk.call('B.GET',k)
        if actual ~= v then
            result[inc()] = {"could not find",k, v, actual}
        else
            successes = successes + 1
        end
        if math.mod(i,logperiod) == 0 then
            --vk.log(vk.LOG_NOTICE, "Checking "..i.." "..failures)
        end

    end
    vk.call('B.ROLLBACK')
    result[inc()] = {"'B.RANGE',convert(2), convert(count-2), 10", vk.call('B.RANGE',convert(2), convert(count-2), 4)}
    result[inc()] = {[['B.MIN']], vk.call('B.MIN')}
    result[inc()] = {[['B.MAX']], vk.call('B.MAX')}
    result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
    result[inc()] = {[['B.SIZE']], vk.call('B.SIZE')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end

local clear = function()
    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i

        local before = vk.call('B.SIZE')
        if vk.call('B.GET',k) ~= v then
            result[inc()] = {"Failed get before remove",k, v, vk.call('B.GET',k)}
            failures = failures + 1
        end
        if vk.call('B.REM',k) == v then
            successes = successes + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('B.GET',k)}
        end

        if vk.call('B.GET',k) then
            result[inc()] = {"Failed remove",k, v, vk.call('B.GET',k)}
            failures = failures + 1
        end
        if vk.call('B.SIZE') == before - 1 then
            successes = successes + 1
        else
            failures = failures + 1
        end
        if math.mod(i,logperiod) == 0 then
            --vk.log(vk.LOG_NOTICE, "Removed "..i.." "..failures)
        end

    end

    result[inc()] = {[['B.STATS']], vk.call('B.STATS')}
    result[inc()] = {[['B.SIZE']], vk.call('B.SIZE')}
    result[inc()] = {[['B.OPS']], vk.call('B.OPS')}
    result[inc()] = {'COUNT', count}
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}
    vk.call('B.CLEAR')

end

--[[ Testing num hash string key types]]
result[inc()] = {"running test "..tests}
vk.call('B.CLEAR')
result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "450m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "on")
--result[inc()] = vk.call("B.CONFIG", "SET","compression", "off")
result[inc()] = vk.call("B.CONFIG", "SET","save_interval", "100")
result[inc()] = vk.call("B.CONFIG", "SET","max_modifications_before_save", "1000")

convert = tochars123
for i = 1,2 do
    test()
    clear()
end
--assert(successes==9*count, "test failures")
--assert(failures==0, "test failures")

return result