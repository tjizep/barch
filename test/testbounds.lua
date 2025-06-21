local vk
vk = redis
vk.call('B.CLEAR')

local chars= {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'}
local count = 17
local result = {}
local index = 0
local convert
local tests = 0
local successes = 0
local inc = function()
    index = index + 1
    return index
end

local tochars123 = function(num)
    return chars[num+1]
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}

    for i = 1, count do
        local k = convert(i-1)
        local v = '#'..i
        vk.call('B.SET',k,v)

    end
    local rtest = vk.call('B.RANGE',convert(2), convert(8), 6)
    if #rtest == 6 then
        successes = successes + 1
    end
    if vk.call('B.MIN') == 'a' then
        successes = successes + 1
    end
    if vk.call('B.MAX') == chars[count] then
        successes = successes + 1
    end
    result[inc()] = {"'B.RANGE',convert(2), convert(8), 6", rtest}
    result[inc()] = {[['B.MIN']], vk.call('B.MIN')}

    result[inc()] = {[['B.MAX']], vk.call('B.MAX')}
    result[inc()] = {"succeses for test "..tests..": "..successes}
end



--[[ Testing num hash string key types]]

convert = tochars123
test()
assert(successes==3, "test failures")
--assert(failures==0, "test failures")

return result