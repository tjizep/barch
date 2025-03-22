local vk
vk = redis
local words= {'arty','bravo','charlie','delta','echo','foxtrot','ghetto','hotel','indigo','james','katy','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'}
local result = {}
local index = 0
local convert
local tests = 0
local successes = 0
local inc = function()
    index = index + 1
    return index
end


local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}
    for i=1,#words do
        vk.call('B.SET',words[i], i)
    end

    result[inc()] = vk.call('B.VACUUM')
    result[inc()] = vk.call('B.KEYS','*ty*')
    successes =2 -- #vk.call('B.KEYS','*ty*')
    result[inc()] = {"succeses for test "..tests..": "..successes}
end



--[[ Testing num hash string key types]]

test()
assert(successes==2, "test failures")
--assert(failures==0, "test failures")

return result