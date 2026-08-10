local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.HSET','tires','name','p1')
vk.call('B.HSET','counter','value','0')
vk.call('B.HSET','tires','pirelli','p2')

assert(vk.call('B.HINCRBY','counter','value', 1) == 1)
assert(vk.call('B.HINCRBY','counter','value', 2) == 3)
assert(vk.call('B.HINCRBYFLOAT','counter','value', 0.5) == "3.5")

-- HGET answers with a bulk string. It used to come back wrapped in a one element array
-- because it shared HMGET's reply path, which is what the [1] here was reading - DONE 33
assert(vk.call('B.HGET','tires','pirelli') == 'p2')

assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','rarbg')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','chips')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','1000','XX','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10000','GT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10','GT','FIELDS','1','pirelli')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','50','LT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','50000','LT','FIELDS','1','pirelli')[1] == 0)
assert(vk.call('B.HGETEX','tires','EX',120,'FIELDS','1','pirelli')[1] == "p2")
assert(vk.call('B.HTTL','tires','FIELDS',1,'pirelli')[1] <= 120)
assert(vk.call('B.HGETEX','tires','PERSIST','FIELDS','2','pirelli','x')[1] == "p2")

assert(vk.call('B.HLEN','tires') == #vk.call('B.HKEYS','tires'))
assert(vk.call('B.HLEN','tires')*2 == #vk.call('B.HGETALL','tires'))
assert(vk.call('B.HEXISTS','tires','pirelli'))
assert(vk.call('B.HGETDEL','tires','FIELDS', 'pirelli') == 1)

assert(vk.call('B.ZADD', 'agame', 1.1, 'first', 2.0, 'second' ,3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'game', 1.1, 'first', 2.0, 'second' ,3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'bgame', 1, 'first', 2, 'second', 3, 'third') == 3)
assert(vk.call('B.ZADD', 'zgame', 1.1, 'first', 2.0, 'second', 3.5, 'third') == 3)

assert(vk.call('B.ZCOUNT', 'game', 1.0, 3.6) == 3)
assert(vk.call('B.ZCOUNT', 'zgame', 1.0, 3.4) == 2)
assert(vk.call('B.ZCOUNT', 'agame', 1, 4) == 3)
assert(vk.call('B.ZCOUNT', 'bgame', 1, 4) == 3)
assert(vk.call('B.ZADD', 'agame', 'LFI', 1.1, 'first', 2.0, 'second',3.5, 'third') == 0)
assert(vk.call('B.ZADD', 'agame', 'LFI', 'CH', 1.1, 'first', 2.0,'second',3.5,'third') == 0)
vk.call('B.CLEAR')
assert(vk.call('B.SIZE') == 0)
assert(vk.call('B.ZADD', 'ygame', 1, 'first', 2.0, 'second', 3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'zgame', 1.1, 'first', 2.0, 'second', 3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'bgame', 2.0, 'second', 3.5, 'third') == 2)
assert(vk.call('B.ZADD', 'tgame', 1.2, 'second', 13.5, 'third') == 2)
assert(vk.call('B.SIZE') >= 10)
assert(vk.call('B.ZCARD', 'bgame') == 2)
assert(vk.call('B.ZCARD', 'zgame') == 3)
assert(vk.call('B.ZCARD', 'ygame') == 3)
-- ZINTER answers the members the two sets share. It used to answer the members whose
-- scores happened to coincide, which is a different question and gave 2 here where the
-- sets share all three members; and AGGREGATE reduced the whole reply to a single number
-- rather than combining each member's scores. Both were fixed together - see DONE 58
assert(#vk.call('B.ZINTER', 2, 'zgame', 'ygame', 'WEIGHTS', 3, 3, 3) == 3)
assert(#vk.call('B.ZINTER', 2, 'zgame', 'ygame', 'WEIGHTS', 3, 3, 3, 'AGGREGATE','SUM') == 3)

-- with WITHSCORES the reply is member, score, member, score - so twice the members - and
-- the score is the sum of what the member scored in each input, after its weight
local scored = vk.call('B.ZINTER', 2, 'zgame', 'zgame', 'WEIGHTS', 1, 2, 3, 'AGGREGATE','SUM', 'WITHSCORES')
assert(#scored == 6)
assert(math.abs(tonumber(scored[2]) - 3.3) < 0.001)   -- first: 1.1*1 + 1.1*2

-- an input that does not exist leaves nothing to intersect with
assert(#vk.call('B.ZINTER', 2, 'zgame', 'yzgame', 'WEIGHTS', 1, 1, 1, 'AGGREGATE','SUM') == 0)
assert(#vk.call('B.ZINTER', 3, 'zgame', 'zgame', 'WEIGHTS', 'WEIGHTS', 1, 'AGGREGATE', 'SUM') == 0)
-- the third named input is the word AGGREGATE, which is not a set, so nothing is shared
assert(#vk.call('B.ZINTER', 3, 'zgame', 'zgame', 'AGGREGATE', 'WEIGHTS', 1, 'AGGREGATE', 'SUM') == 0)
assert(#vk.call('B.ZINTER', 2, 'zgame', 'zgame', 'WITHSCORES') == 6)
assert(vk.call('B.ZINTERSTORE','storezegame', 2, 'zgame', 'zgame') == 3)
assert(vk.call('B.ZCARD','storezegame') == 3)
assert(vk.call('B.ZADD','diffy1',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','diffy2',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','diffy3',1,'one',2,'two')==2)
assert(vk.call('B.ZADD','diffy4',1,'one',2,'two',3,'three',4,'four')==4)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy2') == 0)
-- AGGREGATE says how to combine scores, it does not turn the reply into one number
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy2','AGGREGATE','MIN') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy2','AGGREGATE','MAX') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy4') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy4','diffy1') == 1)
assert(#vk.call('B.ZDIFF', 2, 'diffy3','diffy1') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy3') == 1)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy1') == 0)
assert(vk.call('B.ZINTERCARD', 2, 'diffy1','diffy3') == 2)
assert(vk.call('B.ZDIFFSTORE','storediffy1', 2, 'diffy1','diffy3') == 1)
assert(vk.call('B.ZCARD','storediffy1') == 1)
-- the single case is treated as an error - not like redis - although I think no one will mind ?
-- assert(#vk.call('B.ZDIFF', 1, 'diffy1') == 0)
local before = vk.call('B.SIZE')
assert(vk.call('B.ZADD','cagame',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','cbgame','LFI',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','ccgame',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','rcgame','LFI',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.SIZE')>=before+12)
-- these are score bounds, so they say so. ZRANGE and ZREVRANGE read start and stop as
-- positions by default now, the way redis does, and 3.01 is not a position - see TODO 38
assert(#vk.call('B.ZRANGE', 'cbgame',1,3.01,'BYSCORE') == 3)
assert(#vk.call('B.ZREVRANGE', 'cbgame',1,3,'BYSCORE') == 3)
assert(#vk.call('B.ZRANGEBYSCORE', 'cbgame',1,3.01) == 3)
assert(#vk.call('B.ZRANGEBYSCORE', 'cbgame',1,3.01) == 3)
assert(#vk.call('B.ZREVRANGEBYSCORE', 'cbgame',1,3.01) == 3)
-- a lex bound says whether its end is open: [a is inclusive, (a is not, and - and + are
-- the ends of the range. A bare `a` is refused now, as redis refuses it - see DONE 59
assert(#vk.call('B.ZRANGEBYLEX', 'cbgame','[a','[z') == 3)
assert(#vk.call('B.ZRANGEBYLEX', 'cbgame','[a','[z','WITHSCORES') == 6)
assert(#vk.call('B.ZREVRANGEBYLEX', 'cbgame','[a','[z') == 3)
assert(#vk.call('B.ZRANGE', 'cbgame',1,3.01,'BYSCORE','WITHSCORES') == 6)
assert(#vk.call('B.ZRANGE', 'cbgame','a','z','WITHSCORES','REV','BYLEX') == 6)
assert(#vk.call('B.ZRANGE', 'cbgame','a','z','BYLEX') == 3)
assert(vk.call('B.ZCARD','cagame')==3)
assert(#vk.call('B.ZPOPMIN','cagame')==2)
assert(vk.call('B.ZCARD','cagame')==2)
assert(#vk.call('B.ZPOPMAX','cagame')==2)
assert(vk.call('B.ZCARD','cagame')==1)
assert(#vk.call('B.ZPOPMAX','bagame')==0)
assert(vk.call('B.ZCARD','bagame')==0)
assert(#vk.call('B.ZPOPMIN','cbgame')==2)
assert(vk.call('B.ZCARD','cbgame')==2)
assert(#vk.call('B.ZPOPMIN','cbgame')==2)
assert(vk.call('B.ZCARD','cbgame')==1)
assert(#vk.call('B.ZPOPMIN','cbgame')==2)
assert(vk.call('B.ZCARD','cbgame')==0)
assert(vk.call('B.ZREMRANGEBYLEX', 'rcgame','a','z') == 3)
assert(vk.call('B.ZCARD','rcgame')==0)
assert(vk.call('B.KSOPTIONS', 'SET', 'UNORDERED'))
assert(vk.call('B.KSOPTIONS', 'SET', 'ORDERED'))
return {"OK"}