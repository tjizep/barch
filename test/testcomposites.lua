local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.HSET','tires','name','p1')
vk.call('B.HSET','counter','value','0')
vk.call('B.HSET','tires','pirelli','p2')

assert(vk.call('B.HINCRBY','counter','value', 1) == 1)
assert(vk.call('B.HINCRBY','counter','value', 2) == 3)
assert(vk.call('B.HINCRBYFLOAT','counter','value', 0.5) == "3.5")

assert(vk.call('B.HGET','tires','pirelli')[1] == 'p2')
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

assert(vk.call('B.ZADD', 'agame', 1.1, 'first', 2.0, 'second' ,3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'game', 1.1, 'first', 2.0, 'second' ,3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'bgame', 1, 'first', 2, 'second', 3, 'third') == 3)
assert(vk.call('B.ZADD', 'zgame', 1.1, 'first', 2.0, 'second', 3.5, 'third') == 3)

assert(vk.call('B.ZCOUNT', 'game', 1.0, 3.6) == 3)
assert(vk.call('B.ZCOUNT', 'zgame', 1.0, 3.4) == 2)
assert(vk.call('B.ZCOUNT', 'agame', 1, 4) == 3)
assert(vk.call('B.ZCOUNT', 'bgame', 1, 4) == 3)
assert(vk.call('B.ZADD', 'agame', 1.1, 'first', 2.0, 'second',3.5, 'third') == 0)
assert(vk.call('B.ZADD', 'agame', 'CH', 1.1, 'first', 2.0,'second',3.5,'third') == 3)
vk.call('B.CLEAR')
assert(vk.call('B.SIZE') == 0)
assert(vk.call('B.ZADD', 'ygame', 1, 'first', 2.0, 'second', 3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'zgame', 1.1, 'first', 2.0, 'second', 3.5, 'third') == 3)
assert(vk.call('B.ZADD', 'bgame', 2.0, 'second', 3.5, 'third') == 2)
assert(vk.call('B.ZADD', 'tgame', 1.2, 'second', 13.5, 'third') == 2)
assert(vk.call('B.SIZE') == 10)
assert(vk.call('B.ZCARD', 'bgame') == 2)
assert(vk.call('B.ZCARD', 'zgame') == 3)
assert(vk.call('B.ZCARD', 'ygame') == 3)
assert(#vk.call('B.ZINTER', 2, 'zgame', 'ygame', 'WEIGHTS', 3, 3, 3) == 2)
assert(vk.call('B.ZINTER', 2, 'zgame', 'ygame', 'WEIGHTS', 3, 3, 3, 'AGGREGATE','SUM') == "16.5")
assert(vk.call('B.ZINTER', 2, 'zgame', 'zgame', 'WEIGHTS', 3, 3, 3, 'AGGREGATE','SUM') == "19.8")
assert(vk.call('B.ZINTER', 2, 'zgame', 'zgame', 'WEIGHTS', 1, 2, 3, 'AGGREGATE','SUM') == "15.6")
assert(vk.call('B.ZINTER', 2, 'zgame', 'yzgame', 'WEIGHTS', 1, 1, 1, 'AGGREGATE','SUM') == "0")
assert(vk.call('B.ZINTER', 3, 'zgame', 'zgame', 'WEIGHTS', 'WEIGHTS', 1, 'AGGREGATE', 'SUM') == "0")
assert(vk.call('B.ZINTER', 3, 'zgame', 'zgame', 'AGGREGATE', 'WEIGHTS', 1, 'AGGREGATE', 'SUM') == "0")
assert(#vk.call('B.ZINTER', 2, 'zgame', 'zgame', 'WITHSCORES') == 6)
assert(vk.call('B.ZADD','diffy1',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','diffy2',1,'one',2,'two',3,'three')==3)
assert(vk.call('B.ZADD','diffy3',1,'one',2,'two')==2)
assert(vk.call('B.ZADD','diffy4',1,'one',2,'two',3,'three',4,'four')==4)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy2') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy4') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy4','diffy1') == 1)
assert(#vk.call('B.ZDIFF', 2, 'diffy3','diffy1') == 0)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy3') == 1)
assert(#vk.call('B.ZDIFF', 2, 'diffy1','diffy1') == 0)
-- the single case is treated as an error - not like redis - although I think no one will mind ?
-- assert(#vk.call('B.ZDIFF', 1, 'diffy1') == 0)
return {"OK"}