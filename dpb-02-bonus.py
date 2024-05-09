import redis.asyncio as redis
import itertools
import random
import string
import asyncio

class Leaderboard(object):
    SET_PLAYERS = "leaderboard:players"

    def __init__(self, pool):
        self.conn = redis.Redis(connection_pool=pool)

    async def add_player(self, name, score):
        await self.conn.zadd(self.SET_PLAYERS, {name: score})
    
    async def get_top_three(self):
        return list(map(
            lambda it: (it[0].decode(), it[1]), 
            await self.conn.zrange(self.SET_PLAYERS, 
                999, 0,
                desc=True, 
                withscores=True, 
                byscore=True, 
                offset=0,
                num=3
            )
        ))
    
    async def get_lowest(self):
        ret = list(map(
            lambda it: (it[0].decode(), it[1]), 
            await self.conn.zrange(self.SET_PLAYERS, 
                0, 999,
                withscores=True, 
                byscore=True, 
                offset=0,
                num=1
            )
        ))

        if not ret:
            return None
        return ret[0]

    async def get_lt_100(self):
        return list(map(
            lambda it: (it[0], it[1]), 
            await self.conn.zrange(self.SET_PLAYERS, 
                0, 100,
                withscores=True, 
                byscore=True
            )
        ))

    async def get_gt_850(self):
        return list(map(
            lambda it: (it[0], it[1]), 
            await self.conn.zrange(self.SET_PLAYERS, 
                850, 999,
                withscores=True, 
                byscore=True
            )
        ))

    async def get_pos(self, name):
        ret = list(map(bytes.decode, await self.conn.zrange(self.SET_PLAYERS, 
            999, 0,
            desc=True,
            byscore=True
        )))

        try:
            return ret.index(name) + 1
        except ValueError:
            return None

    async def increment(self, name, by=1):
        await self.conn.zincrby(self.SET_PLAYERS, by, name)

    async def close(self):
        await self.conn.aclose()


async def main():
    pool = redis.ConnectionPool.from_url("redis://localhost:6379")
    board = Leaderboard(pool)

    await board.add_player("Alfred", 888)
    name_gen = map(lambda it: "".join(it), itertools.permutations(string.ascii_letters, 3))
    for _ in range(10):
        await board.add_player(next(name_gen), random.randint(0, 999))

    print(await board.get_top_three())
    print(await board.get_lowest())
    print(await board.get_lt_100())
    print(await board.get_gt_850())

    print(await board.get_pos("Alfred"))
    await board.increment("Alfred", 12)
    print(await board.get_pos("Alfred"))

    await board.close()


asyncio.run(main())
