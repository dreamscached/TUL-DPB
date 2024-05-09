import redis.asyncio as redis
import asyncio

class ToDoList(object):
    KEY_TASKS_PENDING = "tasks:todo"
    KEY_TASKS_DONE = "tasks:done"

    def __init__(self, pool):
        self.conn = redis.Redis(connection_pool=pool)

    async def get_tasks_count(self, include_done=False):
        future = list()

        future.append(self.conn.llen(self.KEY_TASKS_PENDING))
        if include_done:
            future.append(self.conn.llen(self.KEY_TASKS_DONE))
        
        ret = await asyncio.gather(*future)
        return ret[0] if len(ret) == 1 else sum(ret)

    async def get_tasks_list(self, include_done=False):
        future = list()

        future.append(self.conn.lrange(self.KEY_TASKS_DONE, 0, -1))
        if include_done:
            future.append(self.conn.lrange(self.KEY_TASKS_PENDING, 0, -1))
        
        ret = await asyncio.gather(*future)
        ret = list(map(lambda it: list(map(bytes.decode, it)), ret))
        if not include_done:
            return ret[0]
        else:
            return tuple(ret)

    async def add_task(self, task, top=False):
        lpush_or_rpush = self.conn.lpush if top else self.conn.rpush
        await lpush_or_rpush(self.KEY_TASKS_PENDING, task)

    async def set_task_done(self, task):
        async with self.conn.pipeline(transaction=True) as pipe:
            await self.conn.lrem(self.KEY_TASKS_PENDING, 0, task)
            await self.conn.rpush(self.KEY_TASKS_DONE, task)

    async def close(self):
        await self.conn.aclose()

async def main():
    pool = redis.ConnectionPool.from_url("redis://localhost:6379")
    todo = ToDoList(pool)

    await todo.add_task("foo")
    print(await todo.get_tasks_list())

    await todo.close()


asyncio.run(main())