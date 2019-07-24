import asyncpg
from config import settings


class PushDB:
    def __init__(self, bot):
        self.bot = bot

    @staticmethod
    async def create_pool():
        pool = await asyncpg.create_pool(f"{settings['pg']['uri']}/pushbot", max_size=85)
        return pool
