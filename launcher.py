import asyncio
import git
import os
from cogs.utils.db import PushDB
from pushbot import PushBot


if __name__ == '__main__':
    bot = PushBot()
    bot.repo = git.Repo(os.getcwd())
    loop = asyncio.get_event_loop()
    bot.db = PushDB(bot)
    pool = loop.run_until_complete(bot.db.create_pool())
    bot.pool = pool
    bot.run()
