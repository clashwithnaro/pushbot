import asyncio
import git
import os
import coc
from cogs.utils.db import PushDB
from pushbot import PushBot
from config import settings


if __name__ == '__main__':
    bot = PushBot()
    bot.remove_command("help")
    bot.repo = git.Repo(os.getcwd())
    loop = asyncio.get_event_loop()
    bot.db = PushDB(bot)
    pool = loop.run_until_complete(bot.db.create_pool())
    bot.pool = pool
    bot.coc_client = coc.login(settings['supercell']['user'],
                               settings['supercell']['pass'],
                               key_names=bot.coc_names)
    bot.run()
