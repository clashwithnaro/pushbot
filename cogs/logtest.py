import asyncio
from discord.ext import commands
from loguru import logger
from config import settings


class LogTest(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        logger.add(self.send_log)

    @commands.command(name="log_test")
    async def log_test(self, ctx):
        logger.info("Logging is working")

    def send_log(self, message):
        print("inside send_log")
        asyncio.ensure_future(self.send_message(message))

    async def send_message(self, message):
        print("inside send_message")
        record = message.record
        await self.bot.get_channel(settings['logChannels']['push']).send(record)


def setup(bot):
    bot.add_cog(LogTest(bot))
