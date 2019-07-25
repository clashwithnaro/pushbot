import asyncio
from config import settings


class DiscordLogging:
    def __init__(self):
        self.bot = bot

    def write(self, message):
        # record = message.record
        asyncio.ensure_future(self.send_log(message))

    async def send_log(self, message):
        record = message.record
        await self.bot.get_channel(settings['pushChannels']['pushLog']).send(record)
