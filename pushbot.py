import aiohttp
import asyncio
import traceback
import sys
import discord
import coc

from cogs.utils import context
from discord.ext import commands
from loguru import logger
from config import settings
from datetime import datetime

enviro = "dev"

if enviro == "LIVE":
    token = settings['discord']['pushToken']
    prefix = "/"
    log_level = "INFO"
    coc_names = "vps"
else:
    token = settings['discord']['testToken']
    prefix = ">"
    log_level = "DEBUG"
    coc_names = "dev"

description = """Discord bot used to track Clash of Clans Trophy Push Events - by TubaKid/wpmjones"""

initial_extensions = ["cogs.push",
                      "cogs.admin",
                      ]

coc_client = coc.login(settings['supercell']['user'],
                       settings['supercell']['pass'],
                       client=coc.EventsClient,
                       key_names=coc_names)


class PushBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=prefix, description=description,
                         pm_help=None, help_attrs=dict(hidden=True), fetch_offline_members=False)

        self.token = settings['discord']['testToken']
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.coc = coc_client
        self.logger = logger
        self.logger.add(self.send_log)

        for extension in initial_extensions:
            try:
                self.load_extension(extension)
            except Exception as e:
                logger.error(f'Failed to load extension {extension}.', file=sys.stderr)
                traceback.print_exc()

    def send_log(self, message):
        asyncio.ensure_future(self.send_message(message))

    async def send_message(self, message):
        await self.get_channel(settings['logChannels']['push']).send(f"`{message}`")

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.NoPrivateMessage):
            await ctx.author.send('This command cannot be used in private messages.')
        elif isinstance(error, commands.DisabledCommand):
            await ctx.author.send('Sorry. This command is disabled and cannot be used.')
        elif isinstance(error, commands.CommandInvokeError):
            original = error.original
            if not isinstance(original, discord.HTTPException):
                logger.error(f'In {ctx.command.qualified_name}:', file=sys.stderr)
                traceback.print_tb(original.__traceback__)
                logger.error(f'{original.__class__.__name__}: {original}', file=sys.stderr)
        elif isinstance(error, commands.ArgumentParsingError):
            await ctx.send(error)

    async def on_ready(self):
        if not hasattr(self, 'uptime'):
            self.uptime = datetime.utcnow()

        logger.info(f'Ready: {self.user} (ID: {self.user.id})')

    async def on_resumed(self):
        logger.info('resumed...')

    async def process_commands(self, message):
        ctx = await self.get_context(message, cls=context.Context)

        if ctx.command is None:
            return

        try:
            await self.invoke(ctx)
        finally:
            # Just in case we have any outstanding DB connections
            await ctx.release()

    async def on_message(self, message):
        if message.author.bot:
            return
        await self.process_commands(message)

    async def close(self):
        await super().close()
        await self.session.close()
        await self.coc.close()

    def run(self):
        try:
            super().run(settings['discord']['testToken'], reconnect=True)
        except:
            traceback.print_exc()

    @property
    def config(self):
        return __import__('config')
