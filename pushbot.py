import aiohttp
import traceback
import sys
import discord
import coc

from cogs.utils import context
# from cogs.utils.log import DiscordLogging
from discord.ext import commands
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

description = """Discrd bot used to track Clash of Clans Trophy Push Events - by TubaKid/wpmjones"""

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

        for extension in initial_extensions:
            try:
                self.load_extension(extension)
            except Exception as e:
                print(f'Failed to load extension {extension}.', file=sys.stderr)
                traceback.print_exc()

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.NoPrivateMessage):
            await ctx.author.send('This command cannot be used in private messages.')
        elif isinstance(error, commands.DisabledCommand):
            await ctx.author.send('Sorry. This command is disabled and cannot be used.')
        elif isinstance(error, commands.CommandInvokeError):
            original = error.original
            if not isinstance(original, discord.HTTPException):
                print(f'In {ctx.command.qualified_name}:', file=sys.stderr)
                traceback.print_tb(original.__traceback__)
                print(f'{original.__class__.__name__}: {original}', file=sys.stderr)
        elif isinstance(error, commands.ArgumentParsingError):
            await ctx.send(error)

    async def on_ready(self):
        if not hasattr(self, 'uptime'):
            self.uptime = datetime.utcnow()

        print(f'Ready: {self.user} (ID: {self.user.id})')

    async def on_resumed(self):
        print('resumed...')

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

    def run(self):
        try:
            super().run(settings['discord']['testToken'], reconnect=True)
        except:
            traceback.print_exc()

    @property
    def config(self):
        return __import__('config')