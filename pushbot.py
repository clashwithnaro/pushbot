import aiohttp
import asyncio
import traceback
import sys
import discord
import coc
import git
import os

from cogs.utils import context
from cogs.utils.db import PushDB
from discord.ext import commands
from loguru import logger
from config import settings, emojis
from datetime import datetime

enviro = "dev"

if enviro == "LIVE":
    token = settings['discord']['pushToken']
    prefix = ":trophy:"
    log_level = "INFO"
    coc_names = "vps"
elif enviro == "work":
    token = settings['discord']['testToken']
    prefix = ">"
    log_level = "DEBUG"
    coc_names = "work"
else:
    token = settings['discord']['testToken']
    prefix = ">"
    log_level = "DEBUG"
    coc_names = "dev"

description = """Discord bot used to track Clash of Clans Trophy Push Events - by TubaKid/wpmjones"""

initial_extensions = ["cogs.admin",
                      "cogs.newhelp",
                      "cogs.events",
                      ]

coc_client = coc.login(settings['supercell']['user'],
                       settings['supercell']['pass'],
                       client=coc.EventsClient,
                       key_names=coc_names,
                       throttle_limit=40)


class PushBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=prefix,
                         description=description,
                         case_insensitive=True,
                         fetch_offline_members=True)
        self.remove_command("help")
        self.token = settings['discord']['testToken']
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.coc = coc_client
        self.color = discord.Color.purple()

        coc_client.add_events(self.on_event_error)

        for extension in initial_extensions:
            try:
                self.load_extension(extension)
            except Exception as e:
                logger.error(f'Failed to load extension {extension}.', file=sys.stderr)
                traceback.print_exc()

    @property
    def push_board(self):
        return self.get_cog("PushBoard")

    @property
    def events(self):
        return self.get_cog("Events")

    @property
    def event_config(self):
        return self.get_cog("EventConfig")

    @property
    def config(self):
        return __import__("config")

    @property
    def log_channel(self):
        return self.get_channel(settings['logChannels']['push'])

    def send_log(self, message):
        asyncio.ensure_future(self.send_message(message))

    async def send_message(self, message):
        await self.log_channel.send(f"`{message}`")

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

    async def on_event_error(self, event_name, *args, **kwargs):
        e = discord.Embed(title='COC Event Error', colour=0xa32952)
        e.add_field(name='Event', value=event_name)
        e.description = f'```py\n{traceback.format_exc()}\n```'
        e.timestamp = datetime.utcnow()

        args_str = ['```py']
        for index, arg in enumerate(args):
            args_str.append(f'[{index}]: {arg!r}')
        args_str.append('```')
        e.add_field(name='Args', value='\n'.join(args_str), inline=False)

        try:
            self.log_channel.send(embed=e)
        except:
            pass

    async def on_error(self, event_method, *args, **kwargs):
        e = discord.Embed(title='Discord Event Error', colour=0xa32952)
        e.add_field(name='Event', value=event_method)
        e.description = f'```py\n{traceback.format_exc()}\n```'
        e.timestamp = datetime.utcnow()

        args_str = ['```py']
        for index, arg in enumerate(args):
            args_str.append(f'[{index}]: {arg!r}')
        args_str.append('```')
        e.add_field(name='Args', value='\n'.join(args_str), inline=False)

        try:
            await self.log_channel.send(embed=e)
        except:
            pass

    async def on_ready(self):
        logger.add(self.send_log, level="DEBUG")
        if not hasattr(self, 'uptime'):
            self.uptime = datetime.utcnow()
        cog = self.get_cog("PushBoard")
        await cog.update_clan_tags()
        activity = discord.Activity(type=discord.ActivityType.watching,
                                    name="trophies pile up")
        await self.change_presence(activity=activity)
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

    async def log_info(self, guild_id, message, color=None, prompt=False):
        guild_config = await self.get_guild_config(guild_id)
        if not guild_config.log_channel or not guild_config.log_toggle:
            return
        e = discord.Embed(color=color or self.color,
                          description=message,
                          timestamp=datetime.utcnow())
        try:
            msg = await guild_config.log_channel.send(embed=e)
        except (discord.Forbidden, discord.HTTPException):
            return
        if prompt:
            for n in (emojis["push"]["yes"], emojis["push"]["no"]):
                try:
                    await msg.add_reaction(n)
                except (discord.Forbidden, discord.HTTPException):
                    return msg.id
        return msg.id

    async def channel_log(self, channel_id, message, color=None, embed=True):
        channel_config = await self.events.get_channel_config(channel_id)
        if not channel_config.channel or not channel_config.log_toggle:
            return
        if embed:
            e = discord.Embed(color=color or self.color,
                              description=message,
                              timestamp=datetime.utcnow())
            c = None
        else:
            e = None
            c = message
        try:
            await channel_config.channel.send(content=c, embed=e)
        except (discord.Forbidden, discord.HTTPException):
            return

    async def get_guild(self, clan_tag):
        sql = "SELECT guild_id FROM claims WHERE clan_tag = $1"
        fetch = await self.pool.fetch(sql, clan_tag)
        return [self.get_guild(n[0]) for n in fetch if self.get_guild(n[0])]

    async def get_clan(self, guild_id):
        sql = "SELECT clan_tag FROM claims WHERE guild_id = $1"
        fetch = await self.pool.fetch(sql, guild_id)
        return await self.coc.get_clans(n[0].strip() for n in fetch).flatten()

    async def get_channel_config(self, channel_id):
        cog = self.events
        if not cog:
            self.load_extension("cogs.events")
            cog = self.get_cog("Events")
        return await cog.get_channel_config(channel_id)

    async def get_guild_config(self, guild_id):
        cog = self.push_board
        if not cog:
            self.load_extension("cogs.pushboard")
            cog = self.get_cog("PushBoard")
        return await cog.get_guild_config(guild_id)

    def invalidate_guild_cache(self, guild_id):
        cog = self.push_board
        if not cog:
            self.load_extension("cogs.pushboard")
            cog = self.get_cog("PushBoard")
        cog._guild_coinfig_cache[guild_id] = None


if __name__ == '__main__':
    try:
        bot = PushBot()
        bot.repo = git.Repo(os.getcwd())
        loop = asyncio.get_event_loop()
        bot.db = PushDB(bot)
        pool = loop.run_until_complete(bot.db.create_pool())
        bot.pool = pool
        bot.logger = logger
        bot.run(token, reconnect=True)
    except:
        traceback.print_exc()
