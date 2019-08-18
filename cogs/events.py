import asyncio
import asyncpg
import time
import math
import typing
import coc

from datetime import datetime

import discord
from discord.ext import commands, tasks
from cogs.utils.converters import ClanConverter, PlayerConverter
from cogs.utils import formatters, checks
from cogs.utils.db_objects import DatabaseEvent, DatabasePushEvent
from config import emojis


class Events(commands.Cog):
    """Pull information on changes in trophy count for specified clans"""
    def __init__(self, bot):
        self.bot = bot
        self._batch_data = []
        self._batch_lock = asyncio.Lock(loop=bot.loop)
        self.batch_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.batch_insert_loop.start()
        self.report_task.add_exception_type(asyncpg.PostgresConnectionError)
        self.report_task.start()
        self.check_for_timers_task = self.bot.loop.create_task(self.check_for_timers())
        self.bot.coc.add_events(self.on_player_trophies_change)
        self.channel_config_cache = {}

    async def cog_command_error(self, ctx, error):
        self.bot.logger.debug(f"Command Error in {self.__class__.__name__}\n{error}")
        ctx.send(str(error))

    def cog_unload(self):
        self.report_task.cancel()
        self.batch_insert_loop.cancel()
        self.check_for_timers_task.cancel()
        self.bot.coc.remove_events(self.on_player_trophies_change)

    @tasks.loop(seconds=30)
    async def batch_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    async def bulk_insert(self):
        sql = ("INSERT INTO coc_events (player_tag, player_name, clan_tag, clan_name, trophy_change, time_stamp) "
               "SELECT json.player_tag, json.player_name, json.clan_tag, json.clan_name, json.trophy_change, "
               "json.time_stamp FROM jsonb_to_recordset($1::jsonb) "
               "AS json(player_tag TEXT, player_name TEXT, clan_tag TEXT, clan_name TEXT, trophy_change INTEGER, "
               "time_stamp TIMESTAMP)")
        if self._batch_data:
            await self.bot.pool.execute(sql, self._batch_data)
            total = len(self._batch_data)
            if total > 1:
                self.bot.logger.info(f"Registered {total} events to the database.")
            self._batch_data.clear()

    def dispatch_log(self, channel_id, interval, fmt):
        seconds = interval.total_seconds()
        if seconds > 0:
            if seconds < 600:
                self.bot.loop.create_task(self.short_timer(interval.total_seconds(),
                                                           channel_id,
                                                           fmt))
            else:
                asyncio.ensure_future(self.create_new_timer(channel_id,
                                                            fmt,
                                                            datetime.utcnow() + interval))
        else:
            self.bot.logger.info(f"Dispatching a log to channel ID {channel_id}")
            asyncio.ensure_future(self.bot.channel_log(channel_id, fmt, embed=False))

    @tasks.loop(seconds=30)
    async def report_task(self):
        self.bot.logger.info("Starting bulk report loop")
        start = time.perf_counter()
        async with self._batch_lock:
            await self.bulk_report()
        self.bot.logger.info(f"Report loop took {(time.perf_counter() - start) * 1000} ms")

    async def bulk_report(self):
        sql = ("SELECT DISTINCT channel_id FROM events e "
               "INNER JOIN clans c ON e.event_id = c.event_id "
               "INNER JOIN coc_events ce ON c.clan_tag = ce.clan_tag AND ce.reported")
        channel_ids = await self.bot.pool.fetch(sql)
        sql = ("SELECT * FROM coc_events ce "
               "INNER JOIN clans c ON ce.clan_tag = c.clan_tag "
               "INNER JOIN events e ON c.event_id = e.event.id "
               "WHERE e.channel_id = $1 "
               "AND ce.reported = False "
               "ORDER BY e.event_id, ce.time_stamp DESC")
        for channel_id in channel_ids:
            channel_config = await self.bot.get_channel_config(channel_id[0])
            if not channel_config:
                continue
            if not channel_config.log_toggle:
                continue
            events = [DatabaseEvent(bot=self.bot, record=event) for event in
                      await self.bot.pool.fetch(sql, channel_id[0])]
            messages = []
            for event in events:
                clan_name = await self.bot.pushboard.get_clan_name(channel_config.guild_id,
                                                                   event.clan_tag)
                messages.append(formatters.format_event_log_message(event, clan_name))
            group_batch = []
            for i in range(math.ceil(len(messages) / 20)):
                group_batch.append(messages[i*20:(i+1)*20])
            for batch in group_batch:
                interval = channel_config.log_interval - events[0].delta_since
                self.dispatch_log(channel_config.channel_id, interval, "\n".join(batch))
            self.bot.logger.info("Dispatched logs for {} (guild {})".format(channel_config.channel or "Not Found",
                                                                            channel_config.guild or "No guild"))
        # TODO How does trophy change in events translate to currentTrophies in players?
        sql = ("UPDATE events "
               "SET reported = True "
               "WHERE reported = False")
        removed = await self.bot.pool.execute(sql)
        self.bot.logger.info(f"Removed events from the database. Status Code {removed}")

    async def short_timer(self, seconds, channel_id, fmt):
        await asyncio.sleep(seconds)
        await self.bot.channel_log(channel_id, fmt, embed=False)
        self.bot.logger.info(f"Sent a log to channel ID: {channel_id} after sleeping for {seconds} seconds.")

    async def check_for_timers(self):
        try:
            while not self.bot.is_closed():
                sql = "SELECT * FROM log_timers ORDER BY expires LIMIT 1"
                timer = await self.bot.pool.fetchrow(sql)
                if not timer:
                    continue
                now = datetime.utcnow()
                if timer["expires"] >= now:
                    to_sleep = (timer["expires"] - now).total_seconds()
                    await asyncio.sleep(to_sleep)
                await self.bot.channel_log(timer["channel_id"], timer["fmt"], embed=False)
                self.bot.logger.info(f"Sent a log to channel ID: {timer['channel_id']} which "
                                     f"had been saved to the database.")
                sql = "DELETE FROM log_timers WHERE id = $1"
                await self.bot.pool.execute(sql, timer["timer_id"])
        except asyncio.CancelledError:
            raise
        except (OSError, discord.ConnectionClosed, asyncpg.PostgresConnectionError):
            self.create_new_timer_task.cancel()
            self.create_new_timer_task = self.bot.loop.create_task(self.check_for_timers())

    async def create_new_timer(self, channel_id, fmt, expires):
        sql = "INSERT INTO log_timers (channel_id, fmt, expires) VALUES ($1, $2, $3)"
        await self.bot.pool.execute(sql, channel_id, fmt, expires)

    async def on_player_trophies_change(self, old_trophies, new_trophies, player):
        trophy_change = new_trophies - old_trophies
        async with self._batch_lock:
            self._batch_data.append({"player_tag": player.tag,
                                     "player_name": player.name,
                                     "clan_tag": player.clan.tag,
                                     "clan_name": player.clan.name,
                                     "trophy_change": trophy_change,
                                     "time_stamp": datetime.utcnow().isoformat()})

    async def get_channel_config(self,  channel_id):
        config = self.channel_config_cache[channel_id]
        if config:
            return config
        sql = ("SELECT event_id, guild_id, event_name, channel_id, log_interval, log_toggle FROM events "
               "WHERE channel_id = $1")
        fetch = await self.bot.pool.fetchrow(sql, channel_id)
        if not fetch:
            return None
        push_event = DatabasePushEvent(bot=self.bot, record=fetch)
        self.channel_config_cache[channel_id] = push_event
        return push_event

    def invalidate_channel_config(self, channel_id):
        self.channel_config_cache.pop(channel_id, None)

    @commands.group(invoke_without_subcommand=True)
    @checks.manage_guild()
    async def log(self, ctx):
        """Manage the push bot logging for the server"""
        if ctx.invoked_subcommand is None:
            return await ctx.send_help(ctx.command)

    @log.command(name="info")
    async def log_info(self, ctx, *, channel: discord.TextChannel = None):
        """Get information about the log channels for the event tied to this guild"""
        if channel:
            sql = ("SELECT event_id, guild_id, event_name, channel_id, log_interval, log_toggle "
                   "FROM events WHERE channel_id = $1")
            fetch = await ctx.db.fetch(sql, channel.id)
            fmt = channel.mention
        else:
            sql = ("SELECT event_id, guild_id, event_name, channel_id, log_interval, log_toggle "
                   "FROM events WHERE guild_id = $1")
            fetch = await ctx.db.fetch(sql, ctx.guild.id)
            fmt = ctx.guild.name
        e = discord.Embed(color=self.bot.color,
                          description=f"Log info for {fmt}")
        for event in fetch:
            config = DatabasePushEvent(bot=self.bot, record=event)
            fmt = f"Event Name: {config.event_name}\n" \
                  f"Channel: {config.channel.mention if config.channel else 'None'}\n" \
                  f"Log Toggle: {'Enabled' if config.log_toggle else 'Disabled'}\n" \
                  f"Log Interval: {config.interval_seconds} seconds\n"
            e.add_field(name=config.event_name, value=fmt)
            await ctx.send(embed=e)

    @log.command(name="interval")
    async def log_interval(self, ctx, channel: typing.Optional[discord.TextChannel] = None,
                           minutes: int = 1):
        """Update the interval (in minutes) for which the bot will log trophy changes."""
        if not channel:
            channel = ctx.channel
        sql = ("UPDATE events "
               "SET log_interval = ($1 ||' minutes')::interval "
               "WHERE channel_id = $2 "
               "RETURNING event_name")
        fetch = await ctx.db.fetch(sql, str(minutes), channel.id)
        if not fetch:
            return await ctx.send("You haven't created a Push Event yet. You gotta do that first!")
        await ctx.confirm()
        fmt = "\n".join(n[0] for n in fetch)
        self.bot.logger.info(f"Set log interval to {minutes} minutes for {fmt}.")
        await ctx.send(f"Set log interval to {minutes} minutes for {fmt}.")
        self.invalidate_channel_config(channel.id)

    @log.command(name="create")
    async def log_create(self, ctx, channel: typing.Optional[discord.TextChannel] = None):
        """Create log for push events"""
        if not channel:
            channel = ctx.channel
        if not (channel.permissions_for(ctx.me).send_messages or channel.permissions_for(ctx.me).read_messages):
            return await ctx.send("I need permission to read and send messages here!")
        sql = ("UPDATE events "
               "SET channel_id = $1, "
               "log_toggle = True "
               "WHERE guild_id = $2 "
               "RETURNING event_name")
        fetch = await ctx.db.fetch(sql, channel.id, ctx.guild.id)
        if not fetch:
            return await ctx.send("Please add your event using :trophy:add_event")
        event_name = "\n".join(n[0] for n in fetch)
        await ctx.send(F"Log channel has been set to {channel.mention} for {event_name} "
                       F"and logging is enabled.")
        await ctx.confirm()
        self.invalidate_channel_config(channel.id)

    @log.command(name="toggle")
    async def log_toggle(self, ctx, channel: discord.TextChannel = None):
        """Turn logging on off"""
        if not channel:
            channel = ctx.channel
        config = await self.get_channel_config(channel.id)
        if not config:
            return await ctx.send("Please set up a log channel with :trophy: log create.")
        toggle = not config.log_toggle
        sql = ("UPDATE events "
               "SET log_toggle = $1 "
               "WHERE channel_id = $2 "
               "RETURNING event_name")
        fetch = await ctx.db.fetch(sql, toggle, channel.id)
        if not fetch:
            return await ctx.send("Please add your event using :trophy:add_event")
        event_name = "\n".join(n[0] for n in fetch)
        await ctx.send(F"Logging has been {'enabled' if toggle else 'disabled'} for {event_name}")
        await ctx.confirm()
        self.invalidate_channel_config(channel.id)

    @commands.group(invoke_without_command=True)
    async def recent(self, ctx, limit: typing.Optional[int] = 20, *,
                     arg: typing.Union[ClanConverter, PlayerConverter] = None):
        """Check on recent trophy changes. Defaults to 20 recent events."""
        if ctx.invoked_subcommand is not None:
            return
        if not arg:
            arg = limit
        if isinstance(arg, int):
            await ctx.invoke(self.recent_all, limit=arg)
        elif isinstance(arg, coc.BasicPlayer):
            await ctx.invoke(self.recent_player, player=arg, limit=limit)
        elif isinstance(arg, coc.Clan):
            await ctx.inoke(self.recent_clan, clan=arg, limit=limit)
        else:
            await ctx.send("That's not going to work for me. Please try again with a valid player or clan.")

    @recent.command(name="recent_all", hidden=True)
    async def recent_all(self, ctx, limit: int = None):
        sql = ("SELECT player_name, clan_name, trophy_change, time_stamp "
               "FROM coc_events"
               "WHERE clan_tag IN "
               "(SELECT clan_tag FROM clans WHERE event_id IN"
               "(SELECT event_id from events WHERE guild_id = $1)) "
               "ORDER BY time_stamp DESC"
               "LIMIT $2")
        fetch = await ctx.db.fetch(sql, ctx.guild.id, limit)
        if not fetch:
            return await ctx.send("No trophy changes found. Please ensure you have enabled "
                                  "logging and have set up your event for this Discord server.")
        num_pages = math.ceil(len(fetch) / 20)
        title = "Recent Trophy Changes"
        p = formatters.EventsPaginator(ctx, data=fetch, page_count=num_pages, title=title)
        await p.paginate()

    @recent.command(name="player", hidden=True)
    async def recent_player(self, ctx, limit: typing.Optional[int] = 20, *,
                            player: PlayerConverter):
        sql = ("SELECT player_name, clan_name, trophy_change, time_stamp  "
               "FROM coc_events "
               "WHERE player_tag = $1 "
               "ORDER BY time_stamp DESC"
               "LIMIT $2")
        fetch = await ctx.db.fetch(sql, player.tag, limit)
        if not fetch:
            return await ctx.send(f"{player.name} ({player.clan}) is not in the event. "
                                  f"Use :trophy:add_player to add them.")
        title = f"Recent Trophy Changes for {player.name}"
        num_pages = math.ceil(len(fetch) / 20)
        p = formatters.EventsPaginator(ctx, data=fetch, title=title, page_count=num_pages)
        await p.paginate()

    @recent.commands(name="clan", hidden=True)
    async def recent_clan(self, ctx, limit: typing.Optional[int] = 20, *, clans: ClanConverter):
        sql = ("SELECT player_name, clan_name, trophy_change, time_stamp "
               "FROM coc_events "
               "WHERE clan_tag = ANY($1::TEXT[]) "
               "ORDER BY time_stamp DESC "
               "LIMIT $2")
        fetch = ctx.db.fetch(sql, list(set(n.tag for n in clans)), limit)
        if not fetch:
            return await ctx.send("No events found for the clan(s) provided.")
        title = f"Recent Trophy Changes for {', '.join(n.name for n in clans)}"
        num_pages = math.ceil(len(fetch) / 20)
        p = formatters.EventsPaginator(ctx, data=fetch, title=title, page_count=num_pages)
        await p.paginate()


def setup(bot):
    bot.add_cog(Events(bot))