import asyncio
import asyncpg
import time
import math

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
        sql = ("INSERT INTO coc_events (player_tag, clan_tag, trophy_change, time_stamp) "
               "SELECT json.player_tag, json.clan_tag, json.trophy_change, json.time_stamp "
               "FROM jsonb_to_recordset($1::jsonb) "
               "AS json(player_tag TEXT, clan_tag TEXT, trophy_change INTEGER, time_stamp TIMESTAMP)")
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
            self._batch_data.append({"player_tag": player.tag[1:],
                                     "clan_tag": player.clan.tag[1:],
                                     "trophy_change": trophy_change,
                                     "time_stamp": datetime.utcnow().isoformat()})

    async def get_channel_config(self,  channel_id):
        config = self.channel_config_cache[channel_id]
        if config:
            return config
        sql = ("SELECT event_id,  FROM events "
               "WHERE channel_id = $1")
        fetch = await self.bot.pool.fetchrow(sql, channel_id)
        if not fetch:
            return None
        # TODO I don't really want a clan here ,but an event
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
                  f""