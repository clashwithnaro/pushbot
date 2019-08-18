import asyncio
import asyncpg
import coc
import discord
import math
from datetime import datetime
from discord.ext import commands, tasks
from cogs.utils.db_objects import DatabaseGuild, DatabaseMessage, DatabasePlayer
from cogs.utils.formatters import CLYTable
from cogs.utils import checks, cache


class MockPlayer:
    def __init__(self):
        MockPlayer.name = "Unknown"
        MockPlayer.clan = "Unknown"


class PushBoard(commands.Cog):
    """Contains all PushBoard Configurations"""
    def __init__(self, bot):
        self.bot = bot
        self.clan_updates = []
        self.player_updates = []
        self._to_be_deleted = set()
        self._join_prompts = {}
        self.bot.coc.add_events(self.on_player_trophies_change)
        self.bot.coc._clan_retry_interval = 60
        self.bot.coc.start_updates("player")

        self._batch_lock = asyncio.Lock(loop=bot.loop)
        self._data_batch = []
        self._clan_events = set()
        self.bulk_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert_loop.start()
        self.update_pushboard_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.update_pushboard_loop.add_exception_type(coc.ClashOfClansException)
        self.update_pushboard_loop.start()

    def cog_unload(self):
        self.bulk_insert_loop.cancel()
        self.update_pushboard_loop.cancel()
        self.bot.coc.remove_events(self.on_player_trophies_change)

    @tasks.loop(seconds=60.0)
    async def bulk_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    @tasks.loop(seconds=60.0)
    async def update_pushboard_loop(self):
        async with self._batch_lock:
            clan_tags = list(self._clan_events)
            self._clan_events.clear()

        sql = ("SELECT DISTINCT guild_id FROM events e "
               "INNER JOIN clans c ON e.event_id = c.event_id "
               "WHERE clan_tag = ANY($1::TEXT[])")
        fetch = await self.bot.pool.fetch(sql, clan_tags)
        for n in fetch:
            await self.update_pushboard(n["guild_id"])

    async def bulk_insert(self):
        sql = ("UPDATE players p "
               "SET p.current_trophies = p.current_trophies + json.trophy_change "
               "FROM(SELECT json.player_tag, json.trophy_change "
               "FROM jsonb_to_recordset($1::jsonb) "
               "AS json(player_tag TEXT, trophy_change INTEGER)) "
               "AS json "
               "WHERE p.player_tag = json.player_tag")
        if self._data_batch:
            await self.bot.pool.execute(sql, self._data_batch)
            total = len(self._data_batch)
            if total > 1:
                self.bot.logger.info(f"Registered {total} trophy changes to the database.")
            self._data_batch.clear()

    @cache.cache()
    async def get_guild_config(self, guild_id):
        # TODO replace *
        sql = "SELECT * FROM guilds WHERE guild_id = $1"
        fetch = await self.bot.pool.fetchrow(sql, guild_id)
        return DatabaseGuild(guild_id=guild_id,
                             bot=self.bot,
                             record=fetch)

    @cache.cache()
    async def get_message(self, channel, message_id):
        try:
            obj = discord.Object(id=message_id + 1)
            msg = await channel.history(limit=1, before=obj).next()
            if msg.id != message_id:
                return None
            return msg
        except Exception:
            return None

    async def new_pushboard_message(self, guild_id):
        guild_config = await self.get_guild_config(guild_id)
        new_msg = await guild_config.pushboard.send("New Leaderboard incoming...")
        sql = ("INSERT INTO message (guild_id, message_id, channel_id) "
               "VALUES ($1, $2, $3)")
        await self.bot.pool.execute(sql, new_msg.guild.id, new_msg.id, new_msg.channel.id)
        return new_msg

    async def safe_delete(self, message_id, delete_message=True):
        sql = ("DELETE FROM messages WHERE message_id = $1 "
               "RETURNING id, guild_id, message_id, channel_id")
        fetch = await self.bot.pool.fetchrow(sql, message_id)
        if not fetch:
            return None
        message = DatabaseMessage(bot=self.bot, record=fetch)
        if not delete_message:
            return message
        self._to_be_deleted.add(message_id)
        m = await message.get_message()
        if not m:
            return
        await m.delete()

    async def get_message_database(self, message_id):
        sql = ("SELECT id, guild_id, message_id, channel_id "
               "FROM messages WHERE message_id = $1")
        fetch = await self.bot.pool.fetchrow(sql, message_id)
        if not fetch:
            return None
        return DatabaseMessage(bot=self.bot, record=fetch)

    async def update_clan_tags(self):
        sql = "SELECT DISTINCT player_tag FROM players"
        fetch = await self.bot.pool.fetch(sql)
        self.bot.coc._player_updates = [n[0] for n in fetch]

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel):
        if not isinstance(channel, discord.TextChannel):
            return
        guild_config = await self.get_guild_config(channel.guild.id)
        if guild_config.updates_channel_id != channel.id:
            return
        sql = "DELETE FROM messages WHERE channel_id = $1"
        await self.bot.pool.execute(sql, channel.id)
        sql = ("UPDATE guilds "
               "SET pushboard_channel_id = NULL, "
               "pushboard_toggle = False "
               "WHERE guild_id = $1")
        await self.bot.pool.execute(sql, channel.guild.id)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        guild_config = await self.get_guild_config(payload.guild_id)
        if guild_config.updates_channel_id != payload.channel_id:
            return
        if payload.message_id in self._to_be_deleted:
            self._to_be_deleted.discard(payload.message_id)
            return
        self.get_message.invalidate(self, payload.message_id)
        message = await self.safe_delete(message_id=payload.message_id, delete_message=False)
        if message:
            await self.new_pushboard_message(payload.guild_id)

    @commands.Cog.listener()
    async def on_raw_bulk_mssage_delete(self, payload):
        guild_config = await self.get_guild_config(payload.guild_id)
        if guild_config.updates_channel_id != payload.channel_id:
            return
        for n in payload.message_ids:
            if n in self._to_be_deleted:
                self._to_be_deleted.discard(n)
                continue
            self.get_message.invalidate(self, n)
            message = await self.safe_delete(message_id=n, delete_message=False)
            if message:
                await self.new_pushboard_message(payload.guild_id)

    async def on_player_trophies_change(self, old_trophies, new_trophies, player):
        trophy_change = new_trophies - old_trophies
        async with self._batch_lock:
            self._data_batch.append({"player_tag": player.tag,
                                     "player_name": player.name,
                                     "clan_tag": player.clan.tag,
                                     "clan_name": player.clan.name,
                                     "trophy_change": trophy_change,
                                     "time_stamp": datetime.utcnow().isoformat()})
            self._clan_events.add(player.clan.tag)

    async def get_updates_messages(self, guild_id, number_of_msg=None):
        guild_config = await self.get_guild_config(guild_id)
        fetch = await guild_config.updates_messages()
        messages = [await n.get_message() for n in fetch]
        messages = [n for n in messages if n]
        size_of = len(messages)
        if not number_of_msg or size_of == number_of_msg:
            return messages
        if size_of > number_of_msg:
            for n in messages[number_of_msg:]:
                await self.safe_delete(n.id)
            return messages[:number_of_msg]
        for _ in range(number_of_msg - size_of):
            messages.append(await self.new_pushboard_message(guild_id))
        return messages

    async def update_pushboard(self, guild_id):
        guild_config = await self.get_guild_config(guild_id)
        if not guild_config.updates_toggle:
            return
        if not guild_config.pushboard:
            return
        sql = "SELECT DISTINCT clan_tag FROM clans WHERE guild_id = $1"
        fetch = await self.bot.pool.fetch(sql, guild_id)
        clans = await self.bot.coc.get_clans((n[0] for n in fetch)).flatten()
        players = []
        for n in clans:
            players.extend(p for p in n.itermembers)
        sql = ("SELECT player_tag, current_trophies, current_attack_wins - starting_attack_wins AS attacks "
               "FROM players "
               "WHERE player_tag = ANY($1::TEXT[]) "
               "ORDER BY current_trophies "
               "LIMIT 100")
        fetch = await self.bot.pool.fetch(sql, [n.tag for n in players])
        db_players = [DatabasePlayer(bot=self.bot, record=n) for n in fetch]
        players = {n.tag: n for n in players if n.tag in set(x.player_tag for x in db_players)}
        message_count = math.ceil(len(db_players) / 20)
        messages = await self.get_updates_messages(guild_id, number_of_msg=message_count)
        if not messages:
            return
        for i, v in enumerate(messages):
            player_data = db_players[i * 20: (i + 1) * 20]
            table = CLYTable()
            for x, y in enumerate(player_data):
                index = i * 20 + x
                if guild_config.pushboard_render == 2:
                    table.add_row([index,
                                   y.current_trophies,
                                   players.get(y.player_tag, MockPlayer()).name])
                else:
                    table.add_row([index,
                                   y.current_trophies,
                                   y.attacks,
                                   players.get(y.player_tag, MockPlayer()).name])
            fmt = table.render_option_2() if \
                guild_config.pushboard_render == 2 else table.render_option_1()
            e = discord.Embed(color=self.bot.color,
                              description=fmt,
                              timestamp=datetime.utcnow())
            e.set_author(name=guild_config.pushboard_title or "Trophy Push Leaderboard",
                         icon_url=guild_config.icon_url or "https://cdn.discordapp.com/emojis/"
                                                           "592028799768592405.png?v=1")
            e.set_footer(text="Last Updated")
            await v.edit(embed=e, content=None)

    @commands.group(invoke_without_command=True)
    @checks.manage_guild()
    async def pushboard(self, ctx):
        """Manage the pushboard for the guild"""
        if ctx.invoked_subcommand is None:
            return await ctx.send_help(ctx.command)

    @pushboard.command(name="create")
    async def pushboard_create(self, ctx, *, name="pushboard"):
        """Creates a pushboard channel for the Trophy Push Leaderboard"""
        guild_id = ctx.guild.id
        self.get_guild_config.invalidate(self, guild_id)
        guild_config = await self.bot.get_guild_config(guild_id)
        if guild_config.pushboard is not None:
            return await ctx.send(f"This server already has a pushboard ({guild_config.pushboard.mention})")
        perms = ctx.channel.permissions_for(ctx.me)
        if not perms.manage_channels:
            return await ctx.send("Please give the Manage Channel permission to the Push Bot role then try again.")
        overwrites = {
            ctx.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, read_message_history=True,
                                                embed_links=True, manage_messages=True),
            ctx.guild.default_role: discord.PermissionOverwrite(read_messages=True,
                                                                send_messages=True,
                                                                read_message_history=True)
        }
        reason = f"{str(ctx.author)} created a pushboard channel."
        try:
            channel = await ctx.guild.create_text_channel(name=name, overwrites=overwrites, reason=reason)
        except discord.Forbidden:
            return await ctx.send("I do not have permissions to create the pushboard channel.")
        except discord.HTTPException:
            return await ctx.send("Creating the channel failed. Try a better name perhaps?")
        msg = await channel.send("Incoming Trophy Push Leaderboard...")
        sql = "INSERT INTO messages (message_id, guild_id, channel_id) VALUES ($1, $2, $3)"
        await ctx.db.execute(sql, msg.id, ctx.guild.id, channel.id)
        sql = "UPDATE guilds SET updates_channel_id = $1, updates_toggle = True WHERE guild_id = $2"
        await ctx.db.execute(sql, channel.id, ctx.guild.id)
        await ctx.send(f"pushboard channel created: {channel.mention}")
        await ctx.invoke(self.pushboard_edit)

    @pushboard.command(name="edit")
    async def pushboard_edit(self, ctx):
        """Edit the format of the guild's pushboard"""
        table = CLYTable()
        table.add_rows([[0, 4721, 56, "Rowcoy"],
                       [1, 4709, 42, "Stitch"],
                       [2, 4658, 37, "t3pps"]])
        table.title = "**Option 1 Example**"
        option_1_render = f"**Option 1 Example**\n{table.render_option_1()}"
        table.clear_rows()
        table.add_rows([[0, 4721, "Rowcoy (Awesome Clan)"],
                        [1, 4709, "Stitch (Lilo's Clan)"],
                        [2, 4658, "t3pps (Other Clan)"]])
        option_2_render = f"**Option 2 Example**\n{table.render_option_2()}"
        embed = discord.Embed(color=self.bot.color)
        fmt = (f"{option_1_render}\n\n{option_2_render}\n\n\n"
               f"These are the 2 available options.\n"
               f"Please click the reaction of the format you \n"
               f"wish to display on your pushboard.")
        embed.description = fmt
        msg = await ctx.send(embed=embed)
        sql = "UPDATE guilds SET pushboard_render = $1 WHERE guild_id = $2"
        reactions = ["1\N{combining enclosing keycap}", "2\N{combining enclosing keycap}"]
        for r in reactions:
            await msg.add_reaction(r)

        def check(r, u):
            return str(r) in reactions and u.id == ctx.author.id and r.message.id == msg.id

        try:
            r,u = await self.bot.wait_for("reaction_add", check=check, timeout=60.0)
        except asyncio.TimeoutError:
            await ctx.db.execute(sql, 1, ctx.guild.id)
            return await ctx.send("I got bored waiting and selected Option 1 for you.")
        await ctx.db.execute(sql, reactions.index(str(r)) + 1, ctx.guild.id)
        await ctx.confirm()
        await ctx.send("All done. Thank you!")
        self.get_guild_config.invalidate(self, ctx.guild.id)

    @pushboard.command(name="icon")
    async def pushboard_icon(self, ctx, *, url: str = None):
        """Specify a fancy icon for the pushboard"""
        if not url or not url.startswith("https://"):
            attachments = ctx.message.attachments
            if not attachments:
                return await ctx.send("You gotta give me something to work with. How about a url or an attachment?")
            url = attachments[0].url
        sql = "UPDATE guilds SET icon_url = $1 WHERE guild_id = $2"
        await ctx.db.execute(sql, url, ctx.guild.id)
        await ctx.confirm()
        self.get_guild_config.invalidate(self, ctx.guild.id)

    @pushboard.command(name="title")
    async def pushboard_title(self, ctx, *, title: str = None):
        """Specify a title for the guild's pushboard"""
        sql = "UPDATE guilds SET pushboard_title = $1 WHERE guild_id = $2"
        await ctx.db.execute(sql, title, ctx.guild.id)
        await ctx.confirm()
        self.get_guild_config.invalidate(self, ctx.guild.id)

    @pushboard.command(name="info")
    async def pushboard_info(self, ctx):
        """Provides info on guild's pushboard"""
        guild_config = await self.bot.get_guild_config(ctx.guild.id)
        table = CLYTable()
        table.title = guild_config.pushboard_title or "PushBoard"
        if guild_config.pushboard_render == 2:
            table.add_rows([[0, 4721, 56, "Rowcoy"],
                            [1, 4709, 42, "Stitch"],
                            [2, 4658, 37, "t3pps"]])
            render = table.render_option_2()
        else:
            table.add_rows([[0, 4721, "Rowcoy (Awesome Clan)"],
                            [1, 4709, "Stitch (Lilo's Clan)"],
                            [2, 4658, "t3pps (Other Clan)"]])
            render = table.render_option_1()
        fmt = f"**PushBoard Example Format:**\n\n{render}\n" \
              f"**Icon:** Please see the icon displayed above.\n"
        channel = guild_config.pushboard
        data = []
        if channel is None:
            data.append("**Channel:** #deleted-channel")
        else:
            data.append(f"**Channel:** {channel.mention}")
        sql = "SELECT clan_name, clan_tag FROM clans WHERE guild_id = $1"
        fetch = await ctx.db.fetch(sql, ctx.guild.id)
        data.append(f"**Clans:** {', '.join(f'{n[0]} ({n[1]})' for n in fetch)}")
        fmt += "\n".join(data)
        embed = discord.Embed(color=self.bot.color,
                              description=fmt)
        embed.set_author(name="PushBoard Info",
                         icon_url=guild_config.icon_url or "https://cdn.discordapp.com/emojis/"
                                                           "592028799768592405.png?v=1")
        await ctx.send(embed=embed)


def setup(bot):
    bot.add_cog(PushBoard(bot))
