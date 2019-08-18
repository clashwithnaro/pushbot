import discord
import coc
from discord.ext import commands
from .utils import checks


class GuildConfig(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Event listener which is called when the bot joins a server."""
        sql = "INSERT INTO guilds (guild_id, guild_name) VALUES ($1, $2) ON CONFLICT (guild_id) DO NOTHING"
        await self.bot.pool.execute(sql, guild.id, guild.name)
        welcome_msg = ("Thank you for installing PushBot! My job is to help you track your trophy push event! "
                       "My prefix is `:trophy:`. To get started, how about telling me which clans you'd like to "
                       "include in your event.  Use the `:trophy:add` command to add one or more clan tags "
                       "(separated by space).")
        if guild.system_channel:
            try:
                await guild.system_channel.send(welcome_msg)
                return
            except (discord.Forbidden, discord.HTTPException):
                pass
        for channel in guild.channels:
            if not isinstance(channel, discord.TextChannel):
                continue
            if channel.permissions_for(channel.guild.get_member(self.bot.user_id)).send_messages:
                try:
                    await channel.send(welcome_msg)
                except (discord.Forbidden, discord.HTTPException):
                    pass
                return

    @commands.command(name="add", aliases=["addclan", "add_clan"])
    @checks.manage_guild()
    async def add_clan(self, ctx, *tags):
        """Add clan (and its players) to database."""
        # TODO OK to add if event is over | not OK to add if in another active event
        for tag in tags:
            tag = coc.utils.correct_tag(tag)
            sql = ("SELECT event_id FROM events WHERE guild_id = $1 "
                   "ORDER BY event_start_time DESC")
            fetch = await ctx.db.fetchrow(sql, ctx.guild.id)
            guild_event = fetch["event_id"]
            sql = "SELECT clan_tag, event_id FROM clans WHERE event_id = $1 AND clan_tag = $2"
            fetch = await ctx.db.fetch(sql, guild_event, tag)
            if fetch:
                raise commands.BadArgument("This clan is already linked to your event.")
            try:
                clan = await ctx.coc.get_clan(tag)
            except coc.NotFound:
                raise commands.BadArgument("I can't find a clan with the tag: {tag}")
            sql = "INSERT INTO clans (clan_tag, clan_name, event_id) VALUES ($1, $2)"
            await ctx.db.execute(sql, clan.tag, clan.name, guild_event)
            await ctx.send(f"{clan.name} ({clan.tag}) added to the database. Players will be added to the database "
                           f"when the event starts.")

    @commands.command(name="remove", aliases=["removeclan", "remove_clan"])
    @checks.manage_guild()
    async def remove_clan(self, ctx, *tags):
        """Remove clan (and its players) from the database."""
        for tag in tags:
            tag = coc.utils.correct_tag(tag)
            sql = "SELECT event_id FROM clans WHERE clan_tag = $1 ORDER BY event_start_time DESC"
            event_ids = ctx.db.fetch(sql, tag)
            if len(event_ids) == 0:
                try:
                    clan = await ctx.coc.get_clan(tag)
                    raise commands.BadArgument(f"{clan.name} ({clan.tag}) is not currently a part of your event.")
                except coc.NotFound:
                    raise commands.BadArgument(f"I can't find a clan with the tag: {tag}")
            if len(event_ids) == 1:
                sql = "DELETE FROM players WHERE clan_tag = $1"
                await ctx.db.execute(sql, tag)
                sql = "DELETE FROM clans WHERE clan_tag = $1"
                await ctx.db.execute(sql, tag)
                clan = await ctx.coc.get_clan(tag)
                await ctx.send(f"{clan.name} ({clan.tag}) has been removed from your event.")
                return
            # TODO Deal with clans in multiple events (check for past events)

    @commands.command(name="list")
    @checks.manage_guild()
    async def list(self, ctx):
        """Get list of clans in the next event tied to this guild"""
        sql = ("SELECT event_id FROM events WHERE guild_id = $1 AND event_end_time > CURRENT_TIMESTAMP "
               "ORDER BY event_end_time")
        event_id = ctx.db.fetchrow(sql, ctx.guild.id)
        sql = ("SELECT clan_name, clan_tag FROM clans WHERE event_id = $1 "
               "ORDER BY clan_name")
        clans = ctx.db.fetch(sql, event_id)
        clan_list = [f"{clan['clan_name']} ({clan['clan_tag']})" for clan in clans]
        nl = "\n"
        await ctx.send(f"**Clans in your event:**{nl}{nl.join(clan_list)}")


def setup(bot):
    bot.add_cog(GuildConfig(bot))
