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
                       "(separated by comma).")
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

    @commands.command(name="add")
    @checks.is_mod()
    async def add_clan(self, ctx, tag: str):
        """Add clan or player to database."""
        tag = coc.utils.correct_tag(tag)
        sql = "SELECT * FROM claims WHERE clan_tag = $1 AND guild_id = $2"
        fetch = await ctx.db.fetch(sql, tag, ctx.guild.id)
        if fetch:
            raise commands.BadArgument("This clan is already linked to your server")
        try:
            clan = await ctx.coc.get_clan(tag)
        except coc.NotFound:
            raise commands.BadArgument("I can't find a clan with the tag: {tag}")
        sql = "INSERT INTO claims (guild_id, clan_tag) VALUES ($1, $2)"
        await self.bot.pool.execute(sql, ctx.guild.id, tag)
        await ctx.send(f"{clan.name} ({clan.tag}) added to the database.")
        # TODO Do I need to add players at this point? Maybe not. Event start instead.
        # TODO Time adding players vs updating trophies


def setup(bot):
    bot.add_cog(GuildConfig(bot))
