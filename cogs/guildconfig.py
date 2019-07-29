from discord.ext import commands

class GuildConfig(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Event listener which is called when the bot joins a server."""
        # TODO INSERT guild.name, guild.id into database
        pass

    @commands.command(name="add")
    async def add_item(self, ctx, tag: str):
        """Add clan or player to database."""
