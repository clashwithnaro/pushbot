from discord.ext import commands


class NewHelp(commands.Cog):
    """New help file for rcs-bot"""
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="help", hidden=True)
    async def help(self, ctx, command: str = "all"):
        """ Welcome to Push Bot"""
