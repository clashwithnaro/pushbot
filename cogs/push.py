from discord.ext import commands, tasks
from config import settings


class Push(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.clan_list = ["#9L2PRL0U", "CVCJR89", "#2Y28CGP80", "#RUJYCVL"]
        self.channel = self.bot.get_channel(settings['logChannels']['push'])
        self.bot.add_events(self.on_player_trophies_change)

    @commands.command(name="get_clan")
    async def get_clan(self, ctx, tag):
        """Just a test so I know things are working"""
        clan = await self.bot.coc_client.get_clan(tag)
        await ctx.send(clan.name)

    async def on_player_trophies_change(self, old_trophies, new_trophies, player):
        change = new_trophies - old_trophies
        await self.channel.send(f"{player.name} just changed {change}")


def setup(bot):
    bot.add_cog(Push(bot))
