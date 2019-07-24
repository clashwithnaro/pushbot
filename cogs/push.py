from discord.ext import commands, tasks
from config import settings


class Push(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.clan_list.start()
        self.channel = self.bot.get_channel(settings['logChannels']['push'])
        self.bot.coc.add_events(self.on_player_trophies_change)
        self.bot.coc.start_updates("player")

    def cog_unload(self):
        self.clan_list.cancel()
        print("clan list loop ended")

    @tasks.loop(hours=1)
    async def clan_list(self):
        clan_list = ["#9L2PRL0U", "CVCJR89", "#2Y28CGP80", "#RUJYCVL"]
        await self.bot.coc.add_clan_update(clan_list, member_updates=True)
        print("clan list updated")

    @commands.command(name="get_clan")
    async def get_clan(self, ctx, tag):
        """Just a test so I know things are working"""
        clan = await self.bot.coc_client.get_clan(tag)
        await ctx.send(clan.name)

    async def on_player_trophies_change(self, old_trophies, new_trophies, player):
        change = new_trophies - old_trophies
        print(f"{change} for {player.name}")
        if change == 0:
            return
        if change > 0:
            text = f"just won {change} trophies!"
        else:
            text = f"just lost {change} trophies."
        await self.channel.send(f"{player.name} {text}")


def setup(bot):
    bot.add_cog(Push(bot))
