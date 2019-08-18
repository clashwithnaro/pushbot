import coc
from datetime import datetime

from cogs.utils.formatters import readable_time


class DatabaseGuild:
    __slots__ = ('bot', 'guild_id', 'id', 'updates_channel_id', 'updates_header_id', 'updates_toggle',
                 'log_channel_id', 'log_toggle', 'ign', 'don', 'rec', 'tag', 'claimed_by', 'clan',
                 'auto_claim', 'donationboard_title', 'icon_url', 'donationboard_render', 'log_interval')

    def __init__(self, *, guild_id, bot, record=None):
        self.guild_id = guild_id
        self.bot = bot

        if record:
            # TODO Update pushbot.guilds database with these fields
            self.id = record['id']
            self.updates_channel_id = record['updates_channel_id']
            self.updates_toggle = record['updates_toggle']
            self.log_channel_id = record['log_channel_id']
            self.log_toggle = record['log_toggle']
            self.ign = record['updates_ign']
            self.don = record['updates_don']
            self.rec = record['updates_rec']
            self.tag = record['updates_tag']
            self.claimed_by = record['updates_claimed_by']
            self.clan = record['updates_clan']  # record['updates_clan']
            self.auto_claim = record['auto_claim']
            self.donationboard_title = record['donationboard_title']
            self.icon_url = record['icon_url']
            self.donationboard_render = record['donationboard_render']
            self.log_interval = record['log_interval']
        else:
            self.updates_channel_id = None
            self.log_channel_id = None
            self.updates_toggle = False
            self.log_toggle = False
            self.auto_claim = False

    @property
    def pushboard(self):
        return self.bot.get_channel(self.updates_channel_id)

    @property
    def log_channel(self):
        guild = self.bot.get_guild(self.guild_id)
        return guild and guild.get_channel(self.log_channel_id)

    async def updates_messages(self):
        query = "SELECT * FROM messages WHERE guild_id = $1"
        fetch = await self.bot.pool.fetch(query, self.guild_id)
        return [DatabaseMessage(bot=self.bot, record=n) for n in fetch]


class DatabaseClan:
    def __init__(self, *, bot, clan_tag=None, record=None):
        self.bot = bot

        if record:
            self.id = record["clan_id"]
            self.clan_tag = record["clan_tag"]
            self.event_id = record["event_id"]
        else:
            self.clan_tag = coc.utils.correct_tag(clan_tag)

    async def full_clan(self):
        clan = await self.bot.coc.get_clan(self.clan_tag)
        # TODO perhaps grab event_id's while here
        return clan


class DatabasePlayer:
    def __init__(self, *, bot, player_tag=None, record=None):
        self.bot = bot

        if record:
            self.id = record['player_id']
            self.player_name = record['player_name']
            self.player_tag = record['player_tag']
            self.current_trophies = record['current_trophies']
            self.attacks = record['attacks']
            self.user_id = record['user_id']
        else:
            self.user_id = None
            self.player_tag = player_tag

    @property
    def owner(self):
        return self.bot.get_user(self.user_id)

    async def full_player(self):
        return await self.bot.coc.get_player(self.player_tag)


class DatabasePushEvent:
    def __init__(self, *, bot, record=None):
        self.bot = bot

        if record:
            self.id = record['event_id']
            self.guild_id = record['guild_id']
            self.event_name = record['event_name']
            self.channel_id = record['channel_id']
            self.log_interval = record['log_interval']
            self.log_toggle = record['log_toggle']
        else:
            self.guild_id = None

    @property
    def guild(self):
        return self.bot.get_guild(self.guild_id)

    @property
    def channel(self):
        return self.bot.get_channel(self.channel_id)

    @property
    def interval_seconds(self):
        return self.log_interval.total_seconds()


class DatabaseMessage:
    def __init__(self, *, bot, record=None):
        self.bot = bot

        if record:
            self.id = record['id']
            self.guild_id = record['guild_id']
            self.message_id = record['message_id']
            self.channel_id = record['channel_id']

        else:
            self.guild_id = None
            self.channel_id = None
            self.message_id = None

    @property
    def guild(self):
        return self.bot.get_guild(self.guild_id)

    @property
    def channel(self):
        return self.bot.get_channel(self.channel_id)

    async def get_message(self):
        return await self.bot.donationboard.get_message(self.channel, self.message_id)


class DatabaseEvent:
    def __init__(self, *, bot, record=None):
        self.bot = bot

        if record:
            self.id = record['coc_event_id']
            self.player_tag = record['player_tag']
            self.clan_tag = record['clan_tag']
            self.trophy_change = record['trophy_change']
            self.time = record['time_stamp']

        else:
            self.time = None

    @property
    def readable_time(self):
        return readable_time((datetime.utcnow() - self.time).total_seconds())

    @property
    def delta_since(self):
        return datetime.utcnow() - self.time
