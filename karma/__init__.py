# karma - A maubot plugin to track the karma of users.
# Copyright (C) 2018 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Awaitable, Type

from sqlalchemy.engine.base import Engine

from maubot import Plugin, CommandSpec, Command, PassiveCommand, Argument, MessageEvent
from mautrix.types import Event, StateEvent

from .db import make_tables, Karma, KarmaCache, Version

COMMAND_PASSIVE_UPVOTE = "xyz.maubot.karma.up"
COMMAND_PASSIVE_DOWNVOTE = "xyz.maubot.karma.down"

ARG_LIST = "$list"
COMMAND_KARMA_LIST = f"karma {ARG_LIST}"

COMMAND_OWN_KARMA = "karma"

COMMAND_UPVOTE = "upvote"
COMMAND_DOWNVOTE = "downvote"

UPVOTE_EMOJI = r"(?:\U0001F44D[\U0001F3FB-\U0001F3FF]?)"
UPVOTE_EMOJI_SHORTHAND = r"(?:\:\+1\:)|(?:\:thumbsup\:)"
UPVOTE_TEXT = r"(?:\+(?:1|\+)?)"
UPVOTE = f"{UPVOTE_EMOJI}|{UPVOTE_EMOJI_SHORTHAND}|{UPVOTE_TEXT}"

DOWNVOTE_EMOJI = r"(?:\U0001F44E[\U0001F3FB-\U0001F3FF]?)"
DOWNVOTE_EMOJI_SHORTHAND = r"(?:\:-1\:)|(?:\:thumbsdown\:)"
DOWNVOTE_TEXT = r"(?:-(?:1|-)?)"
DOWNVOTE = f"{DOWNVOTE_EMOJI}|{DOWNVOTE_EMOJI_SHORTHAND}|{DOWNVOTE_TEXT}"


class KarmaBot(Plugin):
    karma_cache: Type[KarmaCache]
    karma: Type[Karma]
    version: Type[Version]
    db: Engine

    async def start(self) -> None:
        self.db = self.request_db_engine()
        self.karma_cache, self.karma, self.version = make_tables(self.db)
        self.set_command_spec(CommandSpec(commands=[
            Command(syntax=COMMAND_KARMA_LIST, description="View the karma top lists",
                    arguments={ARG_LIST: Argument(matches="(top|bot(tom)?|high(score)?|low)",
                                                  required=True, description="The list to view")}),
            Command(syntax=COMMAND_OWN_KARMA, description="View your karma"),
            Command(syntax=COMMAND_UPVOTE, description="Upvote a message"),
            Command(syntax=COMMAND_DOWNVOTE, description="Downvote a message"),
        ], passive_commands=[
            PassiveCommand(COMMAND_PASSIVE_UPVOTE, match_against="body", matches=UPVOTE),
            PassiveCommand(COMMAND_PASSIVE_DOWNVOTE, match_against="body", matches=DOWNVOTE)
        ]))

        self.client.add_command_handler(COMMAND_PASSIVE_UPVOTE, self.upvote)
        self.client.add_command_handler(COMMAND_PASSIVE_DOWNVOTE, self.downvote)
        self.client.add_command_handler(COMMAND_UPVOTE, self.upvote)
        self.client.add_command_handler(COMMAND_DOWNVOTE, self.downvote)
        self.client.add_command_handler(COMMAND_KARMA_LIST, self.view_karma_list)
        self.client.add_command_handler(COMMAND_OWN_KARMA, self.view_karma)

    async def stop(self) -> None:
        self.client.remove_command_handler(COMMAND_PASSIVE_UPVOTE, self.upvote)
        self.client.remove_command_handler(COMMAND_PASSIVE_DOWNVOTE, self.downvote)
        self.client.remove_command_handler(COMMAND_UPVOTE, self.upvote)
        self.client.remove_command_handler(COMMAND_DOWNVOTE, self.downvote)
        self.client.remove_command_handler(COMMAND_KARMA_LIST, self.view_karma_list)
        self.client.remove_command_handler(COMMAND_OWN_KARMA, self.view_karma)

    @staticmethod
    def parse_content(evt: Event) -> str:
        if isinstance(evt, MessageEvent):
            return "message event"
        elif isinstance(evt, StateEvent):
            return "state event"
        return "unknown event"

    @staticmethod
    def sign(value: int) -> str:
        if value > 0:
            return f"+{value}"
        elif value < 0:
            return str(value)
        else:
            return "±0"

    async def vote(self, evt: MessageEvent, value: int) -> None:
        reply_to = evt.content.get_reply_to()
        if not reply_to:
            return
        karma_target = await self.client.get_event(evt.room_id, reply_to)
        if not karma_target:
            return
        karma_id = dict(given_to=karma_target.sender, given_by=evt.sender, given_in=evt.room_id,
                        given_for=karma_target.event_id)
        existing = self.karma.get(**karma_id)
        if existing is not None:
            if existing.value == value:
                await evt.reply(f"You already {self.sign(value)}'d that message.")
                return
            existing.update(new_value=value)
        else:
            karma = self.karma(**karma_id, given_from=evt.event_id, value=value,
                               content=self.parse_content(karma_target))
            karma.insert()
        await evt.mark_read()

    def upvote(self, evt: MessageEvent) -> Awaitable[None]:
        return self.vote(evt, +1)

    def downvote(self, evt: MessageEvent) -> Awaitable[None]:
        return self.vote(evt, -1)

    async def view_karma(self, evt: MessageEvent) -> None:
        karma = self.karma_cache.get_karma(evt.sender)
        if karma is None:
            await evt.reply("You don't have any karma :(")
            return
        index = self.karma_cache.find_index_from_top(evt.sender)
        await evt.reply(f"You have {karma} karma and are #{index} on the top list.")

    async def view_karma_list(self, evt: MessageEvent) -> None:
        list_type = evt.content.command.arguments[ARG_LIST]
        if not list_type:
            await evt.reply("**Usage**: !karma [top|bottom]")
            return
        if list_type in ("top", "high", "highscore"):
            karma_list = self.karma_cache.get_high()
            message = "#### Highest karma\n\n"
        elif list_type in ("bot", "bottom", "low"):
            karma_list = self.karma_cache.get_low()
            message = "#### Lowest karma\n\n"
        else:
            return
        message += "\n".join(f"{index + 1}. [{mxid}](https://matrix.to/#/{mxid}): {karma}"
                             for index, (mxid, karma) in enumerate(karma_list))
        await evt.reply(message)
