# karma - A maubot plugin to track the karma of users.
# Copyright (C) 2019 Tulir Asokan
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
from typing import Awaitable, Type, Optional, Tuple
import hashlib
import json
import html

from mautrix.client import Client
from mautrix.types import (Event, StateEvent, EventID, UserID, FileInfo, MessageType, RelationType,
                           MediaMessageEventContent, EventType, ReactionEvent, RedactionEvent)
from mautrix.client.api.types.event.message import media_reply_fallback_body_map
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from maubot import Plugin, MessageEvent
from maubot.handlers import command, event

from .db import make_tables, Karma, Version

UPVOTE_EMOJI = r"(?:\U0001F44D[\U0001F3FB-\U0001F3FF]?)"
UPVOTE_EMOJI_SHORTHAND = r"(?:\:\+1\:)|(?:\:thumbsup\:)"
UPVOTE_TEXT = r"(?:\+(?:1|\+)?)"
UPVOTE = f"^(?:{UPVOTE_EMOJI}|{UPVOTE_EMOJI_SHORTHAND}|{UPVOTE_TEXT})$"

DOWNVOTE_EMOJI = r"(?:\U0001F44E[\U0001F3FB-\U0001F3FF]?)"
DOWNVOTE_EMOJI_SHORTHAND = r"(?:\:-1\:)|(?:\:thumbsdown\:)"
DOWNVOTE_TEXT = r"(?:-(?:1|-)?)"
DOWNVOTE = f"^(?:{DOWNVOTE_EMOJI}|{DOWNVOTE_EMOJI_SHORTHAND}|{DOWNVOTE_TEXT})$"


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("democracy")
        helper.copy("opt_out")
        helper.copy("show_content")
        helper.copy("store_content")
        helper.copy("filter")
        helper.copy("errors.filtered_users")
        helper.copy("errors.vote_on_vote")
        helper.copy("errors.upvote_self")
        helper.copy("errors.already_voted")


def sha1(val: str) -> str:
    return hashlib.sha1(val.encode("utf-8")).hexdigest()


class KarmaBot(Plugin):
    karma_t: Type[Karma]
    version: Type[Version]

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self.karma_t, self.version = make_tables(self.database)

    @command.new("karma", help="View users' karma or karma top lists")
    async def karma(self) -> None:
        pass

    @karma.subcommand("up", help="Upvote an event")
    @command.argument("event_id", "Event ID", required=True)
    def upvote(self, evt: MessageEvent, event_id: EventID) -> Awaitable[None]:
        return self._vote(evt, event_id, +1)

    @karma.subcommand("down", help="Downvote a message")
    @command.argument("event_id", "Event ID", required=True)
    def downvote(self, evt: MessageEvent, event_id: EventID) -> Awaitable[None]:
        return self._vote(evt, event_id, -1)

    @command.passive(UPVOTE)
    def upvote(self, evt: MessageEvent, _: Tuple[str]) -> Awaitable[None]:
        return self._vote(evt, evt.content.get_reply_to(), +1)

    @command.passive(DOWNVOTE)
    def downvote(self, evt: MessageEvent, _: Tuple[str]) -> Awaitable[None]:
        return self._vote(evt, evt.content.get_reply_to(), -1)

    @command.passive(regex=UPVOTE_EMOJI, field=lambda evt: evt.content.relates_to.key,
                     event_type=EventType.REACTION, msgtypes=None)
    def upvote_react(self, evt: ReactionEvent, key: Tuple[str]) -> Awaitable[None]:
        try:
            return self._vote(evt, evt.content.relates_to.event_id, 1)
        except KeyError:
            pass

    @command.passive(regex=DOWNVOTE_EMOJI, field=lambda evt: evt.content.relates_to.key,
                     event_type=EventType.REACTION, msgtypes=None)
    def downvote_react(self, evt: ReactionEvent, key: Tuple[str]) -> Awaitable[None]:
        try:
            return self._vote(evt, evt.content.relates_to.event_id, -1)
        except KeyError:
            pass

    @event.on(EventType.ROOM_REDACTION)
    async def redact(self, evt: RedactionEvent) -> None:
        karma = self.karma_t.get_by_given_from(evt.redacts)
        if karma:
            self.log.debug(f"Deleting {karma} due to redaction by {evt.sender}.")
            karma.delete()

    @karma.subcommand("stats", help="View global karma statistics")
    async def karma_stats(self, evt: MessageEvent) -> None:
        await evt.reply("Not yet implemented :(")

    @karma.subcommand("view", help="View your or another users karma")
    @command.argument("user", "user ID", required=False,
                      parser=lambda val: Client.parse_user_id(val) if val else None)
    async def view_karma(self, evt: MessageEvent, user: Optional[Tuple[str, str]]) -> None:
        if user is not None:
            mxid = UserID(f"@{user[0]}:{user[1]}")
            name = f"[{user[0]}](https://matrix.to/#/{mxid})"
            word_have = "has"
            word_to_be = "is"
        else:
            mxid = evt.sender
            name = "You"
            word_have = "have"
            word_to_be = "are"
        karma = self.karma_t.get_karma(mxid)
        if karma is None or karma.total is None:
            await evt.reply(f"{name} {word_have} no karma :(")
            return
        index = self.karma_t.find_index_from_top(mxid)
        await evt.reply(f"{name} {word_have} {karma.total} karma "
                        f"(+{karma.positive}/-{karma.negative}) "
                        f"and {word_to_be} #{index + 1 or '∞'} on the top list.")

    @karma.subcommand("export", help="Export the data of your karma")
    async def export_own_karma(self, evt: MessageEvent) -> None:
        karma_list = [karma.to_dict() for karma in self.karma_t.export(evt.sender)]
        data = json.dumps(karma_list).encode("utf-8")
        url = await self.client.upload_media(data, mime_type="application/json")
        await evt.reply(MediaMessageEventContent(
            msgtype=MessageType.FILE,
            body=f"karma-{evt.sender}.json",
            url=url,
            info=FileInfo(
                mimetype="application/json",
                size=len(data),
            )
        ))

    @karma.subcommand("breakdown", help="View your karma breakdown")
    async def own_karma_breakdown(self, evt: MessageEvent) -> None:
        await evt.reply("Not yet implemented :(")

    @karma.subcommand("top", help="View the highest rated users")
    async def karma_top(self, evt: MessageEvent) -> None:
        await evt.reply(self._karma_user_list("top"))

    @karma.subcommand("bottom", help="View the lowest rated users")
    async def karma_bottom(self, evt: MessageEvent) -> None:
        await evt.reply(self._karma_user_list("bottom"))

    @karma.subcommand("best", help="View the highest rated messages")
    async def karma_best(self, evt: MessageEvent) -> None:
        await evt.reply(self._karma_message_list("best"))

    @karma.subcommand("worst", help="View the lowest rated messages")
    async def karma_worst(self, evt: MessageEvent) -> None:
        await evt.reply(self._karma_message_list("worst"))

    def _parse_content(self, evt: Event) -> str:
        if not self.config["store_content"]:
            return ""
        if isinstance(evt, MessageEvent):
            if evt.content.msgtype in (MessageType.NOTICE, MessageType.TEXT, MessageType.EMOTE):
                body = evt.content.body
                if evt.content.msgtype == MessageType.EMOTE:
                    body = "/me " + body
                if self.config["store_content"] == "partial":
                    body = body.split("\n")[0]
                    if len(body) > 60:
                        body = body[:50] + " \u2026"
                return body
            name = media_reply_fallback_body_map[evt.content.msgtype]
            return f"[{name}]({self.client.api.get_download_url(evt.content.url)})"
        elif isinstance(evt, StateEvent):
            return "a state event"
        return "an unknown event"

    @staticmethod
    def _sign(value: int) -> str:
        if value > 0:
            return f"+{value}"
        elif value < 0:
            return str(value)
        else:
            return "±0"

    async def _vote(self, evt: MessageEvent, target: EventID, value: int) -> None:
        if not target:
            return
        in_filter = evt.sender in self.config["filter"]
        if self.config["democracy"] == in_filter or sha1(evt.sender) in self.config["opt_out"]:
            if self.config["errors.filtered_users"] and isinstance(evt, MessageEvent):
                await evt.reply("Sorry, you're not allowed to vote.")
            return
        if self.karma_t.is_vote_event(target):
            if self.config["errors.vote_on_vote"] and isinstance(evt, MessageEvent):
                await evt.reply("Sorry, you can't vote on votes.")
            return
        karma_target = await self.client.get_event(evt.room_id, target)
        if not karma_target:
            return
        if karma_target.sender == evt.sender and value > 0:
            if self.config["errors.upvote_self"] and isinstance(evt, MessageEvent):
                await evt.reply("Hey! You can't upvote yourself!")
            return
        karma_id = dict(given_to=karma_target.sender, given_by=evt.sender, given_in=evt.room_id,
                        given_for=karma_target.event_id)
        anonymize = sha1(karma_target.sender) in self.config["opt_out"]
        if anonymize:
            karma_id["given_to"] = ""
        existing = self.karma_t.get(**karma_id)
        if existing is not None:
            if existing.value == value:
                if self.config["errors.already_voted"] and isinstance(evt, MessageEvent):
                    await evt.reply(f"You already {self._sign(value)}'d that message.")
                return
            existing.update(new_value=value)
        else:
            karma = self.karma_t(**karma_id, given_from=evt.event_id, value=value,
                                 content=self._parse_content(karma_target) if not anonymize else "")
            karma.insert()
        if isinstance(evt, MessageEvent):
            await evt.mark_read()

    def _denotify(self, mxid: UserID) -> str:
        localpart, _ = self.client.parse_user_id(mxid)
        return "\u2063".join(localpart)

    def _user_link(self, user_id: UserID) -> str:
        if not user_id:
            return "Anonymous"
        return f"[{self._denotify(user_id)}](https://matrix.to/#/{user_id})"

    def _karma_user_list(self, list_type: str) -> Optional[str]:
        if list_type == "top":
            karma_list = self.karma_t.get_top_users()
            message = "#### Highest karma\n\n"
        elif list_type in ("bot", "bottom"):
            karma_list = self.karma_t.get_bottom_users()
            message = "#### Lowest karma\n\n"
        else:
            return None
        message += "\n".join(
            f"{index + 1}. {self._user_link(karma.user_id)}: "
            f"{self._sign(karma.total)} (+{karma.positive}/-{karma.negative})"
            for index, karma in enumerate(karma_list) if karma.user_id)
        return message

    def _message_text(self, index, event) -> str:
        text = (f"{index + 1}. [Event](https://matrix.to/#/{event.room_id}/{event.event_id})"
                f" by {self._user_link(event.sender)} with"
                f" {self._sign(event.total)} karma (+{event.positive}/-{event.negative})\n")
        if event.content and self.config["show_content"]:
            text += f"    \n    > {html.escape(event.content)}\n"
        return text

    def _karma_message_list(self, list_type: str) -> Optional[str]:
        if list_type == "best":
            karma_list = self.karma_t.get_best_events()
            message = "#### Best messages\n\n"
        elif list_type == "worst":
            karma_list = self.karma_t.get_worst_events()
            message = "#### Worst messages\n\n"
        else:
            return None
        message += "\n".join(self._message_text(index, event)
                             for index, event in enumerate(karma_list))
        return message

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config
