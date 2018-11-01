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
from typing import Tuple, Optional, Type, Iterable, Dict, Any, NamedTuple
from time import time

from sqlalchemy import (Column, String, Integer, BigInteger, Text, Table,
                        select, and_, or_, func, case, asc, desc)
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base

from mautrix.types import Event, UserID, EventID, RoomID

EventKarmaStats = NamedTuple("EventKarmaStats", room_id=RoomID, event_id=EventID, sender=UserID,
                             content=str, total=int, positive=int, negative=int)
UserKarmaStats = NamedTuple("UserKarmaStats", user_id=UserID, total=int, positive=int, negative=int)


class Karma:
    __tablename__ = "karma"
    db: Engine = None
    t: Table = None
    c: ImmutableColumnCollection = None

    given_to: UserID = Column(String(255), primary_key=True)
    given_by: UserID = Column(String(255), primary_key=True)
    given_in: RoomID = Column(String(255), primary_key=True)
    given_for: EventID = Column(String(255), primary_key=True)

    given_from: EventID = Column(String(255), unique=True)
    given_at: int = Column(BigInteger)
    value: int = Column(Integer)
    content: str = Column(Text)

    @classmethod
    def get_best_events(cls, limit: int = 10) -> Iterable['EventKarmaStats']:
        return cls.get_event_stats(direction=desc, limit=limit)

    @classmethod
    def get_worst_events(cls, limit: int = 10) -> Iterable['EventKarmaStats']:
        return cls.get_event_stats(direction=asc, limit=limit)

    @classmethod
    def get_event_stats(cls, direction, limit: int = 10) -> Iterable['EventKarmaStats']:
        c = cls.c
        return (EventKarmaStats(*row) for row in cls.db.execute(
            select([c.given_in, c.given_for, c.given_to, c.content,
                    func.sum(c.value).label("total"),
                    func.sum(case([(c.value > 0, c.value)], else_=0)).label("positive"),
                    func.abs(func.sum(case([(c.value < 0, c.value)], else_=0))).label("negative")])
                .group_by(c.given_for)
                .order_by(direction("total"), asc(c.given_for))
                .limit(limit)))

    @classmethod
    def get_top_users(cls, limit: int = 10) -> Iterable['UserKarmaStats']:
        return cls.get_user_stats(direction=desc, limit=limit)

    @classmethod
    def get_bottom_users(cls, limit: int = 10) -> Iterable['UserKarmaStats']:
        return cls.get_user_stats(direction=asc, limit=limit)

    @classmethod
    def get_user_stats(cls, direction, limit: int = 10) -> Iterable['UserKarmaStats']:
        c = cls.c
        return (UserKarmaStats(*row) for row in cls.db.execute(
            select([c.given_to,
                    func.sum(c.value).label("total"),
                    func.sum(case([(c.value > 0, c.value)], else_=0)).label("positive"),
                    func.abs(func.sum(case([(c.value < 0, c.value)], else_=0))).label("negative")])
                .group_by(c.given_to)
                .order_by(direction("total"), asc(c.given_to))
                .limit(limit)))

    @classmethod
    def get_karma(cls, user_id: UserID) -> Optional['UserKarmaStats']:
        c = cls.c
        rows = cls.db.execute(
            select([c.given_to,
                    func.sum(c.value).label("total"),
                    func.sum(case([(c.value > 0, c.value)], else_=0)).label("positive"),
                    func.abs(func.sum(case([(c.value < 0, c.value)], else_=0))).label("negative")]
                   ).where(c.given_to == user_id))
        try:
            return UserKarmaStats(*next(rows))
        except StopIteration:
            return None

    @classmethod
    def find_index_from_top(cls, user_id: UserKarmaStats) -> int:
        c = cls.c
        rows = cls.db.execute(select([c.given_to])
                              .group_by(c.given_to)
                              .order_by(desc(func.sum(c.value)), asc(c.given_to)))
        for i, row in enumerate(rows):
            if row[0] == user_id:
                return i
        return -1

    @classmethod
    def all(cls, user_id: UserID) -> Iterable['Karma']:
        return (cls(given_to=given_to, given_by=given_by, given_in=given_in, given_for=given_for,
                    given_from=given_from, given_at=given_at, value=value, content=content)
                for given_to, given_by, given_in, given_for, given_from, given_at, value, content
                in cls.db.execute(cls.t.select().where(cls.c.given_to == user_id)))

    @classmethod
    def export(cls, user_id: UserID) -> Iterable['Karma']:
        return (cls(given_to=given_to, given_by=given_by, given_in=given_in, given_for=given_for,
                    given_from=given_from, given_at=given_at, value=value, content=content)
                for given_to, given_by, given_in, given_for, given_from, given_at, value, content
                in cls.db.execute(cls.t.select().where(or_(cls.c.given_to == user_id,
                                                           cls.c.given_by == user_id))))

    @classmethod
    def is_vote_event(cls, event_id: EventID) -> bool:
        rows = cls.db.execute(cls.t.select().where(cls.c.given_from == event_id))
        try:
            next(rows)
            return True
        except StopIteration:
            return False

    @classmethod
    def get(cls, given_to: UserID, given_by: UserID, given_in: RoomID, given_for: Event
            ) -> Optional['Karma']:
        rows = cls.db.execute(cls.t.select().where(and_(
            cls.c.given_to == given_to, cls.c.given_by == given_by,
            cls.c.given_in == given_in, cls.c.given_for == given_for)))
        try:
            (given_to, given_by, given_in, given_for,
             given_from, given_at, value, content) = next(rows)
        except StopIteration:
            return None
        return cls(given_to=given_to, given_by=given_by, given_in=given_in, given_for=given_for,
                   given_from=given_from, given_at=given_at, value=value, content=content)

    def delete(self) -> None:
        self.db.execute(self.t.delete().where(and_(
            self.c.given_to == self.given_to, self.c.given_by == self.given_by,
            self.c.given_in == self.given_in, self.c.given_for == self.given_for)))

    def insert(self) -> None:
        self.given_at = int(time() * 1000)
        self.db.execute(self.t.insert().values(given_to=self.given_to, given_by=self.given_by,
                                               given_in=self.given_in, given_for=self.given_for,
                                               given_from=self.given_from, value=self.value,
                                               given_at=self.given_at, content=self.content))

    def update(self, new_value: int) -> None:
        self.given_at = int(time() * 1000)
        self.value = new_value
        self.db.execute(self.t.update().where(and_(
            self.c.given_to == self.given_to, self.c.given_by == self.given_by,
            self.c.given_in == self.given_in, self.c.given_for == self.given_for
        )).values(given_from=self.given_from, value=self.value, given_at=self.given_at))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "to": self.given_to,
            "by": self.given_by,
            "in": self.given_in,
            "for": self.given_for,
            "from": self.given_from,
            "at": self.given_at,
            "value": self.value,
            "content": self.content,
        }


class Version:
    __tablename__ = "version"
    db: Engine = None
    t: Table = None
    c: ImmutableColumnCollection = None

    version: int = Column(Integer, primary_key=True)


def make_tables(engine: Engine) -> Tuple[Type[Karma], Type[Version]]:
    base = declarative_base()

    class KarmaImpl(Karma, base):
        __table__: Table

    class VersionImpl(Version, base):
        __table__: Table

    base.metadata.bind = engine
    for table in KarmaImpl, VersionImpl:
        table.db = engine
        table.t = table.__table__
        table.c = table.__table__.c
        table.Karma = KarmaImpl

    # TODO replace with alembic
    base.metadata.create_all()

    return KarmaImpl, VersionImpl
