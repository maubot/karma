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
from typing import List, Tuple, Optional
from time import time

from sqlalchemy import Column, String, Integer, BigInteger, Text, Table, select
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.engine.base import Engine, Connection
from sqlalchemy.ext.declarative import declarative_base

from mautrix.types import Event, UserID, EventID, RoomID


def make_tables(engine: Engine):
    base = declarative_base()

    class KarmaCache(base):
        __tablename__ = "karma_cache"
        db: Engine = engine
        t: Table = None
        c: ImmutableColumnCollection = None

        user_id: UserID = Column(String(255), primary_key=True)
        karma: int = Column(Integer)

        @classmethod
        def get_karma(cls, user_id: UserID) -> Optional[int]:
            rows = cls.db.execute(select([cls.c.karma]).where(cls.c.user_id == user_id))
            try:
                row = next(rows)
                return row[0]
            except (StopIteration, IndexError):
                return None

        @classmethod
        def _set_karma(cls, user_id: UserID, karma: int, conn: Connection) -> None:
            conn.execute(cls.t.delete().where(cls.c.user_id == user_id))
            conn.execute(cls.t.insert().values(user_id=user_id, karma=karma))

        @classmethod
        def set_karma(cls, user_id: UserID, karma: int, conn: Optional[Connection] = None) -> None:
            if conn:
                cls._set_karma(user_id, karma, conn)
            else:
                with cls.db.begin() as conn:
                    cls._set_karma(user_id, karma, conn)

        @classmethod
        def get_high(cls, limit: int = 10) -> List[Tuple[UserID, int]]:
            return list(cls.db.execute(cls.t.select().order_by(cls.c.karma.desc()).limit(limit)))

        @classmethod
        def get_low(cls, limit: int = 10) -> List[Tuple[UserID, int]]:
            return list(cls.db.execute(cls.t.select().order_by(cls.c.karma.asc()).limit(limit)))

        @classmethod
        def find_index_from_top(cls, user_id: UserID) -> int:
            i = 0
            for (found,) in cls.db.execute(select([cls.c.user_id]).order_by(cls.c.karma.desc())):
                i += 1
                if found == user_id:
                    return i
            return -1

        @classmethod
        def recalculate(cls, user_id: UserID) -> None:
            with cls.db.begin() as txn:
                cls.set_karma(user_id, sum(entry.value for entry in Karma.all(user_id)), txn)

        @classmethod
        def update(cls, user_id: UserID, value_diff: int, conn: Optional[Connection],
                   ignore_if_not_exist: bool = False) -> None:
            if not conn:
                conn = cls.db
            existing = conn.execute(select([cls.c.karma]).where(cls.c.user_id == user_id))
            try:
                karma = next(existing)[0] + value_diff
                conn.execute(cls.t.update().where(cls.c.user_id == user_id).values(karma=karma))
            except (StopIteration, IndexError):
                if ignore_if_not_exist:
                    return
                conn.execute(cls.t.insert().values(user_id=user_id, karma=value_diff))

    class Karma(base):
        __tablename__ = "karma"
        db: Engine = engine
        t: Table = None
        c: ImmutableColumnCollection = None

        given_to: UserID = Column(String(255), primary_key=True)
        given_by: UserID = Column(String(255), primary_key=True)
        given_in: RoomID = Column(String(255), primary_key=True)
        given_for: EventID = Column(String(255), primary_key=True)

        given_from: EventID = Column(String(255))
        given_at: int = Column(BigInteger)
        value: int = Column(Integer)
        content: str = Column(Text)

        @classmethod
        def all(cls, user_id: UserID) -> List['Karma']:
            return [Karma(*row) for row in
                    cls.db.execute(cls.t.select().where(cls.c.given_to == user_id))]

        @classmethod
        def get(cls, given_to: UserID, given_by: UserID, given_in: RoomID, given_for: Event
                ) -> Optional['Karma']:
            rows = cls.db.execute(cls.t.select()
                                  .where(cls.c.given_to == given_to, cls.c.given_by == given_by,
                                         cls.c.given_in == given_in, cls.c.given_for == given_for))
            try:
                given_to, given_by, given_in, given_for, given_at, value, content = next(rows)
                return Karma(given_to, given_by, given_in, given_for, given_at, value, content)
            except (StopIteration, ValueError):
                return None

        def delete(self) -> None:
            with self.db.begin() as txn:
                txn.execute(self.t.delete().where(
                    self.c.given_to == self.given_to, self.c.given_by == self.given_by,
                    self.c.given_in == self.given_in, self.c.given_for == self.given_for))
                KarmaCache.update(self.given_to, self.value, txn, ignore_if_not_exist=True)

        def insert(self) -> None:
            self.given_at = int(time() * 1000)
            with self.db.begin() as txn:
                txn.execute(self.t.insert().values(given_to=self.given_to, given_by=self.given_by,
                                                   given_in=self.given_in, given_for=self.given_for,
                                                   given_from=self.given_from, value=self.value,
                                                   given_at=self.given_at, content=self.content))
                KarmaCache.update(self.given_to, self.value, txn)

        def update(self, new_value: int) -> None:
            self.given_at = int(time() * 1000)
            value_diff = new_value - self.value
            self.value = new_value
            with self.db.begin() as txn:
                txn.execute(self.t.update()
                            .where(self.c.given_to == self.given_to,
                                   self.c.given_by == self.given_by,
                                   self.c.given_in == self.given_in,
                                   self.c.given_for == self.given_for)
                            .values(given_from=self.given_from, value=self.value,
                                    given_at=self.given_at))
                KarmaCache.update(self.given_to, value_diff, txn)

    base.metadata.bind = engine
    KarmaCache.t = KarmaCache.__table__
    KarmaCache.c = KarmaCache.t.c
    Karma.t = Karma.__table__
    Karma.c = Karma.t.c

    # TODO replace with alembic
    base.metadata.create_all()

    return KarmaCache, Karma
