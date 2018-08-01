#!/usr/bin/env python
import datetime
from contextlib import contextmanager

from sqlalchemy import (
    BigInteger, Text, Integer, DateTime, DECIMAL,
    ForeignKey,
    Column
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.schema import MetaData
from sqlalchemy.orm import relationship

from health_tracker_config import engine, schema_name


def pkey(id_str, dtype=Integer):
    return Column(
        id_str,
        dtype,
        autoincrement=True,
        primary_key=True,
    )


def datetime_col(colname):
    return Column(
        colname,
        DateTime,
        nullable=False,
        default="date_trunc('second', now())::timestamp",
    )

SABase = declarative_base(
    metadata=MetaData(
        bind=engine,
        schema=schema_name,
    ),
)


class Base:
    created_at = datetime_col('created_at')
    modified_at = datetime_col('modified_at')

    def __init__(self, time=None):
        if time is None:
            time = datetime.datetime.now().replace(microsecond=0)
        self.created_at = self.modified_at = time

    @staticmethod
    def get_col_name(col):
        return str(col).split('.')[-1]

    @classmethod
    def get_row(cls, col, value, sess):
        query = sess.query(cls).filter(col==value)

        # if it exists, then return it
        row = query.one_or_none()
        if row is not None:
            return row

        # otherwise, create one
        row = cls()
        setattr(
            row,
            cls.get_col_name(col),
            value
        )

        # make sure the row is entered into the db and
        # has its id field populated so its id can be
        # referenced
        sess.add(row)
        sess.commit()

        return row

    Session = None
    @classmethod
    def set_sess(cls, Session):
        cls.Session = Session

    @classmethod
    @contextmanager
    def get_session(cls):
        """

        Note that operations that create rows in
        multiple tables at once need to share
        the same session.
        """
        if cls.Session is None:
            raise Exception("session not set. must be set using Base.set_sess(sqlalchemy.orm.sessionmaker(bind=engine))")
        sess = cls.Session()
        try:
            yield sess
        except KeyboardInterrupt:
            raise
        except Exception:
            sess.rollback()
        else:
            sess.commit()
        finally:
            sess.close()

    def __repr__(self):
        """Generic repr method for 
        """
        attrs = list()
        for k in self.__init__.__code__.co_varnames[1:]:
            if not hasattr(self, k):
                continue
            attrs.append('{}={}'.format(
                k, repr(getattr(self, k))
            ))
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(attrs),
        )

    def __str__(self):
        return repr(self)


class Weight(Base, SABase):
    __tablename__ = 'weight'
    wid = pkey('wid')
    weight = Column('weight', DECIMAL)

    def __repr__(self):
        return '{}(weight={})'.format(
            self.__class__.__name__,
            repr(self.weight),
        )


class Food(Base, SABase):
    __tablename__ = 'food'
    fid = pkey('fid')
    food = Column('food', Text, unique=True)

    @classmethod
    def get_row(cls, food_str, sess):
        return super(Food, cls).get_row(cls.food, food_str, sess)

    def __repr__(self):
        return '{}(food={})'.format(
            self.__class__.__name__,
            repr(self.food),
        )


class Eat(Base, SABase):
    __tablename__ = 'eat'

    # primary key - "eat id"
    eid = pkey('eid', dtype=BigInteger)

    ### Foreign columns

    fid = Column('fid', Integer, ForeignKey('food.fid'))
    food = relationship('Food')

    def __init__(self, food_str, time=None):
        food = Food.get_row(food_str)
        self.fid = food.fid

        if time is None:
            time = datetime.datetime.now()
        self.created_at = self.modified_at = time

    def __repr__(self):
        return '{}(food_str={}, time={})'.format(
            self.__class__.__name__,
            *[repr(obj) for obj in [
                self.food.food,
                self.created_at,
            ]]
        )
    def __str__(self):
        return repr(self)
