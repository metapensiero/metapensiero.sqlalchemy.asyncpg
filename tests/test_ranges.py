# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- See https://github.com/MagicStack/asyncpg/issues/32
# :Created:   gio 22 dic 2016 11:27:52 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

from datetime import date, timedelta

import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy import asyncpg

from arstecnica.ytefas.model.tables.auth import users


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def test_activity_period(pool):
    today = date.today()
    q = sa.select([users.c.id]) \
        .where(users.c.name == 'test') \
        .where(users.c.validity.contains(today))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None

    oneyearago = date.today() - timedelta(days=365)
    q = sa.select([users.c.id]) \
        .where(users.c.name == 'test') \
        .where(users.c.validity.contains(oneyearago))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is None


async def test_activity_period_with_explicit_range(pool):
    from asyncpg.types import Range

    today = date.today()
    q = sa.select([users.c.id]) \
        .where(users.c.name == 'test') \
        .where(users.c.validity.contains(Range(today, today, upper_inc=True)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None

    oneyearago = date.today() - timedelta(days=365)
    q = sa.select([users.c.id]) \
        .where(users.c.name == 'test') \
        .where(users.c.validity.contains(Range(oneyearago, oneyearago,
                                               upper_inc=True)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is None


async def test_activity_period_works(pool):
    today = date.today()
    q = sa.select([users.c.id]) \
        .where(users.c.name == 'test') \
        .where(users.c.validity.contains(sa.func.daterange(today, today, '[]')))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None
