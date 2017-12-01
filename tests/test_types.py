# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Tests on data types
# :Created:   gio 22 dic 2016 11:27:52 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

from datetime import date
from decimal import Decimal

import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy import asyncpg


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio


## Ranges


async def test_range(pool, users):
    some_time_ago = date(2016, 11, 1)
    q = (sa.select([users.c.name])
         .where(users.c.validity.contains(some_time_ago)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None
        assert result['name'] == 'admin'

    future = date(2018, 3, 18)
    q = (sa.select([users.c.id])
         .where(users.c.validity.contains(future)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert len(result) == 3


async def test_explicit_range(pool, users):
    today = date(2017, 11, 30)
    today_range = asyncpg.Range(today, today, upper_inc=True)
    q = (sa.select([users.c.name])
         .where(users.c.validity.contains(today_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert result is not None
        assert len(result) == 4

    yesterday = date(2017, 11, 29)
    yesterday_range = asyncpg.Range(yesterday, yesterday, upper_inc=True)
    q = (sa.select([users.c.name])
         .where(users.c.validity.contains(yesterday_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert result is not None
        assert len(result) == 2


async def test_daterange_func(pool, users):
    today = date(2017, 11, 30)
    today_range = sa.func.daterange(today, today, '[]')
    q = (sa.select([users.c.id])
         .where(users.c.validity.contains(today_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None


## Intervals


async def test_interval(pool, users):
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            q = (sa.select([users.c.id, users.c.max_renew])
                 .where(users.c.name == 'secretary'))
            id, max_renew = await asyncpg.fetchone(conn, q)
            assert max_renew.months == 12*1 + 2
            assert max_renew.days == 3

            u = (users.update()
                 .where(users.c.id == id)
                 .values(max_renew=asyncpg.Interval(3, 2, 1)))
            assert await asyncpg.execute(conn, u) == 'UPDATE 1'

            q = (sa.select([users.c.max_renew])
                 .where(users.c.id == id))
            result = await asyncpg.scalar(conn, q)

            assert result == asyncpg.Interval(3, 2, 1)
            assert result == (3, 2, 1)
        finally:
            await tx.rollback()


## Hstores


async def test_hstore(pool, users):
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            q = (sa.select([users.c.id, users.c.options])
                 .where(users.c.name == 'secretary'))
            id, options = await asyncpg.fetchone(conn, q)
            assert options is None

            u = (users.update()
                 .where(users.c.id == id)
                 .values(options={'height': "1.69"}))
            assert await asyncpg.execute(conn, u) == 'UPDATE 1'

            q = (sa.select([users.c.options])
                 .where(users.c.id == id))
            options = await asyncpg.scalar(conn, q)
            assert options == {'height': "1.69"}
        finally:
            await tx.rollback()


## JSONB


async def test_jsonb(pool, users):
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            q = (sa.select([users.c.id, users.c.details])
                 .where(users.c.name == 'secretary'))
            id, details = await asyncpg.fetchone(conn, q)
            assert details is None

            u = (users.update()
                 .where(users.c.id == id)
                 .values(details={'height': Decimal("1.69"),
                                  'birthdate': date(1968, 3, 18),
                                  'stage': asyncpg.Range(date(2017, 1, 31),
                                                         date(2017, 3, 31))}))
            assert await asyncpg.execute(conn, u) == 'UPDATE 1'

            q = (sa.select([users.c.details])
                 .where(users.c.id == id))
            details = await asyncpg.scalar(conn, q)
            assert details['height'] == Decimal("1.69")
            assert details['birthdate'] == date(1968, 3, 18)
            assert details['stage'] == '[2017-01-31,2017-03-31)'
        finally:
            await tx.rollback()
