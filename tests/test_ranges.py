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
from arstecnica.ytefas.model.tables.common import person_contracts, persons


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def test_activity_period(pool):
    today = date.today()
    q = (sa.select([users.c.id],
                   from_obj=users.join(persons).join(person_contracts))
         .where(users.c.name == 'test')
         .where(person_contracts.c.validity.contains(today)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None

    one_year_ago = date.today() - timedelta(days=365)
    q = (sa.select([users.c.id],
                   from_obj=users.join(persons).join(person_contracts))
         .where(users.c.name == 'test')
         .where(person_contracts.c.validity.contains(one_year_ago)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is None


async def test_activity_period_with_explicit_range(pool):
    from asyncpg.types import Range

    today = date.today()
    today_range = Range(today, today, upper_inc=True)
    q = (sa.select([users.c.id],
                   from_obj=users.join(persons).join(person_contracts))
         .where(users.c.name == 'test')
         .where(person_contracts.c.validity.contains(today_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None

    one_year_ago = date.today() - timedelta(days=365)
    year_range = Range(one_year_ago, one_year_ago, upper_inc=True)
    q = (sa.select([users.c.id],
                   from_obj=users.join(persons).join(person_contracts))
         .where(users.c.name == 'test')
         .where(person_contracts.c.validity.contains(year_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is None


async def test_activity_period_works(pool):
    today = date.today()
    today_range = sa.func.daterange(today, today, '[]')
    q = (sa.select([users.c.id],
                   from_obj=users.join(persons).join(person_contracts))
         .where(users.c.name == 'test')
         .where(person_contracts.c.validity.contains(today_range)))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result is not None
