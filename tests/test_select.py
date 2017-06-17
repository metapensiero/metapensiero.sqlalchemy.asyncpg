# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Test various select functions
# :Created:   mer 21 dic 2016 13:23:19 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy import asyncpg

from arstecnica.ytefas.model.tables.auth import users


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def test_scalar(pool):
    q = sa.select([users.c.name]) \
        .where(users.c.name == 'segretaria_ca')
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q) == 'segretaria_ca'


async def test_scalar_2nd_column(pool):
    q = sa.select([users.c.person_id, users.c.name]) \
        .where(users.c.name == 'segretaria_ca')
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q, column=1) == 'segretaria_ca'


async def test_scalar_named_args(pool):
    q = sa.select([sa.text('user_id')],
                  from_obj=sa.text('auth.login(:user, :password)'))
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q,
                                    named_args=dict(user='admin',
                                                    password='nimda'))


async def test_fetchall(pool):
    q = sa.select([users]) \
        .where(users.c.name.like('%_ca'))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert isinstance(result, list)
        assert len(result) == 3


async def test_fetchone(pool):
    q = sa.select([users]) \
        .where(users.c.name.like('%_ca'))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert type(result).__name__ == 'Record'
        assert result['name'].endswith('_ca')
