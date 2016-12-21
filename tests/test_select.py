# -*- coding: utf-8 -*-
# :Project:   Ytefas -- Test various select functions
# :Created:   mer 21 dic 2016 13:23:19 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016 Arstecnica s.r.l.
#

import sqlalchemy as sa
import pytest

from arstecnica.ytefas import asyncpg
from arstecnica.ytefas.model.tables import auth as auth_tables



@pytest.mark.asyncio
async def test_scalar(pool):
    q = sa.select([auth_tables.users.c.email]) \
        .where(auth_tables.users.c.name == 'segretaria_ca')
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q) == 'segre@ca.it'


@pytest.mark.asyncio
async def test_scalar_named_args(pool):
    q = sa.select([sa.text('auth.login(:user, :password)')])
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q,
                                    named_args=dict(user='admin',
                                                    password='nimda'))


@pytest.mark.asyncio
async def test_fetchall(pool):
    q = sa.select([auth_tables.users]) \
        .where(auth_tables.users.c.email.like('%@ca.it'))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert isinstance(result, list)
        assert len(result) == 2


@pytest.mark.asyncio
async def test_fetchone(pool):
    q = sa.select([auth_tables.users]) \
        .where(auth_tables.users.c.email.like('%@ca.it'))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert type(result).__name__ == 'Record'
        assert result['email'].endswith('@ca.it')
        assert 'name' in result