# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Test various select functions
# :Created:   mer 21 dic 2016 13:23:19 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

import logging
import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy import asyncpg


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio


async def test_scalar(pool, users):
    q = sa.select([users.c.password]).where(users.c.name == 'secretary')
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q) == 'secret'


async def test_scalar_2nd_column(pool, users):
    q = (sa.select([users.c.id, users.c.name])
         .where(users.c.name == 'secretary'))
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q, column=1) == 'secretary'


async def test_scalar_named_args(pool, users):
    q = (sa.select([users.c.password])
         .where(users.c.name == sa.bindparam('name'))
         .where(users.c.password == sa.bindparam('password')))
    async with pool.acquire() as conn:
        assert await asyncpg.scalar(conn, q,
                                    named_args=dict(name='admin',
                                                    password='nimda'))


async def test_fetchall(pool, users):
    q = sa.select([users])
    async with pool.acquire() as conn:
        result = await asyncpg.fetchall(conn, q)
        assert isinstance(result, list)
        assert len(result) == 4


async def test_fetchone(pool, users):
    q = (sa.select([users])
         .where(users.c.name.like('secr%')))
    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert type(result).__name__ == 'Record'
        assert result['name'] == 'secretary'


async def test_slow_select(pool):
    from metapensiero.sqlalchemy.asyncpg.funcs import logger

    class MyLogHandler(logging.Handler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.logs = []

        def handle(self, record):
            self.logs.append(record)

    handler = MyLogHandler()
    logger.addHandler(handler)
    try:
        async with pool.acquire() as conn:
            await asyncpg.execute(conn, "SELECT pg_sleep(0.2)",
                                  warn_slow_query_threshold=0.1)
        assert len(handler.logs) == 2
        assert handler.logs[1].levelname == 'WARNING'
    finally:
        logger.removeHandler(handler)
