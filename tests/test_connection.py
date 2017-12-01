# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Connection tests
# :Created:   ven 01 dic 2017 11:19:56 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2017 Lele Gaifax
#

import pytest
import sqlalchemy as sa


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio


async def test_cursor(connection, users):
    q = sa.select([users.c.id]).where(users.c.name == 'admin')
    cursor = connection.cursor(q)
    async with connection.transaction():
        count = 0
        async for result in cursor:
            assert result and result['id']
            count += 1
        assert count == 1


async def test_execute(connection, users):
    i = users.insert().values(name='foo', password='bar', validity=None)
    tx = connection.transaction()
    await tx.start()
    try:
        result = await connection.execute(i)
        assert result == 'INSERT 0 1'
    finally:
        await tx.rollback()


async def test_fetchone(connection, users):
    q = sa.select([users.c.id]).where(users.c.name == 'admin')
    result = await connection.fetchone(q)
    assert result and result['id']


async def test_prepare(connection, users):
    q = sa.select([users.c.id]).where(users.c.name == 'admin')
    stmt = await connection.prepare(q)
    assert stmt.get_parameters()[0].name == 'varchar'
