# -*- coding: utf-8 -*-
# :Project:   Ytefas -- Test for the asyncpg proxy
# :Created:   gio 29 dic 2016 10:32:24 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016 Arstecnica s.r.l.
#

import pytest

from arstecnica.ytefas.asyncpg.proxy import AsyncpgProxiedQuery
from arstecnica.ytefas.model.tables.auth import users


@pytest.mark.asyncio
async def test_metadata(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn, result=False, metadata='metadata')

    assert result['metadata']['primary_key'] == 'id'
    for finfo in result['metadata']['fields']:
        if finfo['name'] == 'email':
            assert finfo['label'] == 'Email'
            break
    else:
        assert False, "Metadata about 'email' is missing!"


@pytest.mark.asyncio
async def test_plain(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn)

    assert len(result) == 4


@pytest.mark.asyncio
async def test_limit(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn, limit=1, asdict=True)

    assert len(result) == 1
    assert 'id' in result[0]


@pytest.mark.asyncio
async def test_count(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn, result='rows', count='count')

    assert result['message'] == 'Ok'
    assert len(result['rows']) == result['count']


@pytest.mark.asyncio
async def test_sort(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn,
                             sort='[{"property":"name","direction":"DESC"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'titolare_ca'

    async with pool.acquire() as conn:
        result = await proxy(conn, sort='[{"property":"name"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'admin'

    async with pool.acquire() as conn:
        result = await proxy(conn,
                             sort=[dict(property="name", direction="ASC")])

    assert len(result) == 4
    assert result[0]['name'] == 'admin'


@pytest.mark.asyncio
async def test_filters(pool):
    proxy = AsyncpgProxiedQuery(users.select())

    async with pool.acquire() as conn:
        result = await proxy(conn, filters=[dict(property="name",
                                                 value="titolare_ca",
                                                 operator="=")])

    assert len(result) == 1
    assert result[0]['name'] == 'titolare_ca'
