# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Test the asyncpg proxy
# :Created:   gio 29 dic 2016 10:32:24 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

import pytest

from metapensiero.sqlalchemy.asyncpg.proxy import AsyncpgProxiedQuery


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio


async def test_metadata(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result=False, metadata='metadata')

    assert result['metadata']['primary_key'] == 'id'

    byname = {finfo['name']: finfo for finfo in result['metadata']['fields']}
    assert 'name' in byname
    assert 'id' in byname

    assert byname['name']['label'] == 'User name'


async def test_plain(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection)

    assert len(result) == 4


async def test_limit(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, limit=1)

    assert len(result) == 1
    assert 'id' in result[0]


async def test_count(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result='rows', count='count')

    assert result['message'] == 'Ok'
    assert len(result['rows']) == result['count']


async def test_sort(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection,
                         sorters='[{"property":"name","direction":"DESC"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'secretary'

    result = await proxy(connection, sort='[{"property":"name"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'admin'

    result = await proxy(connection,
                         sorters=[dict(property="name", direction="ASC")])

    assert len(result) == 4
    assert result[0]['name'] == 'admin'


async def test_filters(connection, users):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, filters=[dict(property="name",
                                                   value="secretary",
                                                   operator="=")])

    assert len(result) == 1
    assert result[0]['name'] == 'secretary'
