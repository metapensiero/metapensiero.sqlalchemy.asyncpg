# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Test the asyncpg proxy
# :Created:   gio 29 dic 2016 10:32:24 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

import pytest

from metapensiero.sqlalchemy.asyncpg.proxy import AsyncpgProxiedQuery
from arstecnica.ytefas.model.tables.auth import users


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def test_metadata(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result=False, metadata='metadata')

    assert result['metadata']['primary_key'] == 'id'

    byname = {finfo['name']: finfo for finfo in result['metadata']['fields']}
    assert 'name' in byname
    assert 'person_id' in byname

    assert byname['name']['label'] == 'User name'
    assert byname['person_id']['foreign_keys'] == ('common.persons.id',)


async def test_plain(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection)

    assert len(result) == 5


async def test_limit(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, limit=1)

    assert len(result) == 1
    assert 'id' in result[0]


async def test_count(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result='rows', count='count')

    assert result['message'] == 'Ok'
    assert len(result['rows']) == result['count']


async def test_sort(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection,
                         sorters='[{"property":"name","direction":"DESC"}]')

    assert len(result) == 5
    assert result[0]['name'] == 'titolare_ca'

    result = await proxy(connection, sort='[{"property":"name"}]')

    assert len(result) == 5
    assert result[0]['name'] == 'admin'

    result = await proxy(connection,
                         sorters=[dict(property="name", direction="ASC")])

    assert len(result) == 5
    assert result[0]['name'] == 'admin'


async def test_filters(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, filters=[dict(property="name",
                                                   value="titolare_ca",
                                                   operator="=")])

    assert len(result) == 1
    assert result[0]['name'] == 'titolare_ca'
