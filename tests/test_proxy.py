# -*- coding: utf-8 -*-
# :Project:   arstecnica.utils.asyncpg -- Test the asyncpg proxy
# :Created:   gio 29 dic 2016 10:32:24 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

import pytest

from arstecnica.utils.asyncpg.proxy import AsyncpgProxiedQuery
from arstecnica.ytefas.model.tables.auth import users


@pytest.mark.asyncio
async def test_metadata(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result=False, metadata='metadata')

    assert result['metadata']['primary_key'] == 'id'

    byname = {finfo['name']: finfo for finfo in result['metadata']['fields']}
    assert 'email' in byname
    assert 'person_id' in byname

    assert byname['email']['label'] == 'Email'
    assert byname['person_id']['foreign_keys'] == ['risk.persons.id']


@pytest.mark.asyncio
async def test_plain(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection)

    assert len(result) == 4


@pytest.mark.asyncio
async def test_limit(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, limit=1, asdict=True)

    assert len(result) == 1
    assert 'id' in result[0]


@pytest.mark.asyncio
async def test_count(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, result='rows', count='count')

    assert result['message'] == 'Ok'
    assert len(result['rows']) == result['count']


@pytest.mark.asyncio
async def test_sort(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection,
                         sorters='[{"property":"name","direction":"DESC"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'titolare_ca'

    result = await proxy(connection, sort='[{"property":"name"}]')

    assert len(result) == 4
    assert result[0]['name'] == 'admin'

    result = await proxy(connection,
                         sorters=[dict(property="name", direction="ASC")])

    assert len(result) == 4
    assert result[0]['name'] == 'admin'


@pytest.mark.asyncio
async def test_filters(connection):
    proxy = AsyncpgProxiedQuery(users.select())

    result = await proxy(connection, filters=[dict(property="name",
                                            value="titolare_ca",
                                            operator="=")])

    assert len(result) == 1
    assert result[0]['name'] == 'titolare_ca'
