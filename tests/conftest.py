# -*- coding: utf-8 -*-
# :Project:   Ytefas -- pytest configuration
# :Created:   mer 21 dic 2016 12:37:39 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

from os import environ
from sys import exit

import asyncpg
import pytest

from arstecnica.ytefas.asyncpg import fetchall, scalar
from arstecnica.ytefas.model.utils import assert_database_is_up


DB_HOST = environ['DATABASE_HOST']
DB_PORT = environ['DATABASE_PORT']
DB_NAME = environ['DATABASE_NAME']
DB_USER = environ['DATABASE_USER']
DB_PWD = environ['DATABASE_PASSWORD']


@pytest.fixture
def pool(event_loop):
    db_url = f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = event_loop.run_until_complete(asyncpg.create_pool(
        db_url,
        min_size=1,
        max_size=10
    ))
    yield pool
    event_loop.run_until_complete(pool.close())


class Connection(object):
    def __init__(self, pool):
        self.pool = pool

    async def fetchall(self, *args, **kwargs):
        async with self.pool.acquire() as dbc:
            return await fetchall(dbc, *args, **kwargs)

    async def scalar(self, *args, **kwargs):
        async with self.pool.acquire() as dbc:
            return await scalar(dbc, *args, **kwargs)


@pytest.fixture
def connection(pool):
    return Connection(pool)


if not assert_database_is_up(DB_HOST, int(DB_PORT)):
    exit(2)
