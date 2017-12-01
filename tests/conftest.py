# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- pytest configuration
# :Created:   mer 21 dic 2016 12:37:39 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

import asyncio

import asyncpg
import pytest
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from metapensiero.sqlalchemy.asyncpg import Connection, register_custom_codecs


@pytest.fixture(scope='session')
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='session')
def pool(event_loop):
    event_loop = asyncio.get_event_loop()
    adminconn = event_loop.run_until_complete(
        asyncpg.connect(database='postgres', loop=event_loop))
    event_loop.run_until_complete(adminconn.execute(
        'CREATE DATABASE sasyncpg_test'))

    conn = event_loop.run_until_complete(asyncpg.connect(
        database='sasyncpg_test', loop=event_loop))
    event_loop.run_until_complete(conn.execute('CREATE EXTENSION "hstore"'))
    event_loop.run_until_complete(conn.execute(
        "CREATE TABLE users ("
        " id serial NOT NULL PRIMARY KEY,"
        " name varchar(25) NOT NULL,"
        " password varchar(25) NOT NULL,"
        " validity daterange,"
        " max_renew interval,"
        " options hstore,"
        " details jsonb,"
        " CONSTRAINT uk_users UNIQUE (name)"
        ");"
        "INSERT INTO users (name, password, validity, max_renew) VALUES"
        " ('admin', 'nimda', '(-INFINITY,INFINITY)', NULL),"
        " ('secretary', 'secret', '[2017-11-30,2018-11-30]', '1 year 2 months 3 days'),"
        " ('ceo', 'ultrasecret', '[2017-11-01,INFINITY]', NULL),"
        " ('inter', 'retni', '[2017-11-30,2017-12-31]', '1 month')"
    ))
    event_loop.run_until_complete(conn.close())

    pool = event_loop.run_until_complete(asyncpg.create_pool(
        database='sasyncpg_test',
        min_size=1,
        max_size=10,
        init=register_custom_codecs,
        loop=event_loop,
    ))

    try:
        yield pool
    finally:
        event_loop.run_until_complete(pool.close())
        event_loop.run_until_complete(adminconn.execute(
            'DROP DATABASE sasyncpg_test'))
        event_loop.run_until_complete(adminconn.close())


@pytest.fixture
def connection(event_loop, pool):
    c = event_loop.run_until_complete(pool.acquire())
    try:
        yield Connection(c)
    finally:
        event_loop.run_until_complete(pool.release(c))


users_t = sa.Table('users', sa.MetaData(),
                   sa.Column('id', sa.types.Integer, primary_key=True),
                   sa.Column('name', sa.types.String, nullable=False,
                             info=dict(label='User name')),
                   sa.Column('password', sa.types.String, nullable=False),
                   sa.Column('validity', sapg.DATERANGE, nullable=True),
                   sa.Column('max_renew', sa.types.Interval, nullable=True),
                   sa.Column('options', sapg.HSTORE, nullable=True),
                   sa.Column('details', sapg.JSONB, nullable=True))


@pytest.fixture
def users():
    return users_t
