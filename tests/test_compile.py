# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Tests for the compile() function
# :Created:   mer 21 dic 2016 12:40:11 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

from datetime import datetime

import sqlalchemy as sa
import pytest

from metapensiero.sqlalchemy.asyncpg import compile


table = sa.Table('test', sa.MetaData(),
                 sa.Column('id', sa.types.Integer),
                 sa.Column('name', sa.types.String),
                 sa.Column('gender', sa.types.String,
                           nullable=False, default='M'),
                 sa.Column('updated', sa.types.DateTime,
                           default=None, onupdate=datetime.now))


@pytest.mark.asyncio
async def test_compile_raw_sql():
    query = "SELECT id FROM test WHERE id = $1"
    sql, args = compile(query, pos_args=(1,))
    assert sql == query
    assert args == (1,)


@pytest.mark.asyncio
async def test_compile_select_simple():
    query = sa.select([table.c.id]) \
              .where(table.c.id == 1)
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "SELECT test.id FROM test WHERE test.id = $1"
    assert args == (1,)


@pytest.mark.asyncio
async def test_compile_select_bind_params():
    query = sa.select([table.c.id]) \
              .where(table.c.id == 1) \
              .where(table.c.name != sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "SELECT test.id FROM test WHERE test.id = $1 AND test.name != $2"
    assert args == (1, 'lele')


@pytest.mark.asyncio
async def test_compile_insert_simple():
    query = table.insert().values(id=1, name='rosy', gender='F')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1, $2, $3)"
    assert args == (1, 'rosy', 'F')


@pytest.mark.asyncio
async def test_compile_insert_simple_with_default():
    query = table.insert().values(id=1, name='lele')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1, $2, $3)"
    assert args == (1, 'lele', 'M')


@pytest.mark.asyncio
async def test_compile_insert_bind_params():
    query = table.insert().values(id=1, name=sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1, $2, $3)"
    assert args == (1, 'lele', 'M')


@pytest.mark.asyncio
async def test_compile_delete_simple():
    query = table.delete().where(table.c.id == 1)
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "DELETE FROM test WHERE test.id = $1"
    assert args == (1,)


@pytest.mark.asyncio
async def test_compile_delete_bind_params():
    query = table.delete().where(table.c.name == sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "DELETE FROM test WHERE test.name = $1"
    assert args == ('lele',)


@pytest.mark.asyncio
async def test_compile_update_simple():
    query = table.update().where(table.c.id == 1).values(gender='F')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "UPDATE test SET gender=$1, updated=$2 WHERE test.id = $3"
    assert args[0] == 'F'
    assert isinstance(args[1], datetime)
    assert args[2] == 1


@pytest.mark.asyncio
async def test_compile_update_simple_with_default():
    query = table.update().where(table.c.id == 1).values(name='lele')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "UPDATE test SET name=$1, updated=$2 WHERE test.id = $3"
    assert args[0] == 'lele'
    assert isinstance(args[1], datetime)
    assert args[2] == 1


@pytest.mark.asyncio
async def test_compile_update_bind_params():
    query = table.update().where(table.c.id == 1).values(name=sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "UPDATE test SET name=$1, updated=$2 WHERE test.id = $3"
    assert args[0] == 'lele'
    assert isinstance(args[1], datetime)
    assert args[2] == 1
