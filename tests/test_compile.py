# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Tests for the compile() function
# :Created:   mer 21 dic 2016 12:40:11 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

from datetime import datetime

import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy.asyncpg import compile


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


table = sa.Table('test', sa.MetaData(),
                 sa.Column('id', sa.types.Integer),
                 sa.Column('name', sa.types.String),
                 sa.Column('gender', sa.types.String,
                           nullable=False, default='M'),
                 sa.Column('updated', sa.types.DateTime,
                           default=None, onupdate=datetime.now))


async def test_compile_raw_sql():
    query = "SELECT id FROM test WHERE id = $1::INTEGER"
    sql, args = compile(query, pos_args=(1,))
    assert sql == query
    assert args == (1,)


async def test_compile_select_simple():
    query = sa.select([table.c.id]) \
              .where(table.c.id == 1)
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "SELECT test.id FROM test WHERE test.id = $1::INTEGER"
    assert args == (1,)


async def test_compile_select_bind_params():
    query = sa.select([table.c.id]) \
              .where(table.c.id == 1) \
              .where(table.c.name != sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "SELECT test.id FROM test WHERE test.id = $1::INTEGER AND test.name != $2::VARCHAR"
    assert args == (1, 'lele')


async def test_compile_select_func():
    tc = table.c
    query = sa.select([sa.func.concat_ws('_', tc.name, tc.gender)]) \
              .where(table.c.id == 1)
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "SELECT concat_ws($1::VARCHAR, test.name, test.gender) AS concat_ws_1"\
        " FROM test WHERE test.id = $2::INTEGER"
    assert args == ('_', 1)


async def test_compile_insert_simple():
    query = table.insert().values(id=1, name='rosy', gender='F')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1::INTEGER, $2::VARCHAR, $3::VARCHAR)"
    assert args == (1, 'rosy', 'F')


async def test_compile_insert_simple_with_default():
    query = table.insert().values(id=1, name='lele')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1::INTEGER, $2::VARCHAR, $3::VARCHAR)"
    assert args == (1, 'lele', 'M')


async def test_compile_insert_bind_params():
    query = table.insert().values(id=1, name=sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "INSERT INTO test (id, name, gender) VALUES ($1::INTEGER, $2::VARCHAR, $3::VARCHAR)"
    assert args == (1, 'lele', 'M')


async def test_compile_delete_simple():
    query = table.delete().where(table.c.id == 1)
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "DELETE FROM test WHERE test.id = $1::INTEGER"
    assert args == (1,)


async def test_compile_delete_bind_params():
    query = table.delete().where(table.c.name == sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "DELETE FROM test WHERE test.name = $1::VARCHAR"
    assert args == ('lele',)


async def test_compile_update_simple():
    query = table.update().where(table.c.id == 1).values(gender='F')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "UPDATE test SET gender=$1::VARCHAR, updated=$2::TIMESTAMP WITHOUT TIME ZONE WHERE test.id = $3::INTEGER"
    assert args[0] == 'F'
    assert isinstance(args[1], datetime)
    assert args[2] == 1


async def test_compile_update_simple_with_default():
    query = table.update().where(table.c.id == 1).values(name='lele')
    sql, args = compile(query)
    assert sql.replace('\n', '') == \
        "UPDATE test SET name=$1::VARCHAR, updated=$2::TIMESTAMP WITHOUT TIME ZONE WHERE test.id = $3::INTEGER"
    assert args[0] == 'lele'
    assert isinstance(args[1], datetime)
    assert args[2] == 1


async def test_compile_update_bind_params():
    query = table.update().where(table.c.id == 1).values(name=sa.sql.bindparam('some_name'))
    with pytest.raises(sa.exc.InvalidRequestError):
        compile(query)
    sql, args = compile(query, named_args={'some_name': 'lele'})
    assert sql.replace('\n', '') == \
        "UPDATE test SET name=$1::VARCHAR, updated=$2::TIMESTAMP WITHOUT TIME ZONE WHERE test.id = $3::INTEGER"
    assert args[0] == 'lele'
    assert isinstance(args[1], datetime)
    assert args[2] == 1


async def test_compile_unknown_type():
    query = sa.select([sa.func.foo(sa.bindparam('bar'))])
    sql, args = compile(query, named_args={'bar': 1})
    assert sql.replace('\n', '') == 'SELECT foo($1) AS foo_1'
    assert args[0] == 1
