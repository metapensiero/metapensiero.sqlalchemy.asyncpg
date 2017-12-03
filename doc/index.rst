.. -*- coding: utf-8 -*-
.. :Project:   metapensiero.sqlalchemy.asyncpg -- Documentation
.. :Created:   mer 21 dic 2016 11:49:21 CET
.. :Author:    Lele Gaifax <lele@metapensiero.it>
.. :License:   GNU General Public License version 3 or later
.. :Copyright: © 2016, 2017 Lele Gaifax
..

=================================
 metapensiero.sqlalchemy.asyncpg
=================================

SQLAlchemy adaptor for asyncpg
==============================

 :author: Lele Gaifax
 :contact: lele@metapensiero.it
 :license: GNU General Public License version 3 or later

This is a Python 3 package, spin-off from the proprietary ``Ytefas`` application, that
implements the ability of executing SQLAlchemy core statements through asyncpg__, in a
cleaner way than asyncpgsa__ (I'm biased, of course 😜): the main difference is that it
uses explicitly typed parameter placeholders, to avoid `the problem`__ that initially
prompted me to write this package.

It emits ``DEBUG`` logs with `prettified`__ SQL statements, with *parameters* resolved to
the actual *values*, and supplies an asyncpg variant of `metapensiero.sqlalchemy.proxy`__
\ 's ProxiedQuery__.


__ https://pypi.python.org/pypi/asyncpg
__ https://pypi.python.org/pypi/asyncpgsa
__ https://github.com/MagicStack/asyncpg/issues/32
__ https://pypi.python.org/pypi/metapensiero.sqlalchemy.proxy
__ http://metapensierosqlalchemyproxy.readthedocs.io/en/latest/\
   core.html#metapensiero.sqlalchemy.proxy.core.ProxiedQuery
__ http://pg-query.readthedocs.io/en/latest/


Typical usage
-------------

The following script:

.. code-block:: python

  import asyncio
  from datetime import date
  import logging

  import sqlalchemy as sa
  from asyncpg import create_pool
  from asyncpg.types import Range
  from metapensiero.sqlalchemy import asyncpg as sasyncpg


  async def dml_tests(connection):
      sasyncpg_test = sa.Table('sasyncpg_test', sa.MetaData(),
                               sa.Column('id', sa.types.Integer, primary_key=True),
                               sa.Column('value', sa.types.Text),
                               sa.Column('period', sa.dialects.postgresql.DATERANGE))

      value = 'First test'
      insert_stmt = sasyncpg_test.insert().values(id=1, value=value)
      await connection.execute(insert_stmt)

      new_value = 'Second test'
      update_stmt = (sasyncpg_test.update()
                     .values(value=new_value,
                             period=Range(date(2016, 2, 1), date(2016, 3, 1)))
                     .where(sasyncpg_test.c.id == 1))
      await connection.execute(update_stmt)

      select_stmt = (sa.select([sasyncpg_test.c.value])
                     .where(sasyncpg_test.c.id == sa.bindparam('id')))
      for row in await connection.fetchall(select_stmt, named_args={'id': 1}):
          print('Row:', row)

      single_row = (sa.select([sasyncpg_test])
                    .where(sasyncpg_test.c.period.contains(date(2016, 2, 15))))
      print('Row:', await connection.fetchone(single_row))


  async def run(loop):
      pool = await create_pool(database="test", loop=loop)

      async with pool.acquire() as apgc:
          connection = sasyncpg.Connection(apgc)

          query = sa.select([sa.func.version()])
          result = await connection.scalar(query)
          print("PostgreSQL version:", result)

          await connection.execute('create table sasyncpg_test ('
                                   ' id integer not null primary key,'
                                   ' value text,'
                                   ' period daterange)')

          try:
              await dml_tests(connection)
          finally:
              await connection.execute('DROP TABLE sasyncpg_test')


  def main():
      loop = asyncio.get_event_loop()
      loop.run_until_complete(run(loop))


  if __name__ == '__main__':
      logging.basicConfig(level=logging.DEBUG)
      main()

produces something like::

  DEBUG:asyncio:Using selector: EpollSelector
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetching scalar in transaction 9ddb60:
      SELECT version() AS version_1
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetched value in 869 µsec
  PostgreSQL version: PostgreSQL 9.6.6 on x86_64-pc-linux-gnu, compiled by gcc (Debian 7.2.0-12) 7.2.1 20171025, 64-bit
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Executing in transaction 9ddb60:
      CREATE TABLE sasyncpg_test (
          id integer NOT NULL PRIMARY KEY, value text, period daterange
      )
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Execution took 97.1 msec
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Executing in transaction 9ddb60:
      INSERT INTO sasyncpg_test (id, value)
      VALUES (1::integer, 'First test'::text)
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Execution took 1.26 msec
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Executing in transaction 9ddb60:
      UPDATE sasyncpg_test
      SET value = 'Second test'::text, period = '[2016-02-01,2016-03-01)'::daterange
      WHERE sasyncpg_test.id = 1::integer
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Execution took 14.1 msec
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetching rows in transaction 9ddb60:
      SELECT sasyncpg_test.value
      FROM sasyncpg_test
      WHERE sasyncpg_test.id = 1::integer
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetched 1 records in 909 µsec
  Row: <Record value='Second test'>
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetching row in transaction 9ddb60:
      SELECT sasyncpg_test.id, sasyncpg_test.value, sasyncpg_test.period
      FROM sasyncpg_test
      WHERE sasyncpg_test.period @> datetime.date(2016, 2, 15)::date
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Fetched one record in 951 µsec
  Row: <Record id=1 value='Second test' period=<Range [datetime.date(2016, 2, 1), datetime.date(2016, 3, 1))>>
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Executing in transaction 9ddb60:
      DROP TABLE sasyncpg_test RESTRICT
  DEBUG:metapensiero.sqlalchemy.asyncpg.funcs:Execution took 12 msec

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   dialect
   funcs
   connection
   types
   proxy

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
