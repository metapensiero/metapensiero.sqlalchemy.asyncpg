# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Class-hic interface
# :Created:   ven 24 mar 2017 08:15:50 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2017 Lele Gaifax
#

from .funcs import compile, execute, fetchall, fetchone, prepare, scalar


class Connection:
    """Class wrapper to low level functions.

    :param apgconnection: an AsyncPG Connection__ instance

    __ https://magicstack.github.io/asyncpg/current/api/index.html#connection
    """

    __slots__ = ('apgc',)

    def __init__(self, apgconnection):
        self.apgc = apgconnection

    def cursor(self, stmt, pos_args=None, named_args=None, **kwargs):
        """Return a `Cursor`__ instance on given `stmt`.

        :param stmt: any SQLAlchemy core statement or a raw SQL instruction
        :param pos_args: a possibly empty sequence of positional arguments
        :param named_args: a possibly empty mapping of named arguments

        __ https://magicstack.github.io/asyncpg/current/api/index.html\
           #asyncpg.connection.Connection.cursor
        """

        sql, args = compile(stmt, pos_args, named_args)
        return self.apgc.cursor(sql, *args, **kwargs)

    async def execute(self, stmt, pos_args=None, named_args=None,
                      expected_result=None):
        """Invoke :func:`~.funcs.execute()` forwarding the arguments,
        returning its result.
        """

        return await execute(self.apgc, stmt, pos_args, named_args,
                             expected_result=expected_result)

    async def fetchall(self, stmt, pos_args=None, named_args=None):
        """Invoke :func:`~.funcs.fetchall()` forwarding the arguments,
        returning its result.
        """

        return await fetchall(self.apgc, stmt, pos_args, named_args)

    async def fetchone(self, stmt, pos_args=None, named_args=None):
        """Invoke :func:`~.funcs.fetchone()` forwarding the arguments,
        returning its result.
        """

        return await fetchone(self.apgc, stmt, pos_args, named_args)

    async def prepare(self, stmt, **kwargs):
        """Invoke :func:`~.funcs.prepare()` forwarding the arguments,
        returning its result.
        """

        return await prepare(self.apgc, stmt, **kwargs)

    async def scalar(self, stmt, pos_args=None, named_args=None):
        """Invoke :func:`~.funcs.scalar()` forwarding the arguments,
        returning its result.
        """

        return await scalar(self.apgc, stmt, pos_args, named_args)

    def transaction(self):
        """Start an explicit transaction and return it.

        Typically used in an ``async with`` statement:

        .. code-block:: python

           async with dbc.transaction():
               dbc.execute(stmt1)
               dbc.execute(stmt2)
        """

        return self.apgc.transaction()
