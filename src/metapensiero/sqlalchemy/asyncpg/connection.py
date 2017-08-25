# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Class-hic interface
# :Created:   ven 24 mar 2017 08:15:50 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2017 Arstecnica s.r.l.
#

from .funcs import execute, fetchall, fetchone, prepare, scalar


class UnexpectedResultError(RuntimeError):
    "Exception raised when the execution result does not match."


class Connection:
    """Class wrapper to low level functions.

    :param apgconnection: an AsyncPG Connection__ instance

    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    """

    __slots__ = ('apgc',)

    def __init__(self, apgconnection):
        self.apgc = apgconnection

    async def execute(self, stmt, pos_args=None, named_args=None,
                      expected_result=None):
        """Invoke :func:`~.funcs.execute()` forwarding the arguments, with
        the exception of `expected_result`, returning its result.

        If `expected_result` is not ``None``, then it must match the value
        returned by the underlying function, otherwise an
        :Class:`UnexpectedResultError` is raised.
        """

        result = await execute(self.apgc, stmt, pos_args, named_args)
        if expected_result is not None and expected_result != result:
            raise UnexpectedResultError(f'Expected "{expected_result}"'
                                        f' got "{result}"')
        return result

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
