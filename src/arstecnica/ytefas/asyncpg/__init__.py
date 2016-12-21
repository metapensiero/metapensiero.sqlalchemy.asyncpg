# -*- coding: utf-8 -*-
# :Project:   arstecnica.ytefas.asyncpg -- SQLAlchemy adaptor for asyncpg
# :Created:   Tue 20 Dec 2016 21:17:12 CET
# :Author:    Lele Gaifax <lele@arstecnica.it>
# :License:   No license
# :Copyright: © 2016 Arstecnica s.r.l.
#

from .dialect import PGDialect_asyncpg


def compile(stmt, pos_args=None, named_args=None, _d=PGDialect_asyncpg()):
    """Compile an SQLAlchemy core statement and extract its parameters.

    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :return: a tuple of two items, a string with the final SQL instruction and
             a sequence of positional arguments, if any

    If `stmt` is a plain string, it is returned as is together with a tuple of
    the positional arguments specified in `pos_args`, possibly empty.

    When `stmt` is a SQLAlchemy :class:`Executable
    <sqlalchemy.sql.base.Executable>` it is compiled to its raw SQL using a
    custom SA :class:`dialect <.PGDialect_asyncpg>` to produce a syntax
    compatible with asyncpg (in particular, using ``$1``, ``$2`` and so as the
    parametric placeholders) and it's parameters collected in a tuple, picking
    named parameters (when using :func:`sqlalchemy.bindparam` for example)
    from the `named_args` dictionary.

    The result is suitable to be passed to the various methods of asyncpg's
    connection for execution.
    """

    if isinstance(stmt, str):
        return stmt, tuple(pos_args) if pos_args is None else ()
    else:
        compiled = stmt.compile(dialect=_d)
        params = compiled.construct_params(named_args)
        return compiled.string, tuple(params[p] for p in compiled.positiontup)


async def execute(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    """Execute the given statement on a asyncpg connection.

    :param apgconn: an asyncpg :class:`Connection
                    <asyncpg.connection.Connection>` instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `execute()`__ keyword argument
    :return: a string with the status of the last instruction

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters.

    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.execute
    """

    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.execute(sql, *args, **kwargs)


async def prepare(apgconn, stmt, **kwargs):
    """Create a `prepared statement`__.

    :param apgconn: an asyncpg :class:`Connection
                    <asyncpg.connection.Connection>` instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param \*\*kwargs: any valid `prepare()`__ keyword argument
    :return: a string with the status of the last instruction

    __ https://magicstack.github.io/asyncpg/current/api/index.html#prepared-statements
    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.prepare
    """

    sql, args = compile(stmt)
    return await apgconn.prepare(sql, **kwargs)


async def fetchall(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    """Execute the given statement on a asyncpg connection and return resulting records.

    :param apgconn: an asyncpg :class:`Connection
                    <asyncpg.connection.Connection>` instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetch()`__ keyword argument
    :return: a list of `Record`__ instances

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. It's whole resultset
    is finally returned.

    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.fetch
    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.Record
    """

    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetch(sql, *args, **kwargs)


async def fetchone(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    """Execute the given statement on a asyncpg connection and return the first
    row.

    :param apgconn: an asyncpg :class:`Connection
                    <asyncpg.connection.Connection>` instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetchrow()`__ keyword argument
    :return: either ``None`` or a `Record`__ instance

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. The first row of it's
    result is finally returned.

    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.fetchrow
    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.Record
    """

    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetchrow(sql, *args, **kwargs)


async def scalar(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    """Execute the given statement on a asyncpg connection and return a single
    column of the first row.

    :param apgconn: an asyncpg :class:`Connection
                    <asyncpg.connection.Connection>` instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetchval()`__ keyword argument
    :return: a list of `Record`__ instances

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. A particular column
    of the first row (specified positionally by the keyword `column`, by
    default the first) is finally returned.

    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.fetchval
    __ https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.Record
    """

    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetchval(sql, *args, **kwargs)
