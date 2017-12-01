# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Adaptor functions
# :Created:   Tue 20 Dec 2016 21:17:12 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

import logging
from time import perf_counter

from .dialect import PGDialect_asyncpg


SLOW_QUERY_THRESHOLD = 2.0
"Warn about SQL statements that take more than this amount of seconds."

logger = logging.getLogger(__name__)


def _honor_column_default(params, column, default, key_getter):
    key = key_getter(column)
    val = params.get(key)
    if val is None:
        if not default.is_sequence and default.is_scalar:
            val = default.arg
        else:
            if default.is_callable:
                val = default.arg(None)

        if val is not None:
            params[key] = val


def _format_arg(arg):
    from uuid import UUID
    from asyncpg.types import Range

    if isinstance(arg, UUID):
        arg = str(arg)
    elif isinstance(arg, Range):
        if arg.isempty:
            arg = 'empty range'
        else:
            lb = '[' if arg.lower_inc else '('
            ub = ']' if arg.upper_inc else ')'
            v = arg.lower
            lv = '' if v is None else v.isoformat()
            v = arg.upper
            uv = '' if v is None else v.isoformat()
            arg = '%s%s,%s%s' % (lb, lv, uv, ub)

    rarg = repr(arg)

    if len(rarg) > 50:
        rarg = rarg[:20] + ' … ' + rarg[-20:]

    return rarg


def _format_elapsed_time(et):
    precision = 3
    units = {"nsec": 1e-9, "µsec": 1e-6, "msec": 1e-3, "sec": 1.0}
    scales = [(scale, unit) for unit, scale in units.items()]
    scales.sort(reverse=True)
    for scale, unit in scales:
        if et >= scale:
            break
    return "%.*g %s" % (precision, et / scale, unit)


def _log_sql_statement(connection, operation, sql, args, logf=logger.debug):
    from textwrap import indent
    from asyncpg.pool import PoolConnectionProxy
    from pg_query import prettify
    from pg_query.printer import get_printer_for_node_tag, node_printer
    import pg_query.printers.dml  # noqa

    try:
        orig_paramref_printer = get_printer_for_node_tag(None, 'ParamRef')
        try:
            @node_printer('ParamRef', override=True)
            def replace_param_ref(node, output):
                if 0 < node.number.value <= len(args):
                    output.write(_format_arg(args[node.number.value - 1]))
                else:
                    orig_paramref_printer(node, output)
            newsql = prettify(sql, compact_lists_margin=80, safety_belt=False)
        finally:
            node_printer('ParamRef', override=True)(orig_paramref_printer)
    except Exception as e:
        logger.error('Something wrong with SQL prettification: %s\n'
                     '  arguments: %r', e, [_format_arg(a) for a in args])
    else:
        sql = newsql

    sql = indent(sql, '    ')

    if isinstance(connection, PoolConnectionProxy):
        tx = connection._con._top_xact
    else:
        tx = connection._top_xact

    logf('%s in transaction %0x:\n%s', operation, id(tx), sql)


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
    compatible with asyncpg (in particular, using ``$1``, ``$2`` and so on as
    the parametric placeholders) and it's parameters collected in a tuple,
    picking named parameters (when using :func:`sqlalchemy.bindparam` for
    example) from the `named_args` dictionary.

    The result is suitable to be passed to the various methods of asyncpg's
    connection for execution.
    """

    if isinstance(stmt, str):
        return stmt, tuple(pos_args) if pos_args is not None else ()
    else:
        compiled = stmt.compile(dialect=_d)
        params = compiled.construct_params(named_args)

        # Honor column's default or unupdate setting: following code adapted
        # from SA's DefaultExecutionContext._process_executesingle_defaults()
        # logic

        key_getter = compiled._key_getters_for_crud_column[2]
        for c in compiled.insert_prefetch:
            default = c.default
            if default is not None:
                _honor_column_default(params, c, default, key_getter)

        for c in compiled.update_prefetch:
            default = c.onupdate
            if default is not None:
                _honor_column_default(params, c, default, key_getter)

        return compiled.string, tuple(params[p] for p in compiled.positiontup)


class UnexpectedResultError(RuntimeError):
    "Exception raised when the execution result does not match."

    def __init__(self, got, expected):
        super().__init__(f'Expected {expected!r}, got {got!r}')
        self.expected = expected
        self.got = got


async def execute(apgconn, stmt, pos_args=None, named_args=None,
                  expected_result=None,
                  warn_slow_query_threshold=SLOW_QUERY_THRESHOLD, **kwargs):
    r"""Execute the given statement on a asyncpg connection.

    :param apgconn: an ASyncPG Connection__ instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param expected_result: the expected result of the execution, if any
    :param \*\*kwargs: any valid `execute()`__ keyword argument
    :return: a string with the status of the last instruction

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters.

    If `expected_result` is not ``None``, then it must match the value
    returned by the underlying function, otherwise an
    :class:`UnexpectedResultError` is raised.

    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.connection.Connection.execute
    """

    sql, args = compile(stmt, pos_args, named_args)
    debug = logger.isEnabledFor(logging.DEBUG)
    if debug:
        _log_sql_statement(apgconn, 'Executing', sql, args)
    t0 = perf_counter()
    try:
        result = await apgconn.execute(sql, *args, **kwargs)
    except Exception as e:
        if not debug:
            _log_sql_statement(apgconn, 'Error "%s" executing' % e,
                               sql, args, logf=logger.error)
        raise
    else:
        if expected_result is not None and result != expected_result:
            if not debug:
                _log_sql_statement(apgconn, 'Unexpected result executing',
                                   sql, args, logf=logger.error)
            raise UnexpectedResultError(result, expected_result)

    elapsed = perf_counter() - t0
    if debug or elapsed > warn_slow_query_threshold:
        if debug:
            logf = (logger.debug if elapsed < warn_slow_query_threshold
                    else logger.warning)
            logf('Execution took %s', _format_elapsed_time(elapsed))
        else:
            _log_sql_statement(apgconn, 'Suspiciously SLOW query', sql, args,
                               logf=logger.warning)
    return result


async def prepare(apgconn, stmt, **kwargs):
    r"""Create a `prepared statement`__.

    :param apgconn: an asyncpg Connection__ instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param \*\*kwargs: any valid `prepare()`__ keyword argument
    :return: a string with the status of the last instruction

    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#prepared-statements
    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.connection.Connection.prepare
    """

    sql, args = compile(stmt)
    debug = logger.isEnabledFor(logging.DEBUG)
    if debug:
        _log_sql_statement(apgconn, 'Preparing', sql, args)
        t0 = perf_counter()
    try:
        result = await apgconn.prepare(sql, **kwargs)
    except Exception as e:
        if not debug:
            _log_sql_statement(apgconn, 'Error "%s" preparing' % e,
                               sql, args, logf=logger.error)
        raise
    if debug:
        t1 = perf_counter()
        logger.debug('Preparation took %s', _format_elapsed_time(t1 - t0))
    return result


async def fetchall(apgconn, stmt, pos_args=None, named_args=None,
                   warn_slow_query_threshold=SLOW_QUERY_THRESHOLD, **kwargs):
    r"""Execute the given statement on a asyncpg connection and return
    resulting records.

    :param apgconn: an asyncpg Connection__ instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetch()`__ keyword argument
    :return: a list of `Record`__ instances

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. It's whole resultset
    is finally returned.

    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.connection.Connection.fetch
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.Record
    """

    sql, args = compile(stmt, pos_args, named_args)
    debug = logger.isEnabledFor(logging.DEBUG)
    if debug:
        _log_sql_statement(apgconn, 'Fetching rows', sql, args)
    t0 = perf_counter()
    try:
        result = await apgconn.fetch(sql, *args, **kwargs)
    except Exception as e:
        if not debug:
            _log_sql_statement(apgconn, 'Error "%s" fetching rows' % e,
                               sql, args, logf=logger.error)
        raise
    elapsed = perf_counter() - t0
    if debug or elapsed > warn_slow_query_threshold:
        if debug:
            logf = (logger.debug if elapsed < warn_slow_query_threshold
                    else logger.warning)
            logf('Fetched %d records in %s',
                 len(result), _format_elapsed_time(elapsed))
        else:
            _log_sql_statement(apgconn, 'Suspiciously SLOW query', sql, args,
                               logf=logger.warning)
    return result


async def fetchone(apgconn, stmt, pos_args=None, named_args=None,
                   warn_slow_query_threshold=SLOW_QUERY_THRESHOLD, **kwargs):
    r"""Execute the given statement on a asyncpg connection and return the
    first row.

    :param apgconn: an asyncpg Connection__ instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetchrow()`__ keyword argument
    :return: either ``None`` or a `Record`__ instance

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. The first row of it's
    result is finally returned, or ``None`` if the result is empty.

    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.connection.Connection.fetchrow
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.Record
    """

    sql, args = compile(stmt, pos_args, named_args)
    debug = logger.isEnabledFor(logging.DEBUG)
    if debug:
        _log_sql_statement(apgconn, 'Fetching row', sql, args)
    t0 = perf_counter()
    try:
        result = await apgconn.fetchrow(sql, *args, **kwargs)
    except Exception as e:
        if not debug:
            _log_sql_statement(apgconn, 'Error "%s" fetching row' % e,
                               sql, args, logf=logger.error)
        raise
    elapsed = perf_counter() - t0
    if debug or elapsed > warn_slow_query_threshold:
        if debug:
            logf = (logger.debug if elapsed < warn_slow_query_threshold
                    else logger.warning)
            logf('Fetched %s in %s',
                 'no records' if result is None else 'one record',
                 _format_elapsed_time(elapsed))
        else:
            _log_sql_statement(apgconn, 'Suspiciously SLOW query', sql, args,
                               logf=logger.warning)
    return result


async def scalar(apgconn, stmt, pos_args=None, named_args=None,
                 warn_slow_query_threshold=SLOW_QUERY_THRESHOLD, **kwargs):
    r"""Execute the given statement on a asyncpg connection and return a
    single column of the first row.

    :param apgconn: an asyncpg Connection__ instance
    :param stmt: any SQLAlchemy core statement or a raw SQL instruction
    :param pos_args: a possibly empty sequence of positional arguments
    :param named_args: a possibly empty mapping of named arguments
    :param \*\*kwargs: any valid `fetchval()`__ keyword argument
    :return: the value of the specified column of the first record, or
             ``None`` if the query does not return any rows

    The `stmt` is first compiled with :func:`.compile` and then executed on
    the `apgconn` connection with the needed parameters. A particular column
    of the first row (specified positionally with the keyword `column`, by
    default ``0`` to mean the first) is finally returned.

    .. warning:: There is no way to distinguish between a *non existing
                 record* and a ``None`` value in the requested column of the
                 first row. If you need to discriminate the case, use
                 :meth:`fetchone()` instead:

                 .. code-block:: python

                    rec = conn.fetchone(stmt, pos_args)
                    if rec is not None:
                        val = rec[column]
                        ...

    __ https://magicstack.github.io/asyncpg/devel/api/index.html#connection
    __ https://magicstack.github.io/asyncpg/devel/api/\
       index.html#asyncpg.connection.Connection.fetchval
    """

    sql, args = compile(stmt, pos_args, named_args)
    debug = logger.isEnabledFor(logging.DEBUG)
    if debug:
        _log_sql_statement(apgconn, 'Fetching scalar', sql, args)
    t0 = perf_counter()
    try:
        result = await apgconn.fetchval(sql, *args, **kwargs)
    except Exception as e:
        if not debug:
            _log_sql_statement(apgconn, 'Error "%s" fetching scalar' % e,
                               sql, args, logf=logger.error)
        raise
    elapsed = perf_counter() - t0
    if debug or elapsed > warn_slow_query_threshold:
        if debug:
            logf = (logger.debug if elapsed < warn_slow_query_threshold
                    else logger.warning)
            logf('Fetched value in %s', _format_elapsed_time(elapsed))
        else:
            _log_sql_statement(apgconn, 'Suspiciously SLOW query', sql, args,
                               logf=logger.warning)
    return result
