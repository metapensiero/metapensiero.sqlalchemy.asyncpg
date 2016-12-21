# -*- coding: utf-8 -*-
# :Project:   arstecnica.ytefas.asyncpg -- SQLAlchemy adaptor for asyncpg
# :Created:   Tue 20 Dec 2016 21:17:12 CET
# :Author:    Lele Gaifax <lele@arstecnica.it>
# :License:   No license
# :Copyright: © 2016 Arstecnica s.r.l.
#

from .dialect import PGDialect_asyncpg


def compile(stmt, pos_args=None, named_args=None, _d=PGDialect_asyncpg()):
    if isinstance(stmt, str):
        return stmt, pos_args or ()
    else:
        compiled = stmt.compile(dialect=_d)
        params = compiled.construct_params(named_args)
        return compiled.string, tuple(params[p] for p in compiled.positiontup)


async def execute(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.execute(sql, *args, **kwargs)


async def prepare(apgconn, stmt, **kwargs):
    sql, args = compile(stmt)
    return await apgconn.prepare(sql, **kwargs)


async def fetchall(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetch(sql, *args, **kwargs)


async def fetchone(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetchrow(sql, *args, **kwargs)


async def scalar(apgconn, stmt, pos_args=None, named_args=None, **kwargs):
    sql, args = compile(stmt, pos_args, named_args)
    return await apgconn.fetchval(sql, *args, **kwargs)
