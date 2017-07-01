# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Class-hic interface
# :Created:   ven 24 mar 2017 08:15:50 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2017 Arstecnica s.r.l.
#

from .funcs import execute, fetchall, fetchone, prepare, scalar


class Connection:
    __slots__ = ('apgc',)

    def __init__(self, apgconnection):
        self.apgc = apgconnection

    async def execute(self, stmt, pos_args=None, named_args=None):
        return await execute(self.apgc, stmt, pos_args, named_args)

    async def fetchall(self, stmt, pos_args=None, named_args=None):
        return await fetchall(self.apgc, stmt, pos_args, named_args)

    async def fetchone(self, stmt, pos_args=None, named_args=None):
        return await fetchone(self.apgc, stmt, pos_args, named_args)

    async def prepare(self, stmt, **kwargs):
        return await prepare(self.apgc, stmt, **kwargs)

    async def scalar(self, stmt, pos_args=None, named_args=None):
        return await scalar(self.apgc, stmt, pos_args, named_args)

    def transaction(self):
        return self.apgc.transaction()
