# -*- coding: utf-8 -*-
# :Project:   arstecnica.ytefas.asyncpg -- Asyncpg proxies
# :Created:   mar 20 dic 2016 17:17:37 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016 Arstecnica s.r.l.
#

import logging

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import Selectable

from metapensiero.sqlalchemy.proxy.core import ProxiedQuery
from metapensiero.sqlalchemy.proxy.utils import apply_sorters

from . import fetchall, scalar


log = logging.getLogger(__name__)


class AsyncpgProxiedQuery(ProxiedQuery):
    """An asyncpg variant of the ``ProxiedQuery``."""

    async def getCount(self, session, query):
        "Async reimplementation of :meth:`ProxiedQuery.getCount`."

        pivot = next(query.inner_columns)
        simple = query.with_only_columns([pivot])
        tquery = select([func.count()], from_obj=simple.alias('cnt'))
        count = await scalar(session, tquery, named_args=self.params)
        return count

    async def getResult(self, session, query, asdict):
        "Async reimplementation of :meth:`ProxiedQuery.getResult`."

        if isinstance(query, Selectable):
            rows = await fetchall(session, query, named_args=self.params)
            if asdict:
                fn2key = dict((c.name, c.key) for c in self.getColumns(query))
                result = [dict((fn2key[fn], r[fn]) for fn in fn2key)
                          for r in rows]
            else:
                result = rows
        else:
            result = None
        return result

    async def __call__(self, session, *conditions, **args):
        "Async reimplementation of superclass' ``__call__()``."

        (query, result, asdict,
         resultslot, successslot, messageslot, countslot, metadataslot,
         sort, dir,
         start, limit) = self.prepareQueryFromConditionsAndArgs(session,
                                                                conditions,
                                                                args)

        try:
            if limit != 0:
                if countslot:
                    count = await self.getCount(session, query)
                    result[countslot] = count

                if resultslot:
                    if sort:
                        query = apply_sorters(query, sort, dir)
                    if start:
                        query = query.offset(start)
                    if limit:
                        query = query.limit(limit)
                    rows = await self.getResult(session, query, asdict)
                    result[resultslot] = rows

            if metadataslot:
                result[metadataslot] = self.getMetadata(session, query,
                                                        countslot,
                                                        resultslot,
                                                        successslot)

            result[successslot] = True
            result[messageslot] = 'Ok'
        except SQLAlchemyError as e: # pragma: nocover
            log.error("Error executing %s: %s", query, e)
            raise
        except: # pragma: nocover
            log.exception("Unhandled exception executing %s", query)
            raise

        if resultslot is True:
            return result[resultslot]
        else:
            return result
