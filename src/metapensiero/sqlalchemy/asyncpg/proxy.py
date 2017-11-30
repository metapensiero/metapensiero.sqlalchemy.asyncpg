# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Variant of metapensiero.sqlalchemy.proxy
# :Created:   mer 11 gen 2017 10:16:27 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2017 Lele Gaifax
#

from logging import getLogger

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import Selectable

from metapensiero.sqlalchemy.proxy.core import ProxiedQuery
from metapensiero.sqlalchemy.proxy.sorters import apply_sorters


logger = getLogger(__name__)


class AsyncpgProxiedQuery(ProxiedQuery):
    """
    An asyncpg variant of the `ProxiedQuery`__.

    __ http://metapensierosqlalchemyproxy.readthedocs.io/en/latest/\
       core.html#metapensiero.sqlalchemy.proxy.core.ProxiedQuery
    """

    async def getCount(self, dbconn, query):
        """Async reimplementation of superclass' ``getCount()``.

        :param dbconn: an object carrying a method ``scalar()``, based on
                       :func:`.funcs.scalar`
        :param query: a SQLAlchemy core statement
        :return: an integer, the count of matching records
        """

        if not isinstance(query, Selectable):
            raise ValueError('Query must be a Selectable')

        pivot = next(query.inner_columns)
        simple = query.with_only_columns([pivot])
        tquery = select([func.count()], from_obj=simple.alias('cnt'))
        count = await dbconn.scalar(tquery, named_args=self.params)
        return count

    async def getResult(self, dbconn, query, asdict):
        """Async reimplementation of superclass' ``getResult()``.

        :param dbconn: an object carrying a method ``fetchall()``, based on
                       :func:`.funcs.fetchall`
        :param query: a SQLAlchemy core statement
        :type asdict: bool
        :param asdict: whether to return plain dictionaries instead of
                       asyncpg's native ``Record``\ s
        :return: an integer, the count of matching records
        """

        if isinstance(query, Selectable):
            rows = await dbconn.fetchall(query, named_args=self.params)
            if asdict:
                fn2key = dict((c.name, c.key) for c in self.getColumns(query))
                result = [dict((fn2key[fn], r[fn]) for fn in fn2key)
                          for r in rows]
            else:
                result = rows
        else:
            logger.warning('Requested result of a non-Selectable statement')
            result = None
        return result

    async def __call__(self, dbconn, *conditions, **args):
        "Async reimplementation of superclass' ``__call__()``."

        (query, result, asdict,
         resultslot, successslot, messageslot, countslot, metadataslot,
         start, limit) = self.prepareQueryFromConditionsAndArgs(dbconn,
                                                                conditions,
                                                                args)

        try:
            if limit != 0:
                if countslot:
                    count = await self.getCount(dbconn, query)
                    result[countslot] = count

                if resultslot:
                    query = apply_sorters(query, args)
                    if start:
                        query = query.offset(start)
                    if limit:
                        query = query.limit(limit)
                    rows = await self.getResult(dbconn, query, asdict)
                    result[resultslot] = rows

            if metadataslot:
                result[metadataslot] = self.getMetadata(query,
                                                        countslot,
                                                        resultslot,
                                                        successslot)

            result[successslot] = True
            result[messageslot] = 'Ok'
        except SQLAlchemyError as e: # pragma: nocover
            logger.error("Error executing %s: %s", query, e)
            raise
        except: # pragma: nocover
            logger.exception("Unhandled exception executing %s", query)
            raise

        if resultslot is True:
            return result[resultslot]
        else:
            return result
