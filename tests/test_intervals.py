# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Tests on interval data type
# :Created:   mar 06 giu 2017 11:41:49 CEST
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2017 Arstecnica s.r.l.
#

import pytest
import sqlalchemy as sa

from metapensiero.sqlalchemy import asyncpg

from arstecnica.ytefas.model.tables.risk import schedules


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def test_schedule_repeat_after(pool):
    q = (sa.select([schedules.c.id, schedules.c.repeat_after])
         .where(schedules.c.name == 'Revisione pulegge'))

    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            id, repeat_after = await asyncpg.fetchone(conn, q)
            assert repeat_after.months == 12*5

            u = (schedules.update()
                 .where(schedules.c.id == id)
                 .values(repeat_after=asyncpg.Interval(1, 2, 3)))
            assert await asyncpg.execute(conn, u) == 'UPDATE 1'

            q = (sa.select([schedules.c.repeat_after])
                 .where(schedules.c.id == id))
            result = await asyncpg.scalar(conn, q)

            assert result == asyncpg.Interval(1, 2, 3)
            assert result == (1, 2, 3)
        finally:
            await tx.rollback()
