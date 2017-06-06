# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Tests on interval data type
# :Created:   mar 06 giu 2017 11:41:49 CEST
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2017 Arstecnica s.r.l.
#

import sqlalchemy as sa
import pytest

from metapensiero.sqlalchemy import asyncpg

from arstecnica.ytefas.model.tables.risk import schedules


@pytest.mark.xfail # https://github.com/MagicStack/asyncpg/issues/150
@pytest.mark.asyncio
async def test_schedule_repeat_after(pool):
    q = sa.select([schedules.c.repeat_after]) \
        .where(schedules.c.name == 'Revisione pulegge')

    async with pool.acquire() as conn:
        result = await asyncpg.fetchone(conn, q)
        assert result['repeat_after'].days == 1825
