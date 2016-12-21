# -*- coding: utf-8 -*-
# :Project:   Ytefas -- pytest configuration
# :Created:   mer 21 dic 2016 12:37:39 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016 Arstecnica s.r.l.
#

from os import environ
from sys import exit

import asyncpg
import pytest

from arstecnica.ytefas.model.utils import assert_database_is_up


DB_HOST = environ['DATABASE_HOST']
DB_PORT = environ['DATABASE_PORT']
DB_NAME = environ['DATABASE_NAME']
DB_USER = environ['DATABASE_USER']
DB_PWD = environ['DATABASE_PASSWORD']


@pytest.fixture
def pool(event_loop):
    db_url = f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return event_loop.run_until_complete(asyncpg.create_pool(
        db_url,
        min_size=1,
        max_size=10
    ))


if not assert_database_is_up(DB_HOST, int(DB_PORT)):
    exit(2)
