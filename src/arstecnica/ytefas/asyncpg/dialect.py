# -*- coding: utf-8 -*-
# :Project:   arstecnica.ytefas.asyncpg -- Special dialect
# :Created:   mar 20 dic 2016 21:23:47 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016 Arstecnica s.r.l.
#

from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.sql import compiler


compiler.BIND_TEMPLATES['numeric'] = '$[_POSITION]'


class PGDialect_asyncpg(PGDialect_psycopg2):
    def __init__(self, *args, **kwargs):
        kwargs['paramstyle'] = 'numeric'
        super().__init__(*args, **kwargs)
        self.implicit_returning = True
        self.supports_native_enum = True
        self.supports_smallserial = True
        self._backslash_escapes = False
        self.supports_sane_multi_rowcount = True
        self._has_native_hstore = True
