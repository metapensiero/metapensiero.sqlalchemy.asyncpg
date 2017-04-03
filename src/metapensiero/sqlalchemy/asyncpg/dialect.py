# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Specialized dialect
# :Created:   mar 20 dic 2016 21:23:47 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

import re

from sqlalchemy.dialects.postgresql.psycopg2 import (PGCompiler_psycopg2,
                                                     PGDialect_psycopg2)
from sqlalchemy.sql import compiler
from sqlalchemy.types import NullType


compiler.BIND_TEMPLATES['numeric'] = '$[_POSITION]'


class PGCompiler_asyncpg(PGCompiler_psycopg2):
    """Custom SA PostgreSQL compiler that produces param placeholder
    compatible with asyncpg.

    This solves https://github.com/MagicStack/asyncpg/issues/32.
    """

    def _apply_numbered_params(self):
        idx = 0
        names = self.positiontup
        binds = self.binds
        process = self.dialect.type_compiler.process

        def replace(match):
            nonlocal idx
            name = names[idx]
            idx += 1
            param = binds[name]
            type = param.type
            if isinstance(type, NullType):
                return str(idx)
            else:
                return '%s::%s' % (idx, process(type))

        self.string = re.sub(r'\[_POSITION\]', replace, self.string)


class PGDialect_asyncpg(PGDialect_psycopg2):
    """Custom SA PostgreSQL dialect compatible with asyncpg.

    In particular it uses a variant of the ``numeric`` `paramstyle`, to
    produce placeholders like ``$1::INTEGER``, ``$2::VARCHAR`` and so on.
    """

    statement_compiler = PGCompiler_asyncpg

    def __init__(self, *args, **kwargs):
        kwargs['paramstyle'] = 'numeric'
        super().__init__(*args, **kwargs)
        self.implicit_returning = True
        self.supports_native_enum = True
        self.supports_smallserial = True
        self._backslash_escapes = False
        self.supports_sane_multi_rowcount = True
        self._has_native_hstore = True
