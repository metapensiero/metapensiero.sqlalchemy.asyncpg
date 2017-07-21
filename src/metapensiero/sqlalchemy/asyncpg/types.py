# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- Custom types
# :Created:   lun 10 lug 2017 09:52:36 CEST
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   No License
# :Copyright: © 2017 Arstecnica s.r.l.
#

from decimal import Decimal
from operator import itemgetter

from asyncpg.types import Range
from nssjson import JSONDecoder, JSONEncoder


class Interval(tuple):
    'Represent a PG `interval`, carrying `months`, `days` and `microseconds`.'

    __slots__ = ()

    def __new__(cls, months, days, microseconds):
        'Create new instance of Interval(months, days, microseconds)'
        return tuple.__new__(cls, (months, days, microseconds))

    @classmethod
    def _decode(cls, low_level_tuple):
        assert len(low_level_tuple) == 3
        return tuple.__new__(cls, low_level_tuple)

    months = property(itemgetter(0), doc='Number of months')
    days = property(itemgetter(1), doc='Number of days')
    microseconds = property(itemgetter(2), doc='Number of microseconds')


def _daterange_serializer(obj):
    "nssjson serializer of PG DateRange."

    if isinstance(obj, Range):
        if obj.isempty:
            desc = 'empty'
        else:
            lb = '[' if obj.lower_inc else '('
            ub = ']' if obj.upper_inc else ')'
            v = obj.lower
            lv = '' if v is None else v.isoformat()
            v = obj.upper
            uv = '' if v is None else v.isoformat()
            desc = '%s%s,%s%s' % (lb, lv, uv, ub)
        return desc

    raise TypeError('Unable to serialize %r instance' % type(obj))


json_encode = JSONEncoder(separators=(',', ':'),
                          use_decimal=True,
                          iso_datetime=True,
                          handle_uuid=True,
                          default=_daterange_serializer).encode
"Custom JSON encoder that knows about PG `daterange`."

json_decode = JSONDecoder(parse_float=Decimal,
                          iso_datetime=True,
                          handle_uuid=True).decode
"Custom JSON decoder that knows about PG `daterange`."


def _json_encode(value):
    return json_encode(value).encode('utf-8')


def _jsonb_encode(value):
    return b'\x01' + json_encode(value).encode('utf-8')


def _json_decode(value):
    return json_decode(value.decode('utf-8'))


def _jsonb_decode(value):
    return json_decode(value[1:].decode('utf-8'))


async def register_custom_codecs(con):
    """Register our custom codecs on the asyncpg connection `con`.

    This function should be passed as the ``init`` argument to
    :func:`asyncpg.create_pool()`.
    """

    await con.set_builtin_type_codec('hstore', codec_name='pg_contrib.hstore')
    await con.set_type_codec('json', schema='pg_catalog', format='binary',
                             encoder=_json_encode, decoder=_json_decode)
    await con.set_type_codec('jsonb', schema='pg_catalog', format='binary',
                             encoder=_jsonb_encode, decoder=_jsonb_decode)
    await con.set_type_codec('interval', schema='pg_catalog', format='tuple',
                             encoder=lambda x: x, decoder=Interval._decode)
