.. -*- coding: utf-8 -*-
.. :Project:   arstecnica.utils.asyncpg -- SQLAlchemy adaptor for asyncpg
.. :Created:   Tue 20 Dec 2016 21:17:12 CET
.. :Author:    Lele Gaifax <lele@arstecnica.it>
.. :License:   No license
.. :Copyright: © 2016, 2017 Arstecnica s.r.l.
..

===========================
 arstecnica.utils.asyncpg
===========================

SQLAlchemy adaptor for asyncpg
==============================

 :author: Lele Gaifax
 :contact: lele@arstecnica.it
 :license: No license

Implement the ability of executing SQLAlchemy core statements through
asyncpg__, in a much cleaner way than asyncpgsa__.

Supply also an asyncpg variant of metapensiero.sqlalchemy.proxy's
ProxiedQuery__.

__ https://github.com/MagicStack/asyncpg
__ https://github.com/CanopyTax/asyncpgsa
__ http://metapensierosqlalchemyproxy.readthedocs.io/en/latest/core.html#metapensiero.sqlalchemy.proxy.core.ProxiedQuery
