.. -*- coding: utf-8 -*-
.. :Project:   metapensiero.sqlalchemy.asyncpg -- SQLAlchemy adaptor for asyncpg
.. :Created:   Tue 20 Dec 2016 21:17:12 CET
.. :Author:    Lele Gaifax <lele@metapensiero.it>
.. :License:   GNU General Public License version 3 or later
.. :Copyright: © 2016, 2017 Lele Gaifax
..

=================================
 metapensiero.sqlalchemy.asyncpg
=================================

SQLAlchemy adaptor for asyncpg
==============================

 :author: Lele Gaifax
 :contact: lele@metapensiero.it
 :license: GNU General Public License version 3 or later

Implement the ability of executing SQLAlchemy core statements through
asyncpg__, in a much cleaner way than asyncpgsa__.

Supply also an asyncpg variant of metapensiero.sqlalchemy.proxy's
ProxiedQuery__.

__ https://github.com/MagicStack/asyncpg
__ https://github.com/CanopyTax/asyncpgsa
__ http://metapensierosqlalchemyproxy.readthedocs.io/en/latest/core.html#metapensiero.sqlalchemy.proxy.core.ProxiedQuery
