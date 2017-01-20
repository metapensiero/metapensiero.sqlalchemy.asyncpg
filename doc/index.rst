.. -*- coding: utf-8 -*-
.. :Project:   arstecnica.utils.asyncpg -- Documentation
.. :Created:   mer 21 dic 2016 11:49:21 CET
.. :Author:    Lele Gaifax <lele@metapensiero.it>
.. :License:   No License
.. :Copyright: © 2016, 2017 Arstecnica s.r.l.
..

================================
 SQLAlchemy adaptor for asyncpg
================================

Implement the ability of executing SQLAlchemy core statements through
asyncpg__, in a much cleaner way than asyncpgsa__.

Supply also an asyncpg variant of metapensiero.sqlalchemy.proxy's
ProxiedQuery__.

__ https://magicstack.github.io/asyncpg/devel/
__ https://github.com/CanopyTax/asyncpgsa
__ http://metapensierosqlalchemyproxy.readthedocs.io/en/latest/core.html#metapensiero.sqlalchemy.proxy.core.ProxiedQuery

.. toctree::

   dialect
   funcs
   proxy
