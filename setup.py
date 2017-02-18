# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- SQLAlchemy adaptor for asyncpg
# :Created:   Tue 20 Dec 2016 21:17:12 CET
# :Author:    Lele Gaifax <lele@arstecnica.it>
# :License:   No license
# :Copyright: © 2016, 2017 Arstecnica s.r.l.
#

import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.rst'), encoding='utf-8') as f:
    CHANGES = f.read()
with open(os.path.join(here, 'version.txt'), encoding='utf-8') as f:
    VERSION = f.read().strip()

setup(
    name="metapensiero.sqlalchemy.asyncpg",
    version=VERSION,
    url="https://gitlab.com/metapensiero/metapensiero.sqlalchemy.asyncpg.git",

    description="SQLAlchemy adaptor for asyncpg",
    long_description=README + '\n\n' + CHANGES,

    author="Lele Gaifax",
    author_email="lele@arstecnica.it",

    license="GPLv3+",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        ],
    keywords="",

    packages=['metapensiero.sqlalchemy.' + package
              for package in find_packages('src/metapensiero/sqlalchemy')],
    package_dir={'': 'src'},
    namespace_packages=['metapensiero', 'metapensiero.sqlalchemy'],

    install_requires=[
        'asyncpg',
        'metapensiero.sqlalchemy.proxy',
        'setuptools',
        'sqlalchemy',
    ],
    extras_require={'dev': ['metapensiero.tool.bump_version', 'readme']},
)
