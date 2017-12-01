# -*- coding: utf-8 -*-
# :Project:   metapensiero.sqlalchemy.asyncpg -- SQLAlchemy adaptor for asyncpg
# :Created:   Tue 20 Dec 2016 21:17:12 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: © 2016, 2017 Lele Gaifax
#

export TOPDIR := $(CURDIR)
export VENVDIR := $(TOPDIR)/env
export PYTHON := $(VENVDIR)/bin/python
export SHELL := /bin/bash
export SYS_PYTHON := $(shell which python3.6 || which python3)

all: virtualenv help

help::
	@printf "check\n\trun the test suite\n"

PYTEST = $(VENVDIR)/bin/pytest $(PYTEST_OPTIONS)

.PHONY: check
check:
	$(PYTEST) tests/

help::
	@printf "doc\n\tBuild Sphinx documentation\n"

.PHONY: doc
doc:
	@$(MAKE) -C doc/ --no-print-directory html

include Makefile.release
include Makefile.virtualenv
