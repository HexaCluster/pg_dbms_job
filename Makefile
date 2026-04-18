EXTENSION  = pg_dbms_job
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
		sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

MODULE_big = pg_dbms_job
OBJS = pg_dbms_job.o $(WIN32RES)

PGFILEDESC = "pg_dbms_job - Propose Oracle DBMS_JOB compatibility for PostgreSQL"

PG_LDFLAGS = -L$(libpq_builddir) -lpq

PG_CONFIG = pg_config
PG10 = $(shell $(PG_CONFIG) --version | egrep " 8\.| 9\." > /dev/null && echo no || echo yes)

ifeq ($(PG10),yes)
DOCS = $(wildcard README*)

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of PostgreSQL required is 10.0)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

installcheck:
	$(PROVE)
