MODULE_big = pg_raft
OBJS = \
	src/pg_raft.o \
	src/raft_leader.o \
	src/raft_consensus.o \
	src/raft_apply.o \
	src/raft_remote.o \
	src/topology_monitor.o

EXTENSION = pg_raft
DATA = sql/pg_raft--1.0.sql

PG_CONFIG ?= $(if $(wildcard $(CURDIR)/../pg-install/bin/pg_config),$(CURDIR)/../pg-install/bin/pg_config,pg_config)

PG_CPPFLAGS = -I$(CURDIR)/include -I$(shell $(PG_CONFIG) --includedir) $(shell $(PG_CONFIG) --cppflags)
SHLIB_LINK += $(shell $(PG_CONFIG) --ldflags) -lpq

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
