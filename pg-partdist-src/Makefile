MODULE_big = pg_partdist
OBJS = \
	src/pg_partdist.o \
	src/metadata_cache.o \
	src/router/write_router.o

EXTENSION = pg_partdist
DATA = sql/pg_partdist--1.0.sql

PG_CPPFLAGS = -I$(CURDIR)/include

REGRESS = \
	01_extension_create \
	02_partition_map_basic \
	03_node_map_basic \
	04_partition_constraints \
	05_node_constraints \
	06_secondary_nodes_array \
	07_version_control \
	08_schema_existence \
	09_extension_drop_recreate \
	10_citus_coexistence \
	11_cache_api \
	12_cache_invalidate \
	13_cache_stats \
	14_local_routing \
	15_remote_routing \
	16_partition_not_found \
	17_node_down_routing \
	18_metadata_version \
	19_concurrent_update \
	20_bench_routing

REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CONFIG = /work/pg-install/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
