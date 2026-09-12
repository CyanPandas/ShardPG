MODULE_big = pg_partdist
OBJS = \
	src/pg_partdist.o \
	src/metadata_cache.o \
	src/router/write_router.o \
	src/wal/global_mvcc.o \
	src/wal/partition_wal.o \
	src/wal/partition_wal_writer.o \
	src/wal/partwal_sync.o \
	src/wal/shard_fileset.o \
	src/worker/demux_worker.o \
	src/replay/enhanced_clog.o \
	src/replay/shard_replay.o \
	src/replay/replay_checkpoint.o \
	src/replay/shard_route.o \
	src/replay/replay_worker.o \
	src/dtx/dtx_participant.o \
	src/dtx_pending.o \
	src/shard_guard.o \
	src/raft_boundary.o \
	src/shard_xid.o \
	src/shard_clog.o \
	src/shard_vacuum.o \
	src/shard_visibility.o \
	src/tso.o \
	src/tso_client.o \
	src/gxid.o

EXTENSION = pg_partdist
DATA = sql/pg_partdist--1.0.sql

PG_CPPFLAGS = -I$(CURDIR)/include -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK += -L$(shell $(PG_CONFIG) --libdir) -lpq

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
	20_bench_routing \
	21_partwal_dir_create \
	22_partwal_dir_permissions \
	23_partwal_dir_cleanup \
	24_partwal_header_format \
	25_partwal_header_fields \
	26_partwal_boundary_conditions \
	27_partwal_crash_recovery \
	28_demux_shmem \
	29_count_parwal_records \
	30_demux_progress \
	31_demux_latency_stats \
	32_read_all_headers \
	33_partition_lsn_monotone \
	34_writer_deduplication \
	35_demux_worker_active \
	36_parwal_multi_partition \
	37_demux_full_cycle

REGRESS_OPTS = --inputdir=test --outputdir=/tmp/pg_partdist_regress

PG_CONFIG = /work/pg-install/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
