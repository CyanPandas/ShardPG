-- T2.1.1: partition WAL directory management — creation

-- Single partition directory
SELECT partdist.init_partition_wal(21001::oid);
SELECT partdist.partition_wal_exists(21001::oid) AS exists;

-- Multiple partitions
SELECT partdist.init_partition_wal(21002::oid);
SELECT partdist.init_partition_wal(21003::oid);
SELECT partdist.partition_wal_exists(21002::oid) AS p21002_exists,
       partdist.partition_wal_exists(21003::oid) AS p21003_exists;

-- init is idempotent (calling twice must not error)
SELECT partdist.init_partition_wal(21001::oid);
SELECT partdist.partition_wal_exists(21001::oid) AS still_exists;

-- Non-initialised partition returns false
SELECT partdist.partition_wal_exists(91099::oid) AS not_exists;

-- Path format: pg_parwal/<id>/<24-char-hex-segment-name>
SELECT partdist.partition_wal_path(21001::oid, 1::bigint) ~ '^pg_parwal/21001/[0-9A-F]{24}$' AS path_valid;

-- Segment 2 path has correct id
SELECT partdist.partition_wal_path(21002::oid, 2::bigint) ~ '^pg_parwal/21002/' AS path_prefix_ok;
