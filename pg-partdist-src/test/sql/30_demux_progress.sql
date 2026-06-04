-- T2.2: demux_progress function returns expected structure

-- Function returns a record with two columns
SELECT
    (partdist.demux_progress()).node_name = 'local'   AS node_is_local,
    pg_typeof((partdist.demux_progress()).node_name)  AS node_name_type;

-- last_processed_lsn is NULL initially or a valid LSN — must not error
SELECT (partdist.demux_progress()).last_processed_lsn IS NULL
    OR (partdist.demux_progress()).last_processed_lsn >= '0/0'::pg_lsn AS lsn_valid;
