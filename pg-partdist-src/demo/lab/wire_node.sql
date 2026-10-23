CREATE EXTENSION IF NOT EXISTS citus;
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;
INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat)
VALUES (1, '10.77.110.148', 32432, 'active', now()), (2, '10.77.110.145', 32433, 'active', now()), (3, '10.77.110.147', 32434, 'active', now()), (4, '10.77.110.148', 32435, 'active', now())
ON CONFLICT (node_id) DO UPDATE SET hostname = EXCLUDED.hostname, port = EXCLUDED.port, status = EXCLUDED.status, last_heartbeat = now();
SELECT current_setting('port') AS port, (SELECT string_agg(extname || ' ' || extversion, ', ' ORDER BY extname) FROM pg_extension WHERE extname IN ('citus','pg_partdist','pg_raft')) AS ext, (SELECT count(*) FROM partdist.node_map) AS node_map;
