-- Run on every node after deploying the new pg_raft library, before enabling
-- first-shard coordination. The replicated index is installed by pg_partdist.
CREATE OR REPLACE FUNCTION partdist.dtx_choose_commit(
    p_coord_gsid bigint, p_dtxid bigint, p_participants bigint[]
) RETURNS integer LANGUAGE c STRICT VOLATILE
AS '$libdir/pg_raft', 'pg_raft_dtx_choose_commit';
CREATE OR REPLACE FUNCTION partdist.dtx_refresh(bigint)
RETURNS boolean LANGUAGE c STRICT VOLATILE
AS '$libdir/pg_raft', 'pg_raft_dtx_refresh';
REVOKE ALL ON FUNCTION partdist.dtx_refresh(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION partdist.dtx_choose_commit(bigint,bigint,bigint[]) FROM PUBLIC;
COMMENT ON FUNCTION partdist.dtx_choose_commit(bigint,bigint,bigint[]) IS
    'Replicate irrevocable COMMIT before requesting commit_ts. Returns 1 for COMMIT, 2 for an existing ABORT, NULL on a non-leader. No visibility is published until the timestamped decision is replicated.';
