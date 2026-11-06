# 容器内：从 perfcg.data 算若干函数的"包含"占比（样本栈里出现即计）
PERF="/tmp/perflib/ld-linux-x86-64.so.2 --library-path /tmp/perflib /tmp/perf"
$PERF script -i /tmp/perfcg.data -F comm,ip,sym 2>/dev/null | awk '
  BEGIN { n=split("plpgsql_exec_function distributed_planner standard_planner CitusExecScan ExecuteLocalTaskListExtended ParseQueryString pg_parse_query pg_analyze_and_rewrite_fixedparams FastPathRouterPlan RouterJob GetCachedLocalPlan sv_satisfies_mvcc shard_xid_state_ex.part.0 sv_satisfies_update HeapTupleSatisfiesVisibility CommitTransaction dtx_master_pre_record_commit XLogFlush XLogInsertRecord LWLockAcquire hash_search_with_hash_value exec_simple_query PortalRun ExecutorStart standard_ExecutorStart partdist_executor_start ShardGuardCheckPlan ShardSnapshotEagerStart CitusExecutorRun AdaptiveExecutor GetSnapshotData ExecInitNode shard_relation_xid_impl ShardCommitRegisterRunning TsoGetStartTs heap_update heap_insert index_insert _bt_doinsert PartWALFlush shard_vacuum_prune_pass ShardVacuumSweep RecordTransactionCommit", F, " "); tot=0 }
  function flush() { if (n0>0) { tot++; for (k in seen) c[k]++ } delete seen; n0=0 }
  /^\t/ { s=$0; sub(/^\t +[0-9a-f]+ /,"",s); sub(/ \(inlined\)$/,"",s); sub(/\+0x[0-9a-f]+$/,"",s); seen[s]=1; n0++; next }
  { flush() }
  END { flush(); for (i=1;i<=n;i++) if (c[F[i]]>0) printf "%6.1f%%  %s\n", 100*c[F[i]]/tot, F[i] }' | sort -rn
