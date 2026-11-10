-- FA23 demo node registration: isolated Citus metadata transaction.
-- Apply locally on the coordinator; business sessions retain VACU.
CREATE OR REPLACE FUNCTION shardpg.add_node(p_host text, p_port integer)
RETURNS TABLE (step text, detail text, secs integer) LANGUAGE plpgsql AS $fn$
DECLARE
    t0          timestamptz := clock_timestamp();
    ts          timestamptz;
    v_id        integer;
    v_me        text;                                   -- 新节点在 pg_raft.peers 里的写法：<id>@<host>:<port>
    v_peers     text := current_setting('pg_raft.peers');
    v_new_peers text;
    v_list      text[];
    v_old_n     integer;                                -- 新节点之外的现有节点数
    v_todo      integer := 0;                           -- ② 里这次真正追加的节点数
    v_v         text;
    r           record;
    v_n         integer;
    v_g0        integer;
    v_idx       bigint := 0;
    i           integer;
BEGIN
    -- ① 新节点：连得上、三个扩展装好了、pg_raft.peers = 现有列表 + 它自己
    -- ★ P7-N105：可以续做。①–③ 是在各节点上单独执行的（ALTER SYSTEM、控制面提案），函数中途出错回滚时
    --   撤不掉（现场：④ citus_add_node 提交时出错，重跑就报"节点号已经在集群里"）⇒ 按现状判断哪几步已经做过，
    --   做过的跳过，从没做完的那步接着做
    ts := clock_timestamp();
    v_v := partdist.addr_exec(p_host, p_port, 'SHOW pg_raft.node_id');
    v_id := v_v::integer;
    v_me := v_id || '@' || p_host || ':' || p_port;
    SELECT format('%s:%s', hostname, port) INTO v_v FROM partdist.node_map
     WHERE node_id = v_id AND (hostname <> p_host OR port <> p_port);
    IF FOUND THEN
        RAISE EXCEPTION '节点号 % 已被别的节点占用（node_map 里登记的是 %）', v_id, v_v;
    END IF;
    SELECT node_id::text INTO v_v FROM partdist.node_map
     WHERE node_id <> v_id AND hostname = p_host AND port = p_port;
    IF FOUND THEN
        RAISE EXCEPTION '%:% 在 node_map 里登记的是节点 %，新节点报的是 %', p_host, p_port, v_v, v_id;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_dist_node WHERE nodeport = p_port) THEN
        IF EXISTS (SELECT 1 FROM partdist.node_map WHERE node_id = v_id) THEN
            step := 'done'; secs := 0;
            detail := format('%s (%s:%s) is already in the cluster, nothing to do', shardpg.node_name(v_id), p_host, p_port);
            RETURN NEXT;
            RETURN;
        END IF;
        RAISE EXCEPTION '端口 % 已经是 ShardPG worker，但 node_map 里没有节点 %', p_port, v_id;
    END IF;
    v_list := string_to_array(v_peers, ',');
    IF v_list[array_length(v_list, 1)] = v_me THEN
        -- 上次已经做过 ②（master 自己的 peers 以新节点结尾）：v_peers 还原成追加前的列表
        v_new_peers := v_peers;
        v_peers := array_to_string(v_list[1:array_length(v_list, 1) - 1], ',');
    ELSIF EXISTS (SELECT 1 FROM unnest(v_list) e
                   WHERE split_part(e, '@', 1) = v_id::text OR split_part(e, ':', 2) = p_port::text) THEN
        RAISE EXCEPTION '节点号 % 或端口 % 已在 pg_raft.peers 里，但不是最后一个：%', v_id, p_port, v_peers;
    ELSE
        v_new_peers := v_peers || ',' || v_me;
    END IF;
    SELECT count(*) INTO v_old_n FROM partdist.node_map WHERE node_id <> v_id;
    v_v := partdist.addr_exec(p_host, p_port,
        'SELECT string_agg(extname, '','' ORDER BY extname) FROM pg_extension WHERE extname IN (''citus'', ''pg_partdist'', ''pg_raft'')');
    IF v_v IS DISTINCT FROM 'citus,pg_partdist,pg_raft' THEN
        RAISE EXCEPTION '新节点 %:% 的扩展不全（有：%），先在它上面 CREATE EXTENSION citus / pg_partdist / pg_raft', p_host, p_port, v_v;
    END IF;
    v_v := partdist.addr_exec(p_host, p_port, 'SHOW pg_raft.peers');
    IF v_v <> v_new_peers THEN
        RAISE EXCEPTION '新节点的 pg_raft.peers 应为 %，现为 %', v_new_peers, v_v;
    END IF;
    step := 'new node ready'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s (node id %s, %s:%s): extensions installed, pg_raft.peers = the %s existing nodes + itself',
                     shardpg.node_name(v_id), v_id, p_host, p_port, v_old_n);
    RETURN NEXT;

    -- ② 各节点 pg_raft.peers 末尾追加新节点：ALTER SYSTEM + reload，不重启（只许追加一个，见 pg_raft_peers_check_hook）
    --   已经是新列表的节点（上次做过）跳过
    ts := clock_timestamp();
    FOR r IN SELECT node_id, hostname, port FROM partdist.node_map WHERE node_id <> v_id ORDER BY node_id LOOP
        v_v := partdist.addr_exec(r.hostname, r.port, 'SHOW pg_raft.peers');
        IF v_v <> v_peers AND v_v <> v_new_peers THEN
            RAISE EXCEPTION '节点 %（%:%）的 pg_raft.peers 与 master 不一致：%', r.node_id, r.hostname, r.port, v_v;
        END IF;
    END LOOP;
    FOR r IN SELECT node_id, hostname, port FROM partdist.node_map WHERE node_id <> v_id ORDER BY node_id LOOP
        IF partdist.addr_exec(r.hostname, r.port, 'SHOW pg_raft.peers') <> v_new_peers THEN
            PERFORM partdist.addr_exec(r.hostname, r.port, format('ALTER SYSTEM SET pg_raft.peers = %L', v_new_peers));
            PERFORM partdist.addr_exec(r.hostname, r.port, 'SELECT pg_reload_conf()');
            v_todo := v_todo + 1;
        END IF;
    END LOOP;
    FOR i IN 1..30 LOOP
        SELECT count(*) INTO v_n FROM partdist.node_map nm
         WHERE nm.node_id <> v_id
           AND partdist.addr_exec(nm.hostname, nm.port, 'SHOW pg_raft.peers', false) IS DISTINCT FROM v_new_peers;
        EXIT WHEN v_n = 0;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF v_n > 0 THEN
        RAISE EXCEPTION '% 个节点 reload 之后 pg_raft.peers 仍是旧值（看各节点日志里 pg_raft.peers 的报错）', v_n;
    END IF;
    IF v_todo > 0 THEN
        PERFORM pg_catalog.pg_sleep(2);     -- 等各节点的 Raft 进程在下一次 tick 换上新表
    END IF;
    step := 'append pg_raft.peers'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('reloaded on %s nodes%s; control-plane group 0 members %s -> %s, quorum %s',
                     v_old_n,
                     CASE WHEN v_todo < v_old_n THEN format(' (%s of them already appended by an earlier run)', v_old_n - v_todo) ELSE '' END,
                     v_old_n, v_old_n + 1, (v_old_n + 1) / 2 + 1);
    RETURN NEXT;

    -- ③ 控制面登记新节点地址（各节点的 node_map 按日志序同时学到），等新节点追上控制面日志
    --   登记是 upsert，续做时再提一次也无妨（顺带拿到一个要等新节点追到的 index）
    ts := clock_timestamp();
    FOR i IN 1..30 LOOP
        SELECT leader_node_id INTO v_g0 FROM partdist.pg_raft_group_status() WHERE group_id = 0;
        IF v_g0 IS NOT NULL AND v_g0 > 0 THEN
            BEGIN
                v_idx := partdist.node_exec(v_g0, format(
                    'SELECT partdist.pg_raft_group_propose(0, ''OP_NODE_STATUS'', %L)',
                    json_build_object('node_id', v_id, 'status', 'active', 'hostname', p_host, 'port', p_port)::text))::bigint;
            EXCEPTION WHEN OTHERS THEN
                v_idx := 0;
            END;
        END IF;
        EXIT WHEN v_idx > 0;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF v_idx <= 0 THEN
        RAISE EXCEPTION '控制面登记新节点失败（组 0 leader = %）', v_g0;
    END IF;
    FOR i IN 1..180 LOOP
        v_v := partdist.addr_exec(p_host, p_port,
            'SELECT last_applied FROM partdist.pg_raft_group_status() WHERE group_id = 0', false);
        EXIT WHEN coalesce(nullif(v_v, '')::bigint, 0) >= v_idx
              AND EXISTS (SELECT 1 FROM partdist.node_map WHERE node_id = v_id AND port = p_port);
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF coalesce(nullif(v_v, '')::bigint, 0) < v_idx THEN
        RAISE EXCEPTION '新节点 3 分钟内没追上控制面日志（它在 %，要到 %）', v_v, v_idx;
    END IF;
    -- 新节点的 node_map 以 master 为准（控制面快照里的旧占位地址一并纠正）
    PERFORM partdist.addr_exec(p_host, p_port, format(
        'INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat) '
        'SELECT * FROM jsonb_populate_recordset(NULL::partdist.node_map, %L::jsonb) '
        'ON CONFLICT (node_id) DO UPDATE SET hostname = EXCLUDED.hostname, port = EXCLUDED.port',
        (SELECT json_agg(nm)::text FROM partdist.node_map nm)));
    step := 'join control plane'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s:%s registered at group 0 log index %s, the new node has caught up', p_host, p_port, v_idx);
    RETURN NEXT;

    -- ④ ShardPG 加 worker：同步元数据，各分布表的外壳建到新节点上
    ts := clock_timestamp();
    -- Node registration only changes Citus catalogs; it has no business shard
    -- to enlist in first-shard coordination. Use a separate metadata-only
    -- transaction, as the peer/address steps above already do. Never change
    -- the caller's protocol setting or include caller business writes here.
    PERFORM partdist.dtx_self(format(
        'SET LOCAL shardpg.first_shard_coordinator=off; '
        'SELECT pg_catalog.citus_add_node(%L,%s)', p_host, p_port));
    PERFORM partdist.addr_exec(p_host, p_port, 'SELECT partdist.rebuild_shard_identity()');
    step := 'shardpg add worker'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s:%s added as a ShardPG worker, table metadata synced; no shards on the new node yet, use shardpg.move_replica to move replicas over', p_host, p_port);
    RETURN NEXT;

    step := 'done'; secs := round(extract(epoch FROM clock_timestamp() - t0));
    detail := format('%s (%s:%s) joined the cluster, no node was restarted', shardpg.node_name(v_id), p_host, p_port);
    RETURN NEXT;
END $fn$;
COMMENT ON FUNCTION shardpg.add_node(text, integer) IS
    'P7-N102：新 worker 入簇（不重启）。前提：新节点已按 scripts/new_worker_node.sh 准备好（配置里 pg_raft.peers = 现有列表 + 它自己、建好 citus / pg_partdist / pg_raft 扩展、已启动）。P7-N105：中途失败后直接重跑即可，做过的步骤自动跳过。';

