-- ============================================================================
-- shardpg_demo_functions.sql —— ShardPG 演示用函数库（只用于演示）
--
-- 只装在 master（:5432）上、全部放在 demo 模式里；经 dblink 访问 3 台 worker。
-- 术语：master = 只做路由 + TSO 的那个节点（不放数据、不领导任何数据组）；
--       「协调者」是**事务**的概念 —— 每个全局事务在自己的写集里选一个分片组当协调组（见 DTX_2PC_DESIGN §2.1）。
-- 由 shardpg_demo.sh start 安装，shardpg_demo.sh stop 时 DROP SCHEMA demo CASCADE 整体删除。
-- 不改动项目的任何文件、表结构或函数；对集群的临时改动（选举超时冻结等）都在函数内复位。
--
-- 用户在演示里只需要输入 SQL：SELECT * FROM demo.xxx(...)，以及普通的 BEGIN/INSERT/…/COMMIT。
-- ============================================================================
\set ON_ERROR_STOP on
SET client_min_messages = warning;
SET citus.enable_ddl_propagation = off;       -- 只建在 master 本地，不往 worker 传播

DROP SCHEMA IF EXISTS demo CASCADE;
CREATE SCHEMA demo;
CREATE EXTENSION dblink SCHEMA demo;

-- ─────────────────────────────── 节点登记 ───────────────────────────────
CREATE TABLE demo.node (
    name    text PRIMARY KEY,      -- cn / w1 / w2 / w3
    port    int  NOT NULL,
    raft_id int,                   -- pg_raft 节点号（partition_map.primary_node 用的就是它）
    kind    text NOT NULL,
    datadir text
);

CREATE FUNCTION demo.conn(p_node text) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT format('host=/tmp port=%s dbname=postgres user=postgres sslmode=disable '
                  'options=''-c citus.override_table_visibility=false''', port)
      FROM demo.node WHERE name = p_node
$$;

INSERT INTO demo.node VALUES ('master', 5432, current_setting('pg_raft.node_id')::int, 'master',
                              current_setting('data_directory'));
INSERT INTO demo.node (name, port, kind)
SELECT 'w' || row_number() OVER (ORDER BY nodeport), nodeport, 'worker'
  FROM pg_dist_node WHERE noderole = 'primary' AND groupid <> 0 AND isactive;
UPDATE demo.node n SET raft_id = t.id::int, datadir = t.dir
  FROM (SELECT d.name, r.id, r.dir
          FROM demo.node d,
               LATERAL demo.dblink(demo.conn(d.name),
                   'SELECT current_setting(''pg_raft.node_id''), current_setting(''data_directory'')')
                   AS r(id text, dir text)
         WHERE d.kind = 'worker') t
 WHERE n.name = t.name;

-- 演示用到的表（raft_elect 时登记），stop 时据此删表、删登记
CREATE TABLE demo.managed (tbl text PRIMARY KEY, shardids bigint[]);

-- 演示会改动的参数在各节点 postgresql.auto.conf 里的原样（start 时记下，stop 时原样还原）
CREATE TABLE demo.saved_guc (node text, name text, in_autoconf bool, value text, PRIMARY KEY (node, name));

CREATE FUNCTION demo._save_gucs() RETURNS void LANGUAGE plpgsql AS $$
DECLARE n record; g text; conf text; m text[];
BEGIN
    DELETE FROM demo.saved_guc;
    FOR n IN SELECT * FROM demo.node LOOP
        SELECT t.c INTO conf FROM demo.dblink(demo.conn(n.name), 'SELECT pg_read_file(''postgresql.auto.conf'')') AS t(c text);
        FOREACH g IN ARRAY ARRAY['pg_partdist.tso_master', 'pg_partdist.tso_conninfo', 'pg_partdist.tso_lease_ms',
                                 'pg_raft.election_timeout_ms'] LOOP
            m := regexp_match(conf, '^' || replace(g, '.', '\.') || $r$ = '((?:[^']|'')*)'$r$, 'n');
            INSERT INTO demo.saved_guc VALUES (n.name, g, m IS NOT NULL, replace(m[1], $r$''$r$, $r$'$r$));
        END LOOP;
    END LOOP;
END $$;

CREATE FUNCTION demo._restore_gucs() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; k int := 0;
BEGIN
    FOR r IN SELECT * FROM demo.saved_guc LOOP
        PERFORM demo.dblink_exec(demo.conn(r.node), CASE WHEN r.in_autoconf
            THEN format('ALTER SYSTEM SET %s = %L', r.name, r.value)
            ELSE format('ALTER SYSTEM RESET %s', r.name) END);
        k := k + 1;
    END LOOP;
    FOR r IN SELECT * FROM demo.node LOOP
        PERFORM demo.q(r.name, 'SELECT pg_reload_conf()::text');
    END LOOP;
    RETURN k || ' 项参数已按演示前的 postgresql.auto.conf 还原';
END $$;

-- ─────────────────────────────── 小工具 ───────────────────────────────
-- 在某节点上执行一条返回单个 text 的查询（每次新连）
CREATE FUNCTION demo.q(p_node text, p_sql text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text;
BEGIN
    SELECT t.v INTO v FROM demo.dblink(demo.conn(p_node), p_sql) AS t(v text);
    RETURN v;
END $$;

-- 同上，但用常驻命名连接、连不上（宕机）返回 NULL —— 给轮询用
CREATE FUNCTION demo.pq(p_node text, p_sql text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text; c text := 'demo_' || p_node; attempt int;
BEGIN
    FOR attempt IN 1..2 LOOP        -- 出错先换一条新连接重试一次；两次都不行才当它连不上
        BEGIN
            IF NOT (c = ANY (coalesce(demo.dblink_get_connections(), '{}'::text[]))) THEN
                PERFORM demo.dblink_connect(c, demo.conn(p_node));
            END IF;
            SELECT t.v INTO v FROM demo.dblink(c, p_sql) AS t(v text);
            RETURN v;
        EXCEPTION WHEN OTHERS THEN
            PERFORM set_config('demo.last_error', p_node || ': ' || SQLERRM, false);
            BEGIN PERFORM demo.dblink_disconnect(c); EXCEPTION WHEN OTHERS THEN NULL; END;
        END;
    END LOOP;
    RETURN NULL;
END $$;

CREATE FUNCTION demo.workers() RETURNS SETOF text LANGUAGE sql STABLE AS $$
    SELECT name FROM demo.node WHERE kind = 'worker' ORDER BY port
$$;

CREATE FUNCTION demo.node_of_raft(p_id int) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT name FROM demo.node WHERE raft_id = p_id
$$;

CREATE FUNCTION demo.members() RETURNS text LANGUAGE sql STABLE AS $$
    SELECT 'ARRAY[' || string_agg(raft_id::text, ',' ORDER BY port) || ']' FROM demo.node WHERE kind = 'worker'
$$;

CREATE FUNCTION demo.sids(p_tbl regclass) RETURNS bigint[] LANGUAGE sql STABLE AS $$
    SELECT array_agg(shardid ORDER BY shardid) FROM pg_dist_shard WHERE logicalrelid = p_tbl
$$;

CREATE FUNCTION demo.slabel(p_tbl regclass, p_sid bigint) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT 'S' || array_position(demo.sids(p_tbl), p_sid)
$$;

CREATE FUNCTION demo.sid(p_tbl regclass, p_label text) RETURNS bigint LANGUAGE plpgsql STABLE AS $$
DECLARE s bigint;
BEGIN
    s := (demo.sids(p_tbl))[substr(upper(p_label), 2)::int];
    IF s IS NULL THEN RAISE EXCEPTION '分片 % 不存在（用 S1、S2、S3）', p_label; END IF;
    RETURN s;
END $$;

-- Citus 路由：该分片的读写发往哪个节点（pg_dist_placement）
CREATE FUNCTION demo.route_node(p_sid bigint) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT d.name FROM pg_dist_placement pl
      JOIN pg_dist_node n ON n.groupid = pl.groupid AND n.noderole = 'primary'
      JOIN demo.node d ON d.port = n.nodeport
     WHERE pl.shardid = p_sid
$$;

-- 控制面登记（Raft 0 号组里的 partition_map，master 本地这一份）
CREATE FUNCTION demo.reg(p_sid bigint) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT demo.node_of_raft(primary_node) FROM partdist.partition_map WHERE partition_id = p_sid
$$;

-- 本节点在该组里的状态 "state|term|leader[|dup]"；没有这个组 = '-'，连不上 = NULL。
-- 同一个组在本节点被建成两个槽位时（已知缺陷，见教程"已知问题"），取 leader/follower 那个并标 dup。
CREATE FUNCTION demo.gstate(p_node text, p_sid bigint) RETURNS text LANGUAGE sql AS $$
    SELECT demo.pq(p_node, format(
        'SELECT coalesce((SELECT state||''|''||current_term||''|''||leader_node_id'
        '||CASE WHEN (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = %1$s) > 1 THEN ''|dup'' ELSE '''' END '
        'FROM partdist.pg_raft_group_status() WHERE group_id = %1$s ORDER BY (state = ''leader'') DESC, (state = ''follower'') DESC LIMIT 1), ''-'')', p_sid))
$$;

CREATE FUNCTION demo.leader(p_sid bigint) RETURNS text LANGUAGE plpgsql AS $$
DECLARE w text;
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        IF split_part(coalesce(demo.gstate(w, p_sid), ''), '|', 1) = 'leader' THEN RETURN w; END IF;
    END LOOP;
    RETURN NULL;
END $$;

CREATE FUNCTION demo.loid(p_node text, p_sid bigint) RETURNS text LANGUAGE sql AS $$
    SELECT demo.pq(p_node, format('SELECT partdist.local_partition_for_shard(%s)::text', p_sid))
$$;

-- 演示期间的选举超时：15 s（start 设置、stop 还原）。2 vCPU 演示机上并发 2PC + 刷盘时，单线程的共识 tick
-- 偶尔一轮卡 5–9 s（日志"共识 tick 耗时 … 选举超时 6000 ms"），默认 6 s 会误选主。
-- 演示期间各 worker 实际生效的选举超时（start 设的值，可用 ELECTION_MS 改；产品默认是 6000）
CREATE FUNCTION demo.demo_election_ms() RETURNS int LANGUAGE sql AS $$
    SELECT coalesce(nullif(demo.pq((SELECT name FROM demo.node WHERE kind = 'worker' ORDER BY port LIMIT 1),
                                   'SELECT current_setting(''pg_raft.election_timeout_ms'')'), ''), '15000')::int $$;

-- 选举超时：冻结 / 复位（夹具手法，只在建组供副本期间用；复位 = 回到演示期间的 15 s）
CREATE FUNCTION demo.set_election_timeout(p_ms int) RETURNS void LANGUAGE plpgsql AS $$
DECLARE w text;
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        PERFORM demo.dblink_exec(demo.conn(w),
            format('ALTER SYSTEM SET pg_raft.election_timeout_ms = %s', coalesce(p_ms, demo.demo_election_ms())));
        PERFORM demo.q(w, 'SELECT pg_reload_conf()::text');
    END LOOP;
END $$;

CREATE FUNCTION demo.ms(p_t0 timestamptz) RETURNS int LANGUAGE sql VOLATILE AS $$
    SELECT (extract(epoch FROM clock_timestamp() - p_t0) * 1000)::int
$$;

-- 一条状态变化 → 一句人话
CREATE FUNCTION demo.describe(p_old text, p_new text, p_node text DEFAULT NULL, p_sid bigint DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE o text[] := string_to_array(coalesce(p_old, ''), '|');
        n text[] := string_to_array(p_new, '|');
        ldr text; hasdata text;
BEGIN
    IF p_new = 'down' THEN RETURN '✗ 连不上（节点宕机）'; END IF;
    IF p_new = '-' THEN RETURN '（本节点上还没有这个组）'; END IF;
    ldr := demo.node_of_raft(nullif(n[3], '0')::int);
    IF n[4] = 'dup' THEN
        RETURN format('⚠ 这个组在本节点被建成了 2 个槽位（已知缺陷：重启时并发建组），按 %s（任期 %s）显示', n[1], n[2]);
    END IF;
    IF n[1] = 'leader' THEN RETURN format('★ 当选 leader（任期 %s）', n[2]); END IF;
    IF n[1] = 'candidate' THEN RETURN format('发起竞选 → candidate（任期 %s，向其余成员要票）', n[2]); END IF;
    IF n[1] = 'follower' THEN
        IF o[1] = 'leader' THEN
            -- 当选之后又变回 follower，有两种原因，要分清楚：
            --   ① 本节点没有这个分片的数据 ⇒ 升主前置 RETURN -1 ⇒ **主动让位**并退避 5 个选举周期；
            --   ② 看见了更高的任期（别人已经当选）⇒ 退位跟随。
            IF p_node IS NOT NULL AND p_sid IS NOT NULL THEN
                -- 注意 coalesce：没有本地分片时 local_partition_for_shard 返回 NULL，
                -- (NULL > 0) 是 NULL 不是 false，漏了它这条分支永远走不到。
                hasdata := demo.pq(p_node, format('SELECT (coalesce(partdist.local_partition_for_shard(%s), 0) > 0)::text', p_sid));
                IF hasdata = 'false' THEN
                    RETURN format('✗ 主动让位 → follower（任期 %s）：本节点没有这个分片的数据，升主前置拒绝升主，退避 5 个选举周期让给别人', n[2]);
                END IF;
            END IF;
            IF ldr IS NULL THEN RETURN format('退位 → follower（任期 %s，还没认出新 leader）', n[2]); END IF;
            RETURN format('退位 → follower，跟随 %s（任期 %s）', ldr, n[2]);
        END IF;
        IF ldr IS NULL THEN RETURN format('follower（任期 %s，还没认出 leader）', n[2]); END IF;
        IF p_old = 'down' THEN RETURN format('重新连上：follower，跟随 %s（任期 %s）', ldr, n[2]); END IF;
        RETURN format('follower：认 %s 为 leader（任期 %s）', ldr, n[2]);
    END IF;
    RETURN p_new;
END $$;

-- ★ 轮询观察器：把各节点在这些组里的角色变化、控制面登记、Citus 路由的变化**实时**用 NOTICE 打出来，
--   直到每个组都稳定（有且只有一个活着的 leader、其余活着的成员都认它、控制面登记与 Citus 路由都指向它）。
CREATE FUNCTION demo.watch(p_tbl regclass, p_sids bigint[], p_want text, p_timeout_s int, p_t0 timestamptz,
                           p_last jsonb DEFAULT '{}')
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    last jsonb := p_last; k text; cur text; sid bigint; w text; n text[];
    lead text; lead_id int; reg text; rt text; ok bool; stable int := 0; t int; term text;
    sts text[];
BEGIN
    LOOP
        t := demo.ms(p_t0);
        ok := true;
        FOREACH sid IN ARRAY p_sids LOOP
            lead := NULL; sts := '{}';
            FOR w IN SELECT demo.workers() LOOP
                cur := coalesce(demo.gstate(w, sid), 'down');
                sts := sts || (w || '=' || cur);
                k := sid || ':' || w;
                IF (last ->> k) IS DISTINCT FROM cur THEN
                    RAISE NOTICE '% ms  %  %  %', lpad(t::text, 6), demo.slabel(p_tbl, sid), w, demo.describe(last ->> k, cur, w, sid);
                    last := last || jsonb_build_object(k, cur);
                END IF;
                IF split_part(cur, '|', 1) = 'leader' THEN lead := w; END IF;
            END LOOP;
            reg := demo.reg(sid);
            term := (SELECT primary_term::text FROM partdist.partition_map WHERE partition_id = sid);
            k := sid || ':reg'; cur := coalesce(reg, '?') || '|' || coalesce(term, '?');
            IF (last ->> k) IS DISTINCT FROM cur THEN
                RAISE NOTICE '% ms  %  master  控制面登记（0 号组 partition_map）：%', lpad(t::text, 6), demo.slabel(p_tbl, sid),
                    CASE WHEN reg IS NULL THEN '还没登记' ELSE format('主 = %s（登记任期 %s）', reg, term) END;
                last := last || jsonb_build_object(k, cur);
            END IF;
            rt := demo.route_node(sid);
            k := sid || ':route';
            IF (last ->> k) IS DISTINCT FROM rt THEN
                RAISE NOTICE '% ms  %  master  Citus 路由表：master 把读写发往 % :%', lpad(t::text, 6), demo.slabel(p_tbl, sid),
                    rt, (SELECT port FROM demo.node WHERE name = rt);
                last := last || jsonb_build_object(k, rt);
            END IF;
            -- 稳定判据
            IF lead IS NULL OR (p_want IS NOT NULL AND lead <> p_want)
               OR reg IS DISTINCT FROM lead OR rt IS DISTINCT FROM lead THEN
                ok := false;
            ELSE
                lead_id := (SELECT raft_id FROM demo.node WHERE name = lead);
                FOREACH cur IN ARRAY sts LOOP
                    w := split_part(cur, '=', 1); n := string_to_array(split_part(cur, '=', 2), '|');
                    IF w <> lead AND n[1] <> 'down' AND NOT (n[1] = 'follower' AND n[3] = lead_id::text) THEN
                        ok := false;
                    END IF;
                END LOOP;
            END IF;
        END LOOP;
        IF ok THEN stable := stable + 1; ELSE stable := 0; END IF;
        EXIT WHEN stable >= 2;
        IF t > p_timeout_s * 1000 THEN
            RAISE NOTICE '% ms  （%s 内没有稳定下来）', lpad(t::text, 6), p_timeout_s;
            RETURN -1;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    RETURN t;
END $$;

-- 拍一张初始快照（与 watch 用同样的键），并打一行概要；watch 从它开始只报"变化"
CREATE FUNCTION demo.snap(p_tbl regclass, p_sids bigint[], p_t0 timestamptz, p_title text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE m jsonb := '{}'; sid bigint; w text; cur text; line text; reg text;
BEGIN
    FOREACH sid IN ARRAY p_sids LOOP
        line := NULL;
        FOR w IN SELECT demo.workers() LOOP
            cur := coalesce(demo.gstate(w, sid), 'down');
            m := m || jsonb_build_object(sid || ':' || w, cur);
            line := concat_ws('，', line, w || ' ' || CASE WHEN cur = 'down' THEN '✗宕机' WHEN cur = '-' THEN '无组'
                  ELSE split_part(cur, '|', 1) || '/任期' || split_part(cur, '|', 2) END);
        END LOOP;
        reg := demo.reg(sid);
        m := m || jsonb_build_object(sid || ':reg', coalesce(reg, '?') || '|' ||
                 coalesce((SELECT primary_term::text FROM partdist.partition_map WHERE partition_id = sid), '?'))
               || jsonb_build_object(sid || ':route', demo.route_node(sid));
        RAISE NOTICE '% ms  %  %：%；控制面登记 %；master 把读写路由到 %', lpad(demo.ms(p_t0)::text, 6), demo.slabel(p_tbl, sid), p_title,
            line, coalesce(reg, '还没有'), demo.route_node(sid);
    END LOOP;
    RETURN m;
END $$;

-- 等每个活着的节点把控制面（0 号组）已提交的日志都应用完 —— 新主上那条"登记为主"的应用事务提交之前，
-- 第一笔写入会和它抢该分片的复制认领位（见教程"已知问题"），所以切主 / 宕机后先等它落定再把控制权交给你
CREATE FUNCTION demo.wait_ctrl_applied(p_t0 timestamptz, p_timeout_s int) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n text; v text; ok bool; i int := 0; stable int := 0; lag text;
BEGIN
    LOOP
        ok := true; lag := NULL;
        FOR n IN SELECT name FROM demo.node ORDER BY port LOOP
            v := CASE WHEN n = 'master' THEN (SELECT commit_index || '|' || last_applied FROM partdist.pg_raft_group_status() WHERE group_id = 0)
                      ELSE demo.pq(n, 'SELECT commit_index||''|''||last_applied FROM partdist.pg_raft_group_status() WHERE group_id = 0') END;
            CONTINUE WHEN v IS NULL;                -- 宕机的节点不等
            IF split_part(v, '|', 1) <> split_part(v, '|', 2) THEN
                ok := false; lag := concat_ws('，', lag, n || ' 已提交 ' || split_part(v, '|', 1) || ' / 已应用 ' || split_part(v, '|', 2));
            END IF;
        END LOOP;
        IF ok THEN stable := stable + 1; ELSE stable := 0; END IF;
        IF stable >= 3 THEN
            RAISE NOTICE '% ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了', lpad(demo.ms(p_t0)::text, 6);
            RETURN demo.ms(p_t0);
        END IF;
        i := i + 1;
        IF i = 20 THEN RAISE NOTICE '% ms  master  等控制面在各节点应用完：%', lpad(demo.ms(p_t0)::text, 6), lag; END IF;
        IF demo.ms(p_t0) > p_timeout_s * 1000 THEN
            RAISE NOTICE '% ms  master  控制面还没在各节点应用完（%）', lpad(demo.ms(p_t0)::text, 6), lag;
            RETURN -1;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END $$;

-- 等副本确认最新提交：这些组里，每个活着的从的 Raft 提交位点都追上主的日志末尾。
-- 为什么要等：已知缺陷（见教程"已知问题"）—— 主刚提交的最后一笔，从要等主的下一次心跳才知道"已提交"；
-- 恰在这一拍里切主 / 宕机，新主升主时只回放到它已知的提交位点，那一笔会被判成中止。演示避开这一拍。
CREATE FUNCTION demo.wait_followers_committed(p_tbl regclass, p_sids bigint[], p_t0 timestamptz) RETURNS void LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; tail bigint; c bigint; ok bool; i int;
BEGIN
    FOR i IN 1..100 LOOP
        ok := true;
        FOREACH sid IN ARRAY p_sids LOOP
            lp := demo.leader(sid);
            CONTINUE WHEN lp IS NULL;
            tail := demo.pq(lp, format('SELECT max(last_log_index)::text FROM partdist.pg_raft_group_status() WHERE group_id = %s', sid))::bigint;
            FOR w IN SELECT x FROM demo.workers() x WHERE x <> lp LOOP
                c := demo.pq(w, format('SELECT max(commit_index)::text FROM partdist.pg_raft_group_status() WHERE group_id = %s', sid))::bigint;
                IF c IS NOT NULL AND c < tail THEN ok := false; END IF;
            END LOOP;
        END LOOP;
        IF ok THEN
            RAISE NOTICE '% ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）', lpad(demo.ms(p_t0)::text, 6);
            RETURN;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    RAISE NOTICE '% ms  （10 s 内副本没全部确认最新提交，继续）', lpad(demo.ms(p_t0)::text, 6);
END $$;

-- ════════════════════════════ 以下是演示里直接调用的函数 ════════════════════════════

-- ① 集群里的 4 个节点
CREATE FUNCTION demo.nodes()
RETURNS TABLE(节点 text, 端口 int, 类型 text, raft节点号 int, 状态 text, 控制面0号组 text)
LANGUAGE plpgsql AS $$
DECLARE r record; s text;
BEGIN
    FOR r IN SELECT * FROM demo.node ORDER BY port LOOP
        s := CASE WHEN r.name = 'master' THEN (SELECT state || '（任期 ' || current_term || '）' FROM partdist.pg_raft_group_status() WHERE group_id = 0)
                  ELSE demo.pq(r.name, 'SELECT state||''（任期 ''||current_term||''）'' FROM partdist.pg_raft_group_status() WHERE group_id = 0') END;
        节点 := r.name; 端口 := r.port; 类型 := r.kind; raft节点号 := r.raft_id;
        状态 := CASE WHEN s IS NULL THEN '✗ 宕机' ELSE '在线' END;
        控制面0号组 := coalesce(s, '—');
        RETURN NEXT;
    END LOOP;
END $$;

-- ② 每个分片落在哪个节点
CREATE FUNCTION demo.shards(p_tbl regclass)
RETURNS TABLE(分片 text, 分片号 bigint, 所在节点 text, 端口 int, 哈希范围 text, 行数 bigint, worker上的表名 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; col text;
BEGIN
    col := (SELECT column_to_column_name(logicalrelid, partkey) FROM pg_dist_partition WHERE logicalrelid = p_tbl);
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        分片 := demo.slabel(p_tbl, sid); 分片号 := sid;
        所在节点 := demo.route_node(sid);
        端口 := (SELECT port FROM demo.node WHERE name = 所在节点);
        哈希范围 := (SELECT '[' || shardminvalue || ', ' || shardmaxvalue || ']' FROM pg_dist_shard WHERE shardid = sid);
        EXECUTE format('SELECT count(*) FROM %s WHERE get_shard_id_for_distribution_column(%L, %I) = %s',
                       p_tbl, p_tbl::text, col, sid) INTO 行数;
        worker上的表名 := p_tbl::text || '_' || sid;
        RETURN NEXT;
    END LOOP;
END $$;

-- ③ 某个键落在哪个分片、现在由哪个节点服务
CREATE FUNCTION demo.locate(p_tbl regclass, p_key int)
RETURNS TABLE(键 int, 分片 text, 分片号 bigint, 当前主节点 text, 端口 int)
LANGUAGE plpgsql AS $$
BEGIN
    键 := p_key;
    分片号 := get_shard_id_for_distribution_column(p_tbl, p_key);
    分片 := demo.slabel(p_tbl, 分片号);
    当前主节点 := demo.route_node(分片号);
    端口 := (SELECT port FROM demo.node WHERE name = 当前主节点);
    RETURN NEXT;
END $$;

-- ④ 每个分片自动建一个 Raft 组（成员 = 3 台 worker）—— **不做任何人工干预**：
--    建完组三台各自倒计时，谁先到点谁竞选，全过程实时打出来。
CREATE FUNCTION demo.raft_elect(p_tbl regclass)
RETURNS TABLE(分片 text, 分片号 bigint, 数据在 text, leader text, followers text, 任期 text, 选举轮次 int, 控制面登记的主 text, master路由到 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; w text; t0 timestamptz; t int; sids bigint[]; holders text;
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        PERFORM demo.q(w, 'SELECT partdist.rebuild_shard_identity()::text');
    END LOOP;
    INSERT INTO demo.managed VALUES (p_tbl::text, demo.sids(p_tbl))
        ON CONFLICT (tbl) DO UPDATE SET shardids = EXCLUDED.shardids;
    sids := demo.sids(p_tbl);
    holders := (SELECT string_agg(demo.slabel(p_tbl, x) || '→' || demo.route_node(x), '，' ORDER BY x) FROM unnest(sids) x);
    t0 := clock_timestamp();
    RAISE NOTICE '在 3 台 worker 上把 % 个组都建出来（成员 = %），然后**不做任何干预**，等它们自发选主',
        cardinality(sids), (SELECT string_agg(name, ' ' ORDER BY port) FROM demo.node WHERE kind = 'worker');
    RAISE NOTICE '此刻数据的分布：% —— 另外两台手里是空的',  holders;
    RAISE NOTICE '真实环境里就是这样：谁的选举超时（本演示 % s）先到点谁就竞选，赢家是随机的。',
        demo.demo_election_ms() / 1000;
    RAISE NOTICE '若先当选的那台没有这个分片的数据，它的升主前置会拒绝（日志：拒绝升主：本节点没有该分片的本地副本），';
    RAISE NOTICE '然后主动让位、退避 5 个选举周期 —— 所以下面可能看到"当选又退位"，直到有数据的那台当选才会登记。';
    FOREACH sid IN ARRAY sids LOOP
        FOR w IN SELECT demo.workers() LOOP
            PERFORM demo.q(w, format('SELECT partdist.pg_raft_group_create(%s, %s)::text', sid, demo.members()));
        END LOOP;
    END LOOP;
    RAISE NOTICE '% ms  组已建好，开始等自发竞选 ──────', lpad(demo.ms(t0)::text, 6);
    -- p_want = NULL：不指定谁当选；稳定判据是"有 leader 且控制面已登记它、路由也指向它"，
    -- 没数据的节点当选后登记不了，于是观察器会继续等到真正有资格的那台上位。
    t := demo.watch(p_tbl, sids, NULL, 240, t0);
    IF t < 0 THEN RAISE NOTICE '等待超时：到现在还没全部稳定（看上面的时间线）'; END IF;
    RAISE NOTICE '% ms  全部就位（含新 leader 的升主前置：追平日志、认领无主 xid，然后才上报控制面）',
        lpad(demo.ms(t0)::text, 6);
    FOREACH sid IN ARRAY sids LOOP
        分片 := demo.slabel(p_tbl, sid); 分片号 := sid;
        数据在 := demo.route_node(sid);
        leader := demo.leader(sid);
        followers := (SELECT string_agg(x, ', ') FROM demo.workers() x WHERE x IS DISTINCT FROM leader);
        任期 := split_part(demo.gstate(leader, sid), '|', 2);
        选举轮次 := nullif(任期, '')::int;     -- 任期 N = 一共选了 N 轮（N>1 即有人当选后被拒、让位）
        控制面登记的主 := demo.reg(sid);
        master路由到 := demo.route_node(sid) || ' :' || (SELECT port FROM demo.node WHERE name = demo.route_node(sid));
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑤ 在各组的主上把副本供到其余两台 → 最终的主从
CREATE FUNCTION demo.raft_replicas(p_tbl regclass)
RETURNS TABLE(分片 text, 主 text, 副本供到 text, 基线游标 text, 结果 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; r text; a text; t int;
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid);
        FOR w IN SELECT x FROM demo.workers() x WHERE x <> lp LOOP
            分片 := demo.slabel(p_tbl, sid); 主 := lp; 副本供到 := w;
            BEGIN
                r := demo.q(lp, format('SELECT partdist.provision_shard_replica(%s, %s)', sid,
                                       (SELECT raft_id FROM demo.node WHERE name = w)));
                基线游标 := substring(r FROM 'base=([0-9]+)');
                结果 := '已供：物理基线进分区流 → ' || w || ' 配对文件号、arm 回放槽位';
            EXCEPTION WHEN OTHERS THEN
                基线游标 := NULL; 结果 := '失败：' || left(SQLERRM, 120);
            END;
            RETURN NEXT;
        END LOOP;
    END LOOP;
    -- 补供：没 armed 的副本从当前主再供一遍
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid);
        FOR w IN SELECT x FROM demo.workers() x WHERE x <> lp LOOP
            a := demo.pq(w, format('SELECT coalesce((SELECT armed::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)), ''none'')', sid));
            IF a IS DISTINCT FROM 'true' THEN
                RAISE NOTICE '% 在 % 上的回放槽位没 armed，从当前主 % 补供', demo.slabel(p_tbl, sid), w, lp;
                PERFORM demo.q(lp, format('SELECT partdist.provision_shard_replica(%s, %s)', sid, (SELECT raft_id FROM demo.node WHERE name = w)));
            END IF;
        END LOOP;
    END LOOP;
    -- 等控制面登记与各组主一致
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        FOR t IN 1..120 LOOP
            EXIT WHEN demo.reg(sid) = demo.leader(sid) AND demo.route_node(sid) = demo.leader(sid);
            PERFORM pg_sleep(0.5);
        END LOOP;
    END LOOP;
END $$;

-- ⑥ 各分片的 Raft 组：每台 worker 在组里的角色、任期、日志位点
CREATE FUNCTION demo.raft_groups(p_tbl regclass)
RETURNS TABLE(分片 text, 节点 text, 角色 text, 任期 bigint, 认定的leader text, 日志末尾 bigint, 已提交 bigint, 已应用 bigint)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; w text; v text; f text[];
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        FOR w IN SELECT demo.workers() LOOP
            v := demo.pq(w, format('SELECT coalesce((SELECT concat_ws(''|'', state, current_term, leader_node_id, last_log_index, commit_index, last_applied, '
                                   '(SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = %1$s)) '
                                   'FROM partdist.pg_raft_group_status() WHERE group_id = %1$s ORDER BY (state = ''leader'') DESC, (state = ''follower'') DESC LIMIT 1), ''-'')', sid));
            分片 := demo.slabel(p_tbl, sid); 节点 := w;
            IF v IS NULL THEN 角色 := '✗ 宕机'; 任期 := NULL; 认定的leader := NULL; 日志末尾 := NULL; 已提交 := NULL; 已应用 := NULL;
            ELSIF v = '-' THEN 角色 := '—'; 任期 := NULL; 认定的leader := NULL; 日志末尾 := NULL; 已提交 := NULL; 已应用 := NULL;
            ELSE
                f := string_to_array(v, '|');
                角色 := CASE f[1] WHEN 'leader' THEN '★ leader' ELSE f[1] END || CASE WHEN f[7]::int > 1 THEN ' ⚠重复槽位' ELSE '' END;
                任期 := f[2]::bigint; 认定的leader := demo.node_of_raft(f[3]::int);
                日志末尾 := f[4]::bigint; 已提交 := f[5]::bigint; 已应用 := f[6]::bigint;
            END IF;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑦ 同一节点上的混合角色一览（行 = 节点，列 = 分片）
CREATE FUNCTION demo.roles(p_tbl regclass)
RETURNS TABLE(节点 text, "S1" text, "S2" text, "S3" text, 小结 text)
LANGUAGE plpgsql AS $$
DECLARE w text; sid bigint; v text; f text[]; cells text[]; nl int; nf int; i int;
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        cells := '{}'; nl := 0; nf := 0;
        FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
            v := demo.pq(w, format('SELECT concat_ws(''|'', coalesce((SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = %1$s ORDER BY (state = ''leader'') DESC, (state = ''follower'') DESC LIMIT 1), ''-''), '
                                   'coalesce((SELECT current_term::text FROM partdist.pg_raft_group_status() WHERE group_id = %1$s ORDER BY (state = ''leader'') DESC, (state = ''follower'') DESC LIMIT 1), ''''), '
                                   'coalesce((SELECT armed::text||''/''||applied FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%1$s)), ''''), '
                                   '(SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = %1$s))', sid));
            IF v IS NULL THEN cells := cells || '✗ 宕机'::text; CONTINUE; END IF;
            f := string_to_array(v, '|');
            IF f[4]::int > 1 THEN cells := cells || format('⚠ 重复槽位（%s）', f[1]); nf := nf + 1;
            ELSIF f[1] = 'leader' THEN cells := cells || format('★ leader（任期%s）', f[2]); nl := nl + 1;
            ELSIF f[1] = 'follower' THEN
                cells := cells || format('follower（%s）', CASE WHEN f[3] = '' THEN '无回放槽'
                                                          WHEN split_part(f[3], '/', 1) = 'true' THEN '回放到 ' || split_part(f[3], '/', 2)
                                                          ELSE '回放槽未 armed' END);
                nf := nf + 1;
            ELSE cells := cells || f[1]; END IF;
        END LOOP;
        节点 := w || ' :' || (SELECT port FROM demo.node WHERE name = w);
        "S1" := cells[1]; "S2" := cells[2]; "S3" := cells[3];
        小结 := CASE WHEN cells[1] = '✗ 宕机' THEN '宕机' ELSE format('%s 个 leader + %s 个 follower', nl, nf) END;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑧ 路由信息：三层 —— Citus 路由表 / 控制面登记 / 节点本地角色
CREATE FUNCTION demo.routing(p_tbl regclass)
RETURNS TABLE(分片 text, 层 text, 节点 text, 内容 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; w text; v text; m record;
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        分片 := demo.slabel(p_tbl, sid);
        层 := '① Citus 路由表（在 master 上）：发往 →'; 节点 := demo.route_node(sid);
        内容 := 'master 把这个分片的读写都发往 ' || 节点 || ' :' || (SELECT port FROM demo.node WHERE name = 节点)
                || '（路由只由 master 做，这一列是"发给谁"）';
        RETURN NEXT;
        SELECT * INTO m FROM partdist.partition_map WHERE partition_id = sid;
        层 := '② 控制面登记（0 号组 partition_map）：主 →'; 节点 := demo.node_of_raft(m.primary_node);
        内容 := format('主 = %s，从 = %s，登记任期 %s', 节点,
                     (SELECT string_agg(demo.node_of_raft(x), ',') FROM unnest(m.secondary_nodes) x), m.primary_term);
        RETURN NEXT;
        FOR w IN SELECT demo.workers() LOOP
            v := demo.pq(w, format('SELECT coalesce(partdist.route_status(partdist.local_partition_for_shard(%s)), ''（没有这个分片）'')', sid));
            层 := '③ 节点本地（route_status）'; 节点 := w;
            内容 := CASE WHEN v IS NULL THEN '✗ 宕机'
                        ELSE (CASE WHEN v LIKE '%captured=yes%' THEN '主：写入被捕获进分区流；' ELSE '从：只收流、不接受写；' END) || v END;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑨ 流控信息：Raft 日志环（每组）+ 分区流捕获环（每节点）
CREATE FUNCTION demo.flow(p_tbl regclass)
RETURNS TABLE(分片 text, 节点 text, 角色 text, 日志环深度 bigint, 环容量 int, 环满背压等待 bigint, 环满丢弃 bigint,
              多数派不足丢弃 bigint, 捕获环未消费 int, 捕获环覆盖 bigint, 写路径背压排空 bigint)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; w text; v text; f text[];
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        FOR w IN SELECT demo.workers() LOOP
            v := demo.pq(w, format(
                'SELECT concat_ws(''|'', coalesce((SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = %1$s ORDER BY (state = ''leader'') DESC, (state = ''follower'') DESC LIMIT 1), ''-''), '
                'f.ring_depth, f.ring_capacity, f.ring_full_waits, f.ring_full_drops, f.quorum_drops, '
                'r.unconsumed, r.overwrites, r.backpressure_flushes) '
                'FROM partdist.partwal_ring_stats() r LEFT JOIN partdist.pg_raft_group_flow_stats() f ON f.group_id = %1$s', sid));
            分片 := demo.slabel(p_tbl, sid); 节点 := w;
            IF v IS NULL THEN 角色 := '✗ 宕机'; 日志环深度 := NULL; 环容量 := NULL; 环满背压等待 := NULL; 环满丢弃 := NULL;
                多数派不足丢弃 := NULL; 捕获环未消费 := NULL; 捕获环覆盖 := NULL; 写路径背压排空 := NULL;
            ELSE
                f := string_to_array(v, '|');
                角色 := f[1]; 日志环深度 := nullif(f[2], '')::bigint; 环容量 := nullif(f[3], '')::int;
                环满背压等待 := nullif(f[4], '')::bigint; 环满丢弃 := nullif(f[5], '')::bigint; 多数派不足丢弃 := nullif(f[6], '')::bigint;
                捕获环未消费 := nullif(f[7], '')::int; 捕获环覆盖 := nullif(f[8], '')::bigint; 写路径背压排空 := nullif(f[9], '')::bigint;
            END IF;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑩ 开一个全局事务（跨分片写必须）：取 gxid + TSO start_ts，SET LOCAL join_info（Citus 传播到每条分片连接）
CREATE FUNCTION demo.global_txn(p_coord_shard bigint DEFAULT 0) RETURNS text LANGUAGE plpgsql AS $$
DECLARE g bigint; ts bigint;
BEGIN
    IF transaction_timestamp() = statement_timestamp() THEN
        RAISE EXCEPTION '请先 BEGIN，再在事务里调用 demo.global_txn()';
    END IF;
    g := partdist.partdist_gxid_next();
    ts := partdist.partdist_tso_client_start_ts();
    EXECUTE 'SET LOCAL citus.propagate_set_commands = ''local''';
    EXECUTE format('SET LOCAL pg_partdist.join_info = %L', format('%s,%s,%s', g, ts, p_coord_shard));
    RETURN format('已加入全局事务：gxid = %s，start_ts = %s（本事务在所有分片上共用这一个快照）', g, ts);
END $$;

-- ⑪ 分片级 xid 分配器：每个分片自己发号，与节点原生 xid 无关
CREATE FUNCTION demo.xid(p_tbl regclass)
RETURNS TABLE(分片 text, 主节点 text, 下一个分片xid bigint, 主的持久化水位 text, 主节点的原生xid bigint, 各从学到的水位 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; v text; reps text;
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid);
        分片 := demo.slabel(p_tbl, sid); 主节点 := lp;
        v := demo.pq(lp, format('SELECT partdist.shard_xid_next(partdist.local_partition_for_shard(%1$s)::oid) || ''|'' || '
                                'substring(partdist.route_status(partdist.local_partition_for_shard(%1$s)) from ''xid_watermark=([0-9]+)'') || ''|'' || '
                                'txid_snapshot_xmax(txid_current_snapshot())', sid));
        下一个分片xid := split_part(v, '|', 1)::bigint;
        主的持久化水位 := split_part(v, '|', 2) || '（按 4096 一批落盘，崩溃后从批次上界续发）';
        主节点的原生xid := split_part(v, '|', 3)::bigint;
        reps := NULL;
        FOR w IN SELECT x FROM demo.workers() x WHERE x IS DISTINCT FROM lp LOOP
            v := demo.pq(w, format('SELECT coalesce(substring(partdist.route_status(partdist.local_partition_for_shard(%s)) from ''xid_watermark=([0-9]+)''), ''?'')', sid));
            reps := concat_ws('，', reps, w || '=' || coalesce(v, '宕机'));
        END LOOP;
        各从学到的水位 := reps;
        RETURN NEXT;
    END LOOP;
END $$;

-- 页面上的元组版本（在该分片当前的主上读）；sku 由元组数据的第一列（int4）解出
CREATE FUNCTION demo.page_versions_sql(p_rel text) RETURNS text LANGUAGE sql IMMUTABLE AS $f$
    SELECT format($q$
      SELECT b.blk, h.lp, h.t_xmin::text::bigint AS xmin, h.t_xmax::text::bigint AS xmax,
             (CASE WHEN u >= 2147483648 THEN u - 4294967296 ELSE u END)::int AS id
        FROM generate_series(0, (pg_relation_size(%1$L) / current_setting('block_size')::int)::int - 1) b(blk),
             LATERAL heap_page_items(get_raw_page(%1$L, b.blk)) h,
             LATERAL (SELECT get_byte(h.t_data,0) + get_byte(h.t_data,1)*256 + get_byte(h.t_data,2)*65536
                             + get_byte(h.t_data,3)::bigint*16777216 AS u) x
       WHERE h.lp_len > 0 $q$, p_rel)
$f$;

-- ⑫ 分片级 clog：该分片发出去的每个分片 xid 的判决、start_ts、commit_ts，以及它写入/删改了哪些行
CREATE FUNCTION demo.clog(p_tbl regclass, p_shard text)
RETURNS TABLE(分片xid bigint, 判决 text, start_ts bigint, commit_ts bigint, 写入的行 text, 删改的行 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint := demo.sid(p_tbl, p_shard); lp text := demo.leader(sid); rel text; r record;
BEGIN
    rel := p_tbl::text || '_' || sid;
    RAISE NOTICE '% 当前的主是 %，下面是它那本分片 clog（st：0 空/运行中 1 PREPARED 2 COMMITTED 3 ABORTED）', p_shard, lp;
    FOR r IN SELECT * FROM demo.dblink(demo.conn(lp), format($q$
        WITH v AS (%1$s), x AS (SELECT generate_series(3::bigint, partdist.shard_xid_next(partdist.local_partition_for_shard(%2$s)::oid) - 1) AS xid)
        SELECT x.xid, partdist.shard_clog_status_full(partdist.local_partition_for_shard(%2$s)::oid, x.xid),
               (SELECT string_agg(id::text, ',' ORDER BY id) FROM v WHERE v.xmin = x.xid),
               (SELECT string_agg(id::text, ',' ORDER BY id) FROM v WHERE v.xmax = x.xid)
          FROM x ORDER BY 1 $q$, demo.page_versions_sql(rel), sid))
        AS t(xid bigint, st text, ins text, del text)
    LOOP
        分片xid := r.xid;
        判决 := CASE substring(r.st from 'st=([0-9]+)') WHEN '0' THEN '0 空/运行中' WHEN '1' THEN '1 PREPARED'
                   WHEN '2' THEN '2 COMMITTED' WHEN '3' THEN '3 ABORTED' ELSE r.st END;
        start_ts := substring(r.st from 'sts=([0-9]+)')::bigint;
        commit_ts := substring(r.st from 'cts=([0-9]+)')::bigint;
        写入的行 := r.ins; 删改的行 := r.del;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑬ 页面上的多版本：每个元组版本的 xmin/xmax（分片 xid）与分片 clog 判决
CREATE FUNCTION demo.versions(p_tbl regclass, p_shard text)
RETURNS TABLE(位置 text, id int, xmin bigint, xmin判决 text, xmax bigint, xmax判决 text, 现在可见 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint := demo.sid(p_tbl, p_shard); lp text := demo.leader(sid); rel text; r record;
BEGIN
    rel := p_tbl::text || '_' || sid;
    RAISE NOTICE '% 当前的主是 %；元组头里的 xmin/xmax 是**分片 xid**，查的是这个分片自己的 clog', p_shard, lp;
    FOR r IN SELECT * FROM demo.dblink(demo.conn(lp), format($q$
        WITH v AS (%1$s)
        SELECT '(' || blk || ',' || lp || ')', id, xmin,
               partdist.shard_clog_status_full(partdist.local_partition_for_shard(%2$s)::oid, xmin),
               xmax,
               CASE WHEN xmax = 0 THEN '' ELSE partdist.shard_clog_status_full(partdist.local_partition_for_shard(%2$s)::oid, xmax) END
          FROM v ORDER BY blk, lp $q$, demo.page_versions_sql(rel), sid))
        AS t(pos text, id int, xmin bigint, xst text, xmax bigint, mst text)
    LOOP
        位置 := r.pos; id := r.id; xmin := r.xmin; xmax := r.xmax;
        xmin判决 := r.xst; xmax判决 := nullif(r.mst, '');
        现在可见 := CASE WHEN r.xst NOT LIKE 'st=2%' THEN '否（插入者未提交）'
                        WHEN r.xmax <> 0 AND r.mst LIKE 'st=2%' THEN '否（已被删/改）'
                        ELSE '是' END;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑭ 惰性回放：从副本只把流落盘，不马上 redo
CREATE FUNCTION demo.replay(p_tbl regclass)
RETURNS TABLE(分片 text, 主 text, 主的流位点 bigint, 从 text, 从已收到 bigint, 从已回放 bigint, 待回放 bigint, 回放槽 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; v text; f text[]; tip bigint;
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid);
        tip := demo.pq(lp, format('SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(%s))::text', sid))::bigint;
        FOR w IN SELECT x FROM demo.workers() x WHERE x IS DISTINCT FROM lp LOOP
            v := demo.pq(w, format('SELECT concat_ws(''|'', partdist.get_follower_applied_part_lsn(partdist.local_partition_for_shard(%1$s)), '
                                   'coalesce((SELECT applied::text||''|''||armed::text||''|''||state FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%1$s)), ''||''))', sid));
            分片 := demo.slabel(p_tbl, sid); 主 := lp; 主的流位点 := tip; 从 := w;
            IF v IS NULL THEN 从已收到 := NULL; 从已回放 := NULL; 待回放 := NULL; 回放槽 := '✗ 宕机';
            ELSE
                f := string_to_array(v, '|');
                从已收到 := nullif(f[1], '')::bigint; 从已回放 := nullif(f[2], '')::bigint;
                待回放 := 从已收到 - 从已回放;
                回放槽 := CASE WHEN f[3] = 'true' THEN 'armed，' || f[4] ELSE coalesce(nullif(f[3], ''), '无') END;
            END IF;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑮ 触发惰性回放：让每个从追平到它已收到的位点
CREATE FUNCTION demo.catchup(p_tbl regclass)
RETURNS TABLE(分片 text, 从 text, 回放前 bigint, 目标 bigint, 回放后 bigint, 耗时_ms int)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; tip bigint; recv bigint; t0 timestamptz;
BEGIN
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid);
        tip := demo.pq(lp, format('SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(%s))::text', sid))::bigint;
        FOR w IN SELECT x FROM demo.workers() x WHERE x IS DISTINCT FROM lp LOOP
            分片 := demo.slabel(p_tbl, sid); 从 := w;
            回放前 := demo.pq(w, format('SELECT applied::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)', sid))::bigint;
            IF 回放前 IS NULL THEN 目标 := NULL; 回放后 := NULL; 耗时_ms := NULL; RETURN NEXT; CONTINUE; END IF;
            recv := demo.pq(w, format('SELECT partdist.get_follower_applied_part_lsn(partdist.local_partition_for_shard(%s))::text', sid))::bigint;
            目标 := least(tip, recv);
            t0 := clock_timestamp();
            BEGIN
                PERFORM demo.q(w, format('SELECT partdist.replay_catchup(partdist.local_partition_for_shard(%s)::regclass, %s, 60000)::text', sid, 目标));
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE '% 在 % 上追平失败：%', 分片, w, left(SQLERRM, 150);
            END;
            耗时_ms := demo.ms(t0);
            回放后 := demo.pq(w, format('SELECT applied::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)', sid))::bigint;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑯ 副本与主逐字节比对（主堆 + 主键索引，按内核 heap_mask 口径掩掉提示位等可变部分）
CREATE FUNCTION demo.compare(p_tbl regclass)
RETURNS TABLE(分片 text, 主 text, 从 text, 主堆 text, 主键索引 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; lp text; w text; rel text; kind text; a text; b text; res text; tail_gap bigint;
BEGIN
    IF to_regclass('pg_temp.demo_cmp_out') IS NULL THEN CREATE TEMP TABLE demo_cmp_out(line text); END IF;
    FOR w IN SELECT demo.workers() LOOP        -- 每个节点做一次 CHECKPOINT，把页面刷到盘上再比
        IF demo.pq(w, 'SELECT 1::text') IS NOT NULL THEN PERFORM demo.dblink_exec(demo.conn(w), 'CHECKPOINT'); END IF;
    END LOOP;
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        lp := demo.leader(sid); rel := p_tbl::text || '_' || sid;
        FOR w IN SELECT x FROM demo.workers() x WHERE x IS DISTINCT FROM lp LOOP
            分片 := demo.slabel(p_tbl, sid); 主 := lp; 从 := w;
            IF demo.pq(w, 'SELECT 1::text') IS NULL THEN 主堆 := '✗ 宕机'; 主键索引 := '✗ 宕机'; RETURN NEXT; CONTINUE; END IF;
            tail_gap := demo.pq(lp, format('SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(%s))::text', sid))::bigint
                      - demo.pq(w, format('SELECT partdist.get_follower_applied_part_lsn(partdist.local_partition_for_shard(%s))::text', sid))::bigint;
            FOREACH kind IN ARRAY ARRAY['heap', 'btree'] LOOP
                IF kind = 'heap' THEN
                    a := demo.q(lp, format('SELECT pg_relation_filepath(%L)', rel));
                    b := demo.q(w, format('SELECT pg_relation_filepath(%L)', rel));
                ELSE
                    a := demo.q(lp, format('SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid = %L::regclass AND indisprimary', rel));
                    b := demo.q(w, format('SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid = %L::regclass AND indisprimary', rel));
                END IF;
                TRUNCATE demo_cmp_out;
                EXECUTE format('COPY demo_cmp_out FROM PROGRAM %L',
                    format('python3 /tmp/pagecmp.py --kind=%s %s/%s %s/%s 2>&1 | tail -1', kind,
                           (SELECT datadir FROM demo.node WHERE name = lp), a, (SELECT datadir FROM demo.node WHERE name = w), b));
                res := (SELECT line FROM demo_cmp_out LIMIT 1);
                res := CASE WHEN res = 'IDENTICAL_OUTSIDE_HOLE' THEN '✓ 逐字节一致' ELSE '✗ ' || coalesce(res, '?') END;
                IF res LIKE '✗%' AND tail_gap > 0 THEN
                    res := res || format('（主的流尾还有 %s 条没复制：中止事务留下的，下一笔提交会一并带过去）', tail_gap);
                END IF;
                IF kind = 'heap' THEN 主堆 := res; ELSE 主键索引 := res; END IF;
            END LOOP;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- ⑰ 手动切换 leader（受控切主）：在目标节点上对这一个组发起竞选，过程实时打出来
CREATE FUNCTION demo.switch_leader(p_tbl regclass, p_shard text, p_to text)
RETURNS TABLE(分片 text, 原来的主 text, 新主 text, 新任期 text, 切换耗时_ms int, 原主换下时的下一个分片xid text,
              新主接着发的下一个分片xid text, 原主重新成为副本 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint := demo.sid(p_tbl, p_shard); old text; t0 timestamptz; t int; a text; i int; xo text; base jsonb;
BEGIN
    old := demo.leader(sid);
    IF old = p_to THEN RAISE EXCEPTION '% 现在的主已经是 %', p_shard, p_to; END IF;
    a := demo.pq(p_to, format('SELECT coalesce((SELECT armed::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)), ''none'')', sid));
    IF a IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION '% 在 % 上没有 armed 的回放槽位，不能当选（按设计，候选人必须是完整回放的副本）', p_shard, p_to;
    END IF;
    xo := demo.pq(old, format('SELECT partdist.shard_xid_next(partdist.local_partition_for_shard(%s)::oid)::text', sid));
    RAISE NOTICE '切换前：% 的主 = %，下一个分片 xid = %', p_shard, old, xo;
    t0 := clock_timestamp();
    PERFORM demo.wait_followers_committed(p_tbl, ARRAY[sid], t0);
    base := demo.snap(p_tbl, ARRAY[sid], t0, '切换前');
    PERFORM demo.q(p_to, format('SELECT partdist.pg_raft_group_campaign(%s)::text', sid));
    RAISE NOTICE '% ms  %  %  pg_raft_group_campaign：对 % 的组发起竞选', lpad(demo.ms(t0)::text, 6), p_shard, p_to, p_shard;
    t := demo.watch(p_tbl, ARRAY[sid], p_to, 90, t0, base);
    PERFORM demo.wait_ctrl_applied(t0, 180);
    分片 := p_shard; 原来的主 := old; 新主 := demo.leader(sid);
    新任期 := split_part(demo.gstate(新主, sid), '|', 2); 切换耗时_ms := t;
    原主换下时的下一个分片xid := xo;
    新主接着发的下一个分片xid := demo.pq(新主, format('SELECT partdist.shard_xid_next(partdist.local_partition_for_shard(%s)::oid)::text', sid));
    原主重新成为副本 := '（120 s 内没等到）';
    FOR i IN 1..240 LOOP
        a := demo.pq(old, format('SELECT coalesce((SELECT armed::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)), ''none'')', sid));
        IF a = 'true' THEN
            原主重新成为副本 := format('是：切换后 %s ms 被新主自动重新供给（armed）', demo.ms(t0));
            RAISE NOTICE '% ms  %  %  旧主被新主自动重新供给成副本（回放槽位 armed）', lpad(demo.ms(t0)::text, 6), p_shard, old;
            EXIT;
        END IF;
        PERFORM pg_sleep(0.5);
    END LOOP;
    RETURN NEXT;
END $$;

-- ⑱ 模拟不可抗力宕机：pg_ctl -m immediate（不做 checkpoint，等同断电），看 Raft 自动选主
CREATE FUNCTION demo.crash(p_tbl regclass, p_node text)
RETURNS TABLE(分片 text, 宕机前的主 text, 宕机后的主 text, 任期 text, 不可用时长_ms int)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; t0 timestamptz; t int; before jsonb := '{}'; d text; pgctl text := '/work/pg-install/bin/pg_ctl'; base jsonb;
BEGIN
    IF p_node NOT IN (SELECT demo.workers()) THEN RAISE EXCEPTION '只能停 worker（%）', (SELECT string_agg(x, ' ') FROM demo.workers() x); END IF;
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        before := before || jsonb_build_object(sid::text, demo.leader(sid));
    END LOOP;
    d := (SELECT datadir FROM demo.node WHERE name = p_node);
    RAISE NOTICE '宕机前：% 是 % 的主；其余分片它只是从', p_node,
        (SELECT coalesce(string_agg(demo.slabel(p_tbl, k::bigint), '、'), '（没有分片）') FROM jsonb_each_text(before) e(k, v) WHERE v = p_node);
    t0 := clock_timestamp();
    PERFORM demo.wait_followers_committed(p_tbl, demo.sids(p_tbl), t0);
    base := demo.snap(p_tbl, demo.sids(p_tbl), t0, '宕机前');
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', format('%s -D %s -m immediate stop -w -t 60 >/dev/null 2>&1', pgctl, d));
    RAISE NOTICE '% ms  %  pg_ctl -m immediate stop：进程直接退出，不做 checkpoint（模拟断电）', lpad(demo.ms(t0)::text, 6), p_node;
    t := demo.watch(p_tbl, demo.sids(p_tbl), NULL, 120, t0, base);
    PERFORM demo.wait_ctrl_applied(t0, 400);
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        分片 := demo.slabel(p_tbl, sid); 宕机前的主 := before ->> sid::text; 宕机后的主 := demo.leader(sid);
        任期 := split_part(demo.gstate(宕机后的主, sid), '|', 2);
        不可用时长_ms := CASE WHEN 宕机前的主 = p_node THEN t ELSE 0 END;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑲ 把宕机的节点拉起来：它以 follower 身份归队，原来当主的分片被新主自动重新供给
CREATE FUNCTION demo.recover(p_tbl regclass, p_node text)
RETURNS TABLE(分片 text, 当前的主 text, 节点 text, 在组里的角色 text, 回放槽位 text)
LANGUAGE plpgsql AS $$
DECLARE sid bigint; t0 timestamptz; t int; d text; i int; a text; done jsonb := '{}'; pending int;
        pgctl text := '/work/pg-install/bin/pg_ctl'; base jsonb; dups text;
BEGIN
    d := (SELECT datadir FROM demo.node WHERE name = p_node);
    t0 := clock_timestamp();
    base := demo.snap(p_tbl, demo.sids(p_tbl), t0, '拉起前');
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', format('%s -D %s status >/dev/null 2>&1 || %s -D %s -l %s/pg.log -o "-p %s" start -w -t 60 >/dev/null 2>&1',
                   pgctl, d, pgctl, d, d, (SELECT port FROM demo.node WHERE name = p_node)));
    RAISE NOTICE '% ms  %  pg_ctl start：进程起来了，开始重新加入各个组', lpad(demo.ms(t0)::text, 6), p_node;
    t := demo.watch(p_tbl, demo.sids(p_tbl), NULL, 120, t0, base);
    PERFORM demo.wait_ctrl_applied(t0, 180);
    -- ⚠ 已知缺陷：节点重启时多个后端并发"按注册表恢复 / 按通告建组"，同一个组可能被建成 2 个槽位；
    --   多出来的那个会反复竞选、打扰该组的主。槽位只在共享内存里，再重启一次即可消除。
    FOR i IN 1..5 LOOP
        dups := demo.pq(p_node, 'SELECT string_agg(group_id::text, '','') FROM (SELECT group_id FROM partdist.pg_raft_group_status() GROUP BY 1 HAVING count(*) > 1) d');
        EXIT WHEN dups IS NULL;
        IF i = 5 THEN
            RAISE NOTICE '% ms  %  ⚠ 重启 4 次后组 % 仍有重复槽位，请手动再重启一次该节点', lpad(demo.ms(t0)::text, 6), p_node, dups;
            EXIT;
        END IF;
        RAISE NOTICE '% ms  %  ⚠ 组 % 在本节点被建成了 2 个槽位（已知缺陷：重启时多个后端并发建组），多出的那个会反复竞选、打扰该组的主 —— 再重启一次 % 消除',
            lpad(demo.ms(t0)::text, 6), p_node, dups, p_node;
        EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', format('%s -D %s -l %s/pg.log -o "-p %s" restart -m fast -w -t 60 >/dev/null 2>&1',
                       pgctl, d, d, (SELECT port FROM demo.node WHERE name = p_node)));
        RAISE NOTICE '% ms  %  已重启', lpad(demo.ms(t0)::text, 6), p_node;
        base := demo.snap(p_tbl, demo.sids(p_tbl), t0, '重启后');
        t := demo.watch(p_tbl, demo.sids(p_tbl), NULL, 120, t0, base);
        PERFORM demo.wait_ctrl_applied(t0, 180);
    END LOOP;
    FOR i IN 1..480 LOOP
        IF i = 90 THEN      -- 45 s 还没 armed：新主那边的自动归队工作者可能已过期（它只等约 5 分钟），手动触发一次
            FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
                CONTINUE WHEN demo.leader(sid) = p_node OR done ? sid::text;
                RAISE NOTICE '% ms  %  %  等了 45 s 还没 armed（新主的自动归队工作者只等约 5 分钟，节点停得更久就要手动触发）→ 在 % 上 reprovision_demoted',
                    lpad(demo.ms(t0)::text, 6), demo.slabel(p_tbl, sid), p_node, demo.leader(sid);
                BEGIN
                    PERFORM demo.q(demo.leader(sid), format('SELECT partdist.reprovision_demoted(%s, %s)', sid, (SELECT raft_id FROM demo.node WHERE name = p_node)));
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE '        重供失败：%', left(SQLERRM, 150);
                END;
            END LOOP;
        END IF;
        pending := 0;
        FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
            CONTINUE WHEN demo.leader(sid) = p_node OR done ? sid::text;
            a := demo.pq(p_node, format('SELECT coalesce((SELECT armed::text FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)), ''none'')', sid));
            IF a = 'true' THEN
                RAISE NOTICE '% ms  %  %  回放槽位 armed —— 已是 % 的合格副本', lpad(demo.ms(t0)::text, 6), demo.slabel(p_tbl, sid), p_node, demo.leader(sid);
                done := done || jsonb_build_object(sid::text, true);
            ELSE pending := pending + 1; END IF;
        END LOOP;
        EXIT WHEN pending = 0;
        PERFORM pg_sleep(0.5);
    END LOOP;
    FOREACH sid IN ARRAY demo.sids(p_tbl) LOOP
        分片 := demo.slabel(p_tbl, sid); 当前的主 := demo.leader(sid); 节点 := p_node;
        在组里的角色 := split_part(demo.gstate(p_node, sid), '|', 1);
        回放槽位 := CASE WHEN 当前的主 = p_node THEN '（它是主，没有回放槽）'
                        ELSE coalesce(demo.pq(p_node, format('SELECT CASE WHEN armed THEN ''armed，回放到 ''||applied ELSE ''未 armed'' END FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(%s)', sid)), '无回放槽（待重供）') END;
        RETURN NEXT;
    END LOOP;
END $$;

-- 函数一览
CREATE FUNCTION demo.help() RETURNS TABLE(函数 text, 作用 text) LANGUAGE sql AS $$
    VALUES
    ('demo.nodes()',                          '4 个节点：端口、类型、pg_raft 节点号、在线状态、控制面 0 号组角色'),
    ('demo.shards(''表'')',                   '每个分片落在哪个节点、哈希范围、行数'),
    ('demo.locate(''表'', 键)',               '某个键落在哪个分片、当前由哪个节点服务'),
    ('demo.raft_elect(''表'')',               '每个分片建 Raft 组并选主（实时打出选主过程）'),
    ('demo.raft_replicas(''表'')',            '在各组的主上把副本供到其余两台 → 最终主从'),
    ('demo.raft_groups(''表'')',              '每台 worker 在各组里的角色、任期、日志位点'),
    ('demo.roles(''表'')',                    '同一节点上的混合角色（节点 × 分片）'),
    ('demo.routing(''表'')',                  '路由三层：master 的 Citus 路由表 / 控制面登记 / 节点本地角色'),
    ('demo.flow(''表'')',                     '流控：Raft 日志环 + 分区流捕获环'),
    ('demo.global_txn()',                     '在 BEGIN 之后调用：加入全局事务（跨分片写必须）'),
    ('demo.xid(''表'')',                      '分片级 xid 分配器：每个分片的下一个号、水位、与原生 xid 对比'),
    ('demo.clog(''表'', ''S1'')',             '分片级 clog：每个分片 xid 的判决、start_ts、commit_ts、写了哪些行'),
    ('demo.versions(''表'', ''S1'')',         '页面上的多版本：每个元组版本的 xmin/xmax 与判决'),
    ('demo.replay(''表'')',                   '惰性回放：从已收到 vs 已回放'),
    ('demo.catchup(''表'')',                  '触发回放，让每个从追平'),
    ('demo.compare(''表'')',                  '副本与主逐字节比对'),
    ('demo.switch_leader(''表'', ''S1'', ''w2'')', '手动切换 leader（实时打出切主过程）'),
    ('demo.crash(''表'', ''w2'')',            '模拟宕机（immediate stop），看 Raft 自动选主'),
    ('demo.recover(''表'', ''w2'')',          '拉起宕机节点，看它归队、被自动重新供给')
$$;

RESET citus.enable_ddl_propagation;
SELECT '演示函数已安装（SELECT * FROM demo.help(); 查看）' AS 安装结果;
