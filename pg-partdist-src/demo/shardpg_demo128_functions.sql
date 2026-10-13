-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示二：128 个分区组上的分布式事务 —— 追加函数库
--
-- 只在 DEMO_PROFILE=128 时、装完 shardpg_demo_functions.sql 之后再装（只装在 master 上）。
-- 演示一的函数大多按"逐分片逐节点"查询，4 个分片没问题，128 个就太慢；这里全部改成
-- "每台 worker 一条查询"的集合式写法。分区标签用 P001…P128（与演示一的 S1…S4 区分）。
-- ════════════════════════════════════════════════════════════════════════════

-- 分区标签：P001…P128
CREATE FUNCTION demo.plabel(p_tbl regclass, p_sid bigint) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT 'P' || lpad(array_position(demo.sids(p_tbl), p_sid)::text, 3, '0')
$$;

-- 演示一的公共函数（切主过程的实时时间线 demo.watch / demo.snap）按 demo.slabel 打标签；
-- 这里换成 P001…P128，让切主时间线和 demo.groups 用同一套名字。demo.sid 取标签第 2 位起的数字，
-- 'P001' 照样解析成第 1 个分片，不用改。
CREATE OR REPLACE FUNCTION demo.slabel(p_tbl regclass, p_sid bigint) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT demo.plabel(p_tbl, p_sid)
$$;

-- 各 worker 上全部数据组的状态（每台一条查询；宕机的节点没有行）
CREATE FUNCTION demo.gall()
RETURNS TABLE(node text, sid bigint, state text, term bigint)
LANGUAGE plpgsql AS $$
DECLARE w text; v text; ln text; f text[];
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        v := demo.pq(w, 'SELECT coalesce(string_agg(group_id||'' ''||state||'' ''||current_term, E''\n''), '''') '
                        'FROM partdist.pg_raft_group_status() WHERE group_id <> 0');
        CONTINUE WHEN v IS NULL OR v = '';
        FOREACH ln IN ARRAY string_to_array(v, E'\n') LOOP
            f := string_to_array(ln, ' ');
            node := w; sid := f[1]::bigint; state := f[2]; term := f[3]::bigint;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END $$;

-- 每个分区：主、两个副本、任期（成员集以建组时登记的为准）
CREATE FUNCTION demo.ginfo(p_tbl regclass)
RETURNS TABLE(sid bigint, 分区 text, 主 text, 副本 text, 任期 bigint)
LANGUAGE sql AS $$
    WITH g AS (SELECT * FROM demo.gall()),
         s AS (SELECT x AS sid, row_number() OVER (ORDER BY x) AS i FROM unnest(demo.sids(p_tbl)) x)
    SELECT s.sid,
           'P' || lpad(s.i::text, 3, '0'),
           (SELECT g.node FROM g WHERE g.sid = s.sid AND g.state = 'leader' ORDER BY g.term DESC LIMIT 1),
           (SELECT string_agg(n.name, ',' ORDER BY n.port) FROM demo.group_member m JOIN demo.node n ON n.raft_id = m.raft_id
             WHERE m.sid = s.sid
               AND n.name IS DISTINCT FROM (SELECT g.node FROM g WHERE g.sid = s.sid AND g.state = 'leader' ORDER BY g.term DESC LIMIT 1)),
           (SELECT max(g.term) FROM g WHERE g.sid = s.sid)
      FROM s ORDER BY s.i
$$;

-- ① 集群全貌：每台 worker 当几个分区的主、当几个分区的副本、主分区里有多少行
CREATE FUNCTION demo.cluster(p_tbl regclass)
RETURNS TABLE(节点 text, 端口 int, 主分区 bigint, 副本分区 bigint, 参与的分区组 bigint, 主分区行数 bigint, 状态 text)
LANGUAGE plpgsql AS $$
DECLARE cnt jsonb := '{}';
BEGIN
    -- 行数：经 master 路由读一遍，按分片归类（一条查询）
    EXECUTE format('SELECT coalesce(jsonb_object_agg(sid::text, n), ''{}'') FROM '
                   '(SELECT get_shard_id_for_distribution_column(%L, id) AS sid, count(*) AS n FROM %s GROUP BY 1) x',
                   p_tbl::text, p_tbl) INTO cnt;
    RETURN QUERY
    WITH gi AS (SELECT * FROM demo.ginfo(p_tbl)),
         up AS (SELECT DISTINCT node FROM demo.gall())
    SELECT n.name, n.port,
           (SELECT count(*) FROM gi WHERE gi.主 = n.name),
           (SELECT count(*) FROM gi WHERE n.name = ANY (string_to_array(gi.副本, ','))),
           (SELECT count(*) FROM demo.group_member m WHERE m.raft_id = n.raft_id
               AND m.sid = ANY (demo.sids(p_tbl))),
           (SELECT coalesce(sum((cnt ->> gi.sid::text)::bigint), 0) FROM gi WHERE gi.主 = n.name)::bigint,
           CASE WHEN n.name IN (SELECT node FROM up) THEN '在线' ELSE '✗ 宕机' END
      FROM demo.node n WHERE n.kind = 'worker' ORDER BY n.port;
END $$;

-- ② 分区组一览（默认列全部 128 个；只看前几个：demo.groups('wallet', 8)）
CREATE FUNCTION demo.groups(p_tbl regclass, p_limit int DEFAULT 128)
RETURNS TABLE(分区 text, 分片号 bigint, 主 text, 副本 text, 任期 bigint)
LANGUAGE sql AS $$
    SELECT 分区, sid, coalesce(主, '（选主中）'), 副本, 任期 FROM demo.ginfo(p_tbl) ORDER BY 分区 LIMIT p_limit
$$;

-- ③ 某几个键落在哪个分区、主在哪台、副本在哪两台
CREATE FUNCTION demo.locate(p_tbl regclass, VARIADIC p_keys int[])
RETURNS TABLE(键 int, 分区 text, 主 text, 副本 text)
LANGUAGE sql AS $$
    WITH gi AS (SELECT * FROM demo.ginfo(p_tbl))
    SELECT k, gi.分区, gi.主, gi.副本
      FROM unnest(p_keys) WITH ORDINALITY u(k, o)
      JOIN gi ON gi.sid = get_shard_id_for_distribution_column(p_tbl, k)
     ORDER BY o
$$;

-- ④ 最近一笔分布式事务：参与了哪些分区组、协调组是谁、在哪几台 worker 上
--    数据来自各 worker 的 partdist.dtx_participant（worker 在 PREPARE 时自己登记写过的分区组，
--    master 收齐后从写集里挑协调组并回写）—— 两阶段提交的真实账本，不是演示另记的。
CREATE FUNCTION demo.last_txn(p_tbl regclass)
RETURNS TABLE(项目 text, 内容 text)
LANGUAGE plpgsql AS $$
DECLARE w text; v text; best bigint := 0; d bigint; parts jsonb := '[]'; coord bigint; f text[]; ln text;
        sids bigint[] := demo.sids(p_tbl); gi record; nodes text; plist text;
BEGIN
    -- 先找最大的 dtxid（最近一笔）
    FOR w IN SELECT demo.workers() LOOP
        v := demo.pq(w, 'SELECT coalesce(max(dtxid), 0)::text FROM partdist.dtx_participant');
        d := coalesce(v, '0')::bigint;
        IF d > best THEN best := d; END IF;
    END LOOP;
    IF best = 0 THEN
        项目 := '提示'; 内容 := '还没有分布式事务（写到两个及以上分区的事务才走两阶段提交）'; RETURN NEXT; RETURN;
    END IF;
    FOR w IN SELECT demo.workers() LOOP
        v := demo.pq(w, format('SELECT coalesce(string_agg(g::text || '' '' || coalesce(p.coord_gsid, 0), E''\n''), '''') '
                               'FROM partdist.dtx_participant p, unnest(p.gsids) g WHERE p.dtxid = %s', best));
        CONTINUE WHEN v IS NULL OR v = '';
        FOREACH ln IN ARRAY string_to_array(v, E'\n') LOOP
            f := string_to_array(ln, ' ');
            parts := parts || jsonb_build_object('sid', f[1]::bigint, 'node', w);
            IF f[2]::bigint <> 0 THEN coord := f[2]::bigint; END IF;
        END LOOP;
    END LOOP;
    SELECT string_agg(DISTINCT demo.plabel(p_tbl, (e ->> 'sid')::bigint), ' ' ORDER BY demo.plabel(p_tbl, (e ->> 'sid')::bigint)),
           string_agg(DISTINCT e ->> 'node', '、' ORDER BY e ->> 'node')
      INTO plist, nodes
      FROM jsonb_array_elements(parts) e
     WHERE (e ->> 'sid')::bigint = ANY (sids);
    项目 := '全局事务号（dtxid）'; 内容 := best::text; RETURN NEXT;
    项目 := '参与的分区组';
    内容 := (SELECT count(DISTINCT e ->> 'sid') FROM jsonb_array_elements(parts) e)::text || ' 个：'
           || CASE WHEN array_length(string_to_array(coalesce(plist, ''), ' '), 1) > 16
                   THEN array_to_string((string_to_array(plist, ' '))[1:12], ' ') || ' …'
                   ELSE coalesce(plist, '') END;
    RETURN NEXT;
    项目 := '分区组的主所在'; 内容 := coalesce(nodes, ''); RETURN NEXT;
    项目 := '协调组'; 内容 := CASE WHEN coord IS NULL THEN '（尚未选定）'
                                 ELSE demo.plabel(p_tbl, coord) || '（分片 ' || coord || '，主在 '
                                      || coalesce((SELECT 主 FROM demo.ginfo(p_tbl) WHERE sid = coord), '?')
                                      || '）—— 决议写进它的 Raft 日志，达多数派即全局提交点' END;
    RETURN NEXT;
    项目 := '路由'; 内容 := '全部经 master（:5432）分发；master 只路由 + 发 TSO 时间戳，不放数据、不领导任何组'; RETURN NEXT;
END $$;

-- ⑤ 宕机：immediate 停掉一台 worker（模拟断电），看它当主的那些分区组各自重新选主
CREATE FUNCTION demo.crash_worker(p_tbl regclass, p_node text)
RETURNS TABLE(节点 text, 宕机前_主分区 bigint, 宕机后_主分区 bigint, 说明 text)
LANGUAGE plpgsql AS $$
DECLARE d text; t0 timestamptz; before jsonb; victims bigint[]; n_total int; n_done int; last_note int := -1;
        pgctl text := '/work/pg-install/bin/pg_ctl'; i int; r record;
BEGIN
    IF p_node NOT IN (SELECT demo.workers()) THEN
        RAISE EXCEPTION '只能停 worker（%）', (SELECT string_agg(x, ' ') FROM demo.workers() x);
    END IF;
    SELECT jsonb_object_agg(sid::text, 主) INTO before FROM demo.ginfo(p_tbl);
    victims := ARRAY(SELECT k::bigint FROM jsonb_each_text(before) e(k, v) WHERE v = p_node);
    n_total := coalesce(array_length(victims, 1), 0);
    d := (SELECT datadir FROM demo.node WHERE name = p_node);
    INSERT INTO demo.crashed(node) VALUES (p_node) ON CONFLICT (node) DO UPDATE SET at = now();
    RAISE NOTICE '宕机前：% 是 % 个分区组的主、另有 % 个分区组的副本', p_node, n_total,
        (SELECT count(*) FROM demo.group_member m JOIN demo.node n ON n.raft_id = m.raft_id WHERE n.name = p_node) - n_total;
    t0 := clock_timestamp();
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', format('%s -D %s -m immediate stop -w -t 60 >/dev/null 2>&1', pgctl, d));
    RAISE NOTICE '% ms  pg_ctl -m immediate stop %：进程直接退出，不做 checkpoint（模拟断电）', lpad(demo.ms(t0)::text, 6), p_node;
    FOR i IN 1..600 LOOP
        -- 新主 = 其余成员里当选的那台，且控制面登记（路由）已经跟过去
        SELECT count(*) INTO n_done FROM demo.ginfo(p_tbl) g
         WHERE g.sid = ANY (victims) AND g.主 IS NOT NULL AND g.主 <> p_node
           AND demo.reg(g.sid) = g.主;
        IF n_done <> last_note AND (n_done = n_total OR i % 4 = 0) THEN
            RAISE NOTICE '% ms  已有 %/% 个分区组在其余成员里选出新主并登记到路由', lpad(demo.ms(t0)::text, 6), n_done, n_total;
            last_note := n_done;
        END IF;
        EXIT WHEN n_done = n_total;
        PERFORM pg_sleep(0.5);
    END LOOP;
    FOR r IN SELECT n.name, n.port FROM demo.node n WHERE n.kind = 'worker' ORDER BY n.port LOOP
        节点 := r.name;
        宕机前_主分区 := (SELECT count(*) FROM jsonb_each_text(before) e(k, v) WHERE v = r.name);
        宕机后_主分区 := (SELECT count(*) FROM demo.ginfo(p_tbl) g WHERE g.主 = r.name);
        说明 := CASE WHEN r.name = p_node THEN '✗ 宕机' ELSE '' END;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑥ 归队：把停掉的 worker 拉起来。它以 follower 身份回到各个组；
--    它原来当主的那些分区，新主会在后台把它重新供成副本（demo.rejoin 看进度）
CREATE FUNCTION demo.recover_worker(p_tbl regclass)
RETURNS TABLE(项目 text, 内容 text)
LANGUAGE plpgsql AS $$
DECLARE n text; d text; t0 timestamptz; i int; nf int; nm int;
        pgctl text := '/work/pg-install/bin/pg_ctl';
BEGIN
    n := (SELECT node FROM demo.crashed ORDER BY at DESC LIMIT 1);
    IF n IS NULL THEN RAISE EXCEPTION '没有记录到被停掉的节点（先跑 demo.crash_worker）'; END IF;
    d := (SELECT datadir FROM demo.node WHERE name = n);
    t0 := clock_timestamp();
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', format('%s -D %s status >/dev/null 2>&1 || %s -D %s -l %s/pg.log -o "-p %s" start -w -t 60 >/dev/null 2>&1',
                   pgctl, d, pgctl, d, d, (SELECT port FROM demo.node WHERE name = n)));
    DELETE FROM demo.crashed WHERE node = n;
    RAISE NOTICE '% ms  pg_ctl start %：进程起来了，开始重新加入各个组', lpad(demo.ms(t0)::text, 6), n;
    nm := (SELECT count(*) FROM demo.group_member m JOIN demo.node x ON x.raft_id = m.raft_id
            WHERE x.name = n AND m.sid = ANY (demo.sids(p_tbl)));
    FOR i IN 1..240 LOOP
        nf := (SELECT count(*) FROM demo.gall() g WHERE g.node = n AND g.sid = ANY (demo.sids(p_tbl))
                  AND g.state IN ('follower', 'leader'));
        EXIT WHEN nf >= nm;
        IF i % 10 = 0 THEN
            RAISE NOTICE '% ms  % 已回到 %/% 个分区组', lpad(demo.ms(t0)::text, 6), n, nf, nm;
        END IF;
        PERFORM pg_sleep(0.5);
    END LOOP;
    项目 := '节点'; 内容 := n || ' 已启动（' || demo.ms(t0) || ' ms）'; RETURN NEXT;
    项目 := '回到的分区组'; 内容 := nf || '/' || nm || '（全部以 follower 身份归队，主不抢回来）'; RETURN NEXT;
    项目 := '后台重新供副本'; 内容 := '它宕机前当主的那些分区，新主正在后台把它重新供成副本；进度：SELECT * FROM demo.rejoin(''' || p_tbl::text || ''');'; RETURN NEXT;
END $$;

-- ⑦ 每台 worker 的副本是否都已就绪（回放槽位 armed）
CREATE FUNCTION demo.rejoin(p_tbl regclass)
RETURNS TABLE(节点 text, 应有副本 bigint, 已就绪 bigint, 待重新供给 bigint)
LANGUAGE plpgsql AS $$
DECLARE w text; v text; armed bigint[]; gi record;
BEGIN
    FOR w IN SELECT demo.workers() LOOP
        v := demo.pq(w, 'SELECT coalesce(string_agg(partdist.global_id_for_partition(shard)::text, '',''), '''') FROM partdist.replay_status() WHERE armed');
        armed := CASE WHEN v IS NULL OR v = '' THEN '{}'::bigint[] ELSE string_to_array(v, ',')::bigint[] END;
        节点 := w || CASE WHEN v IS NULL THEN '（宕机）' ELSE '' END;
        SELECT count(*), count(*) FILTER (WHERE g.sid = ANY (armed))
          INTO 应有副本, 已就绪
          FROM demo.ginfo(p_tbl) g
         WHERE w = ANY (string_to_array(g.副本, ','));
        待重新供给 := 应有副本 - 已就绪;
        RETURN NEXT;
    END LOOP;
END $$;

-- ⑧ 手动切主：把某个分区组的主切到同组里另一台**副本已就绪**（回放槽位 armed）的成员。
--    p_to 不给就自动挑。过程（竞选 → 当选 → 控制面登记 → master 路由跟过去 → 旧主被新主重新供成副本）
--    由演示一的 demo.switch_leader 实时打出来；这里只负责按 P 标签找组、挑目标。
CREATE FUNCTION demo.switch_group(p_tbl regclass, p_part text, p_to text DEFAULT NULL)
RETURNS TABLE(分区 text, 原来的主 text, 新主 text, 新任期 text, 切换耗时_ms int,
              旧主换下时的下一个分片xid text, 新主接着发的下一个分片xid text, 原主 text)
LANGUAGE plpgsql AS $$
DECLARE v_sid bigint; cur text; tgt text := p_to; w text; r record;
BEGIN
    v_sid := (SELECT g.sid FROM demo.ginfo(p_tbl) g WHERE g.分区 = upper(p_part));
    IF v_sid IS NULL THEN RAISE EXCEPTION '没有这个分区：%（P001…P128，见 demo.groups）', p_part; END IF;
    cur := demo.leader(v_sid);
    IF cur IS NULL THEN RAISE EXCEPTION '% 此刻没有主（正在选主），稍后再试', p_part; END IF;
    IF tgt IS NULL THEN
        FOR w IN SELECT n.name FROM demo.group_member m JOIN demo.node n ON n.raft_id = m.raft_id
                  WHERE m.sid = v_sid AND n.name <> cur ORDER BY n.port LOOP
            IF demo.pq(w, format('SELECT coalesce((SELECT armed::text FROM partdist.replay_status() '
                                 'WHERE shard = partdist.local_partition_for_shard(%s)), ''none'')', v_sid)) = 'true' THEN
                tgt := w; EXIT;
            END IF;
        END LOOP;
        IF tgt IS NULL THEN
            RAISE EXCEPTION '% 的其余成员都还没有就绪的副本（armed 回放槽位），不能切过去', p_part;
        END IF;
    END IF;
    RAISE NOTICE '把 % 的主从 % 切到 %（同组成员：%）', upper(p_part), cur, tgt,
        (SELECT string_agg(n.name, ' ' ORDER BY n.port) FROM demo.group_member m JOIN demo.node n ON n.raft_id = m.raft_id
          WHERE m.sid = v_sid);
    FOR r IN SELECT * FROM demo.switch_leader(p_tbl, upper(p_part), tgt) LOOP
        分区 := r.分片; 原来的主 := r.原来的主; 新主 := r.新主; 新任期 := r.新任期;
        切换耗时_ms := r.切换耗时_ms; 原主 := r.原主重新成为副本;
        旧主换下时的下一个分片xid := r.原主换下时的下一个分片xid;
        新主接着发的下一个分片xid := r.新主接着发的下一个分片xid;
        RETURN NEXT;
    END LOOP;
END $$;

-- 分片 xid 发号状态（第 6 步切主前后对照）：一个分区组的每个成员各一行。
--   只有主在发号，所以两列都只在主上显示（副本上是"—"）：
--   下一个分片xid：主接下来要发的号；已预留上界：主按批（4096 个一批）预留并写进 Raft 日志的发号上界。
--   新主不知道旧主在预留范围内实际发到了哪儿，所以从旧主的预留上界之后的下一批接着发 —— 号往前跳，绝不重号。
CREATE FUNCTION demo.part_xid(p_tbl regclass, p_part text)
RETURNS TABLE(分区 text, 节点 text, 角色 text, 任期 text, 下一个分片xid text, 已预留上界 text)
LANGUAGE plpgsql AS $$
DECLARE v_sid bigint; w text; st text; v text;
BEGIN
    v_sid := (SELECT g.sid FROM demo.ginfo(p_tbl) g WHERE g.分区 = upper(p_part));
    IF v_sid IS NULL THEN RAISE EXCEPTION '没有这个分区：%（P001…P128，见 demo.groups）', p_part; END IF;
    FOR w IN SELECT n.name FROM demo.group_member m JOIN demo.node n ON n.raft_id = m.raft_id
              WHERE m.sid = v_sid ORDER BY n.port LOOP
        分区 := upper(p_part); 节点 := w;
        st := demo.gstate(w, v_sid);
        角色 := CASE split_part(coalesce(st, ''), '|', 1) WHEN 'leader' THEN '主' WHEN 'follower' THEN '副本'
                    WHEN '' THEN '连不上' ELSE split_part(st, '|', 1) END;
        任期 := nullif(split_part(coalesce(st, ''), '|', 2), '');
        下一个分片xid := '—'; 已预留上界 := '—';
        IF 角色 = '主' THEN
            下一个分片xid := coalesce(demo.pq(w, format(
                'SELECT partdist.shard_xid_next(partdist.local_partition_for_shard(%s)::oid)::text', v_sid)), '?');
            已预留上界 := coalesce(demo.pq(w, format(
                'SELECT substring(partdist.route_status(partdist.local_partition_for_shard(%s)) from ''xid_watermark=([0-9]+)'')', v_sid)), '?');
        END IF;
        RETURN NEXT;
    END LOOP;
END $$;

-- 某个分区的分片 clog：只列真正写过行的分片 xid（从该分区当前主的页面元组头 xmin/xmax 里收集），
-- 最新的 p_limit 笔。两阶段提交的判决（COMMITTED + commit_ts）是异步落到各参与分区 clog 里的：
-- 先进本节点的"待应用判决"队列，后台约每半分钟清扫一次（实测刚 COMMIT 完 30–40 s 内这里还是 PREPARED；
-- 这期间别的事务来读，会当场向协调组问判决，所以可见性不受影响）。
-- 这里看到还有 PREPARED 时，先在主上手动触发一次同一个清扫（partdist.dtx_pending_sweep()），再等落定（最多 p_wait 秒）。
CREATE FUNCTION demo.part_clog(p_tbl regclass, p_part text, p_limit int DEFAULT 3, p_wait int DEFAULT 15)
RETURNS TABLE(分区 text, 分片xid bigint, 判决 text, start_ts bigint, commit_ts bigint, 写入的行 text, 删改的行 text)
LANGUAGE plpgsql AS $$
DECLARE v_sid bigint; lp text; rel text; q text; i int; pending int;
BEGIN
    v_sid := (SELECT g.sid FROM demo.ginfo(p_tbl) g WHERE g.分区 = upper(p_part));
    IF v_sid IS NULL THEN RAISE EXCEPTION '没有这个分区：%（P001…P128，见 demo.groups）', p_part; END IF;
    lp := demo.leader(v_sid);
    IF lp IS NULL THEN RAISE EXCEPTION '% 此刻没有主（正在选主），稍后再试', p_part; END IF;
    rel := p_tbl::text || '_' || v_sid;
    q := format($q$
        WITH v AS (%1$s), x AS (SELECT xmin AS xid FROM v UNION SELECT xmax FROM v WHERE xmax <> 0
                               ORDER BY 1 DESC LIMIT %3$s)
        SELECT x.xid, partdist.shard_clog_status_full(partdist.local_partition_for_shard(%2$s)::oid, x.xid),
               (SELECT string_agg(id::text, ',' ORDER BY id) FROM v WHERE v.xmin = x.xid),
               (SELECT string_agg(id::text, ',' ORDER BY id) FROM v WHERE v.xmax = x.xid)
          FROM x ORDER BY 1 DESC $q$, demo.page_versions_sql(rel), v_sid, p_limit);
    FOR i IN 1..greatest(p_wait * 2, 1) LOOP
        SELECT count(*) INTO pending FROM demo.dblink(demo.conn(lp), q) AS t(xid bigint, st text, ins text, del text)
         WHERE t.st !~ 'st=[23]';
        EXIT WHEN pending = 0;
        PERFORM demo.pq(lp, 'SELECT partdist.dtx_pending_sweep()::text');
        PERFORM pg_sleep(0.5);
    END LOOP;
    RAISE NOTICE '% 当前的主是 %（st：1 PREPARED 2 COMMITTED 3 ABORTED）', upper(p_part), lp;
    RETURN QUERY
        SELECT upper(p_part), t.xid,
               CASE substring(t.st from 'st=([0-9]+)') WHEN '0' THEN '0 运行中' WHEN '1' THEN '1 PREPARED'
                    WHEN '2' THEN '2 COMMITTED' WHEN '3' THEN '3 ABORTED' ELSE t.st END,
               substring(t.st from 'sts=([0-9]+)')::bigint, substring(t.st from 'cts=([0-9]+)')::bigint, t.ins, t.del
          FROM demo.dblink(demo.conn(lp), q) AS t(xid bigint, st text, ins text, del text)
         ORDER BY t.xid DESC;
END $$;

-- 总账：账户数、余额总和（守恒检查）
CREATE FUNCTION demo.ledger(p_tbl regclass)
RETURNS TABLE(账户数 bigint, 余额总和 bigint, 涉及分区 bigint)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY EXECUTE format('SELECT count(*), sum(balance)::bigint, count(DISTINCT get_shard_id_for_distribution_column(%L, id)) FROM %s',
                                p_tbl::text, p_tbl);
END $$;
