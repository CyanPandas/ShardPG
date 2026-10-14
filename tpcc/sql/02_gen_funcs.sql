-- TPC-C 数据生成与随机数辅助函数（TPC-C 标准 v5.11 §2.1.6 / §4.3）。
-- 只在协调者上用；不读写任何表，可以放心随 DDL 传播到各节点。

CREATE SCHEMA IF NOT EXISTS tpcc;

-- [x, y] 均匀整数
CREATE OR REPLACE FUNCTION tpcc.rand_int(x int, y int) RETURNS int
LANGUAGE sql VOLATILE AS $$ SELECT x + floor(random() * (y - x + 1))::int $$;

-- NURand(A, x, y) = (((random(0,A) | random(x,y)) + C) % (y-x+1)) + x
-- C 取值：装载与运行用不同的 C_LAST（差值 66，落在 §2.1.6.1 允许的 [65,119] 内且不是 96/112）
CREATE OR REPLACE FUNCTION tpcc.nurand(a int, x int, y int, c int) RETURNS int
LANGUAGE sql VOLATILE AS $$
    SELECT (((tpcc.rand_int(0, a) | tpcc.rand_int(x, y)) + c) % (y - x + 1)) + x
$$;

CREATE OR REPLACE FUNCTION tpcc.c_last_load() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 157 $$;
CREATE OR REPLACE FUNCTION tpcc.c_last_run()  RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 223 $$;
CREATE OR REPLACE FUNCTION tpcc.c_id_c()      RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 259 $$;
CREATE OR REPLACE FUNCTION tpcc.ol_i_id_c()   RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 7911 $$;

-- a-string：[minlen, maxlen] 个随机字母数字。每次 md5 产出 32 个十六进制字符，
-- 比逐字符 random() 快一个数量级（逐字符版装 1 仓要好几分钟）；字符集是
-- [0-9a-f] 的子集，对负载特征（长度、可压缩性）没有影响。
CREATE OR REPLACE FUNCTION tpcc.astring(minlen int, maxlen int) RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT left(string_agg(md5(random()::text), ''), len)
      FROM (SELECT tpcc.rand_int(minlen, maxlen) AS len) l,
           LATERAL generate_series(1, (len + 31) / 32)
     GROUP BY len
$$;

-- n-string：[minlen, maxlen] 个随机数字
CREATE OR REPLACE FUNCTION tpcc.nstring(minlen int, maxlen int) RETURNS text
LANGUAGE sql VOLATILE AS $$
    SELECT translate(tpcc.astring(minlen, maxlen), 'abcdef', '012345')
$$;

-- zip = 4 位随机数字 + '11111'
CREATE OR REPLACE FUNCTION tpcc.zip() RETURNS text
LANGUAGE sql VOLATILE AS $$ SELECT tpcc.nstring(4, 4) || '11111' $$;

-- i_data / s_data：26..50 字符，10% 在随机位置嵌入 'ORIGINAL'
CREATE OR REPLACE FUNCTION tpcc.data_original() RETURNS text
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    s   text := tpcc.astring(26, 50);
    pos int;
BEGIN
    IF random() < 0.10 THEN
        pos := tpcc.rand_int(1, length(s) - 8 + 1);
        s := overlay(s placing 'ORIGINAL' from pos for 8);
    END IF;
    RETURN s;
END $$;

-- 姓：0..999 三位数字各映射一个音节（§4.3.2.3）
CREATE OR REPLACE FUNCTION tpcc.lastname(n int) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT (ARRAY['BAR','OUGHT','ABLE','PRI','PRES','ESE','ANTI','CALLY','ATION','EING'])[n / 100 + 1]
        || (ARRAY['BAR','OUGHT','ABLE','PRI','PRES','ESE','ANTI','CALLY','ATION','EING'])[(n / 10) % 10 + 1]
        || (ARRAY['BAR','OUGHT','ABLE','PRI','PRES','ESE','ANTI','CALLY','ATION','EING'])[n % 10 + 1]
$$;

-- 装载期每单订单行数：要让 oorder.o_ol_cnt 与 order_line 的实际行数一致，
-- 用 (w,d,o) 的确定性哈希代替 random()，两张表分别生成时取到同一个值。
CREATE OR REPLACE FUNCTION tpcc.load_ol_cnt(w int, d int, o int) RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT 5 + (abs(hashtext(w || ':' || d || ':' || o)) % 11)
$$;

-- 装载期订单的客户号：每个区 3000 单对 1..3000 做一个确定性置换（§4.3.3.1
-- 要求 o_c_id 是客户号的随机置换）。用 o 乘一个与 3000 互素的数取模。
CREATE OR REPLACE FUNCTION tpcc.load_o_c_id(w int, d int, o int) RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT ((o - 1) * 1777 + (w * 10 + d) * 7) % 3000 + 1
$$;

-- 加入 ShardPG 全局事务（每个事务函数 / 装载函数的第一句）。
--
-- 跨分片写走 Citus 2PC，而打标分片只放行"已加入全局事务"的 PREPARE
-- （"未加入全局事务的分片写不允许 PREPARE TRANSACTION"）。加入协议由驱动端完成：
-- 取 gxid + TSO start_ts，SET LOCAL pg_partdist.join_info，再让 Citus 把这条
-- SET LOCAL 传播到本事务之后打开的每条分片连接（propagate_set_commands = local）。
-- 协调者分片给 0（同 demo.global_txn() 的默认用法）。只读事务也加入：所有 worker
-- 连接共用这一个 start_ts，跨节点的读才是同一个快照。
-- 必须在本事务第一次访问分布表**之前**调用（连接一旦打开，后来的 SET 不再补发）。
CREATE OR REPLACE FUNCTION tpcc.begin_global() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    g  bigint := partdist.partdist_gxid_next();
    ts bigint := partdist.partdist_tso_client_start_ts();
BEGIN
    -- 必须是 SET LOCAL **语句**：Citus 在 ProcessUtility 里拦截它再传播，set_config() 它看不到
    EXECUTE 'SET LOCAL citus.propagate_set_commands = ''local''';
    EXECUTE format('SET LOCAL pg_partdist.join_info = %L', format('%s,%s,0', g, ts));
END $$;
