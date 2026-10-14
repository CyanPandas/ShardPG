-- TPC-C 五类事务（§2.4–§2.8），协调者上的 plpgsql 函数，pgbench 每次调一个。
--
-- 写法与 BenchmarkSQL 同构（district/stock/customer/new_order 先 SELECT ... FOR
-- UPDATE 再改），依赖补丁 0011 的分片表行锁。ShardPG 相关的三条纪律：
--   · **函数里不写 EXCEPTION 块**：plpgsql 的异常块是子事务，分片表禁子事务写；
--   · **冲突即 40001**：分片表是 SI（first-committer-wins），并发改同一行时后来者
--     在持有者提交后报 could not serialize access —— 由 pgbench --max-tries 重试，
--     报告里要单列重试率；
--   · **NewOrder 的 1% 回滚在任何写入之前判定**：标准要求"处理到无效商品时回滚"，
--     没有异常块就无法回滚到半途，于是先把全部商品查一遍，有无效的直接返回 -1，
--     零写入 —— 对数据库的净效果与回滚相同，但少了被回滚的那部分写负载。
--
-- 随机参数在函数内生成；终端的"主仓库"由 pgbench 传入（client_id 映射）。
-- 每个函数第一句 tpcc.begin_global()：加入 ShardPG 全局事务（见 02_gen_funcs.sql）。

-- §2.4 New-Order。返回新订单号；-1 = 1% 的无效商品回滚
CREATE OR REPLACE FUNCTION tpcc.new_order(p_w int, p_nw int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_d        int := tpcc.rand_int(1, 10);
    v_c        int := tpcc.nurand(1023, 1, 3000, tpcc.c_id_c());
    v_ol_cnt   int := tpcc.rand_int(5, 15);
    v_rbk      boolean := tpcc.rand_int(1, 100) = 1;
    v_items    int[] := '{}';
    v_supply   int[] := '{}';
    v_qty      int[] := '{}';
    v_price    numeric[] := '{}';
    v_all_local int := 1;
    v_w_tax    numeric;
    v_d_tax    numeric;
    v_o_id     int;
    v_disc     numeric;
    v_i        int;
    v_sw       int;
    v_s_qty    int;
    v_dist     text;
    v_p        numeric;
    r          record;
BEGIN
    PERFORM tpcc.begin_global();     -- 必须先于任何分布表访问
    -- 生成订单行；按 (供货仓, 商品) 排序，压低 stock 行锁的死锁概率
    FOR r IN
        SELECT * FROM (
            SELECT CASE WHEN v_rbk AND g = v_ol_cnt THEN 100001            -- 不存在的商品
                        ELSE tpcc.nurand(8191, 1, 100000, tpcc.ol_i_id_c()) END AS item,
                   CASE WHEN p_nw > 1 AND tpcc.rand_int(1, 100) = 1
                        THEN ((p_w - 1 + tpcc.rand_int(1, p_nw - 1)) % p_nw) + 1  -- 1% 远程仓
                        ELSE p_w END AS supply,
                   tpcc.rand_int(1, 10) AS qty
              FROM generate_series(1, v_ol_cnt) g) x
         ORDER BY supply, item
    LOOP
        v_items  := v_items  || r.item;
        v_supply := v_supply || r.supply;
        v_qty    := v_qty    || r.qty;
        IF r.supply <> p_w THEN v_all_local := 0; END IF;
    END LOOP;

    -- 先查全部商品（只读）：有无效的 ⇒ 零写入返回（见文件头）
    FOR v_i IN 1..v_ol_cnt LOOP
        SELECT i_price INTO v_p FROM item WHERE i_id = v_items[v_i];
        IF NOT FOUND THEN
            RETURN -1;
        END IF;
        v_price := v_price || v_p;
    END LOOP;

    SELECT w_tax INTO v_w_tax FROM warehouse WHERE w_id = p_w;
    SELECT c_discount INTO v_disc FROM customer
     WHERE c_w_id = p_w AND c_d_id = v_d AND c_id = v_c;

    SELECT d_next_o_id, d_tax INTO v_o_id, v_d_tax FROM district
     WHERE d_w_id = p_w AND d_id = v_d FOR UPDATE;
    UPDATE district SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = p_w AND d_id = v_d;

    INSERT INTO oorder (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
    VALUES (p_w, v_d, v_o_id, v_c, NULL, v_ol_cnt, v_all_local, now());
    INSERT INTO new_order (no_w_id, no_d_id, no_o_id) VALUES (p_w, v_d, v_o_id);

    FOR v_i IN 1..v_ol_cnt LOOP
        v_sw := v_supply[v_i];
        SELECT s_quantity,
               CASE v_d WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02 WHEN 3 THEN s_dist_03
                        WHEN 4 THEN s_dist_04 WHEN 5 THEN s_dist_05 WHEN 6 THEN s_dist_06
                        WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08 WHEN 9 THEN s_dist_09
                        ELSE s_dist_10 END
          INTO v_s_qty, v_dist
          FROM stock WHERE s_w_id = v_sw AND s_i_id = v_items[v_i] FOR UPDATE;

        UPDATE stock
           SET s_quantity   = CASE WHEN v_s_qty - v_qty[v_i] >= 10 THEN v_s_qty - v_qty[v_i]
                                   ELSE v_s_qty - v_qty[v_i] + 91 END,
               s_ytd        = s_ytd + v_qty[v_i],
               s_order_cnt  = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt + CASE WHEN v_sw <> p_w THEN 1 ELSE 0 END
         WHERE s_w_id = v_sw AND s_i_id = v_items[v_i];

        INSERT INTO order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
                                ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES (p_w, v_d, v_o_id, v_i, v_items[v_i], NULL,
                (v_qty[v_i] * v_price[v_i] * (1 + v_w_tax + v_d_tax) * (1 - v_disc))::numeric(6,2),
                v_sw, v_qty[v_i], v_dist);
    END LOOP;
    RETURN v_o_id;
END $$;

-- 按姓取客户：同姓客户按 c_first 排序取中位（§2.5.2.2）
-- （plpgsql 而不是 sql：Citus 不支持 sql 函数里带参数访问分布表）
CREATE OR REPLACE FUNCTION tpcc.customer_by_name(p_w int, p_d int, p_last text) RETURNS int
LANGUAGE plpgsql STABLE AS $$
DECLARE
    ids int[];
BEGIN
    SELECT array_agg(c_id ORDER BY c_first) INTO ids FROM customer
     WHERE c_w_id = p_w AND c_d_id = p_d AND c_last = p_last;
    RETURN ids[(array_length(ids, 1) + 1) / 2];
END $$;

-- §2.5 Payment。返回客户号；0 = 按姓没找到客户（不写入）
CREATE OR REPLACE FUNCTION tpcc.payment(p_w int, p_nw int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_d       int := tpcc.rand_int(1, 10);
    v_c_w     int := p_w;
    v_c_d     int;
    v_c       int;
    v_amt     numeric(6,2) := (tpcc.rand_int(100, 500000) / 100.0)::numeric(6,2);
    v_w_name  text;
    v_d_name  text;
    v_credit  text;
    v_data    text;
BEGIN
    PERFORM tpcc.begin_global();     -- 必须先于任何分布表访问
    v_c_d := v_d;
    IF p_nw > 1 AND tpcc.rand_int(1, 100) > 85 THEN               -- 15% 远程客户
        v_c_w := ((p_w - 1 + tpcc.rand_int(1, p_nw - 1)) % p_nw) + 1;
        v_c_d := tpcc.rand_int(1, 10);
    END IF;
    IF tpcc.rand_int(1, 100) <= 60 THEN                           -- 60% 按姓
        v_c := tpcc.customer_by_name(v_c_w, v_c_d,
                   tpcc.lastname(tpcc.nurand(255, 0, 999, tpcc.c_last_run())));
        IF v_c IS NULL THEN
            RETURN 0;
        END IF;
    ELSE
        v_c := tpcc.nurand(1023, 1, 3000, tpcc.c_id_c());
    END IF;

    SELECT w_name INTO v_w_name FROM warehouse WHERE w_id = p_w FOR UPDATE;
    UPDATE warehouse SET w_ytd = w_ytd + v_amt WHERE w_id = p_w;
    SELECT d_name INTO v_d_name FROM district WHERE d_w_id = p_w AND d_id = v_d FOR UPDATE;
    UPDATE district SET d_ytd = d_ytd + v_amt WHERE d_w_id = p_w AND d_id = v_d;

    SELECT c_credit, c_data INTO v_credit, v_data FROM customer
     WHERE c_w_id = v_c_w AND c_d_id = v_c_d AND c_id = v_c FOR UPDATE;
    IF v_credit = 'BC' THEN
        v_data := left(format('%s %s %s %s %s %s | %s', v_c, v_c_d, v_c_w, v_d, p_w, v_amt, v_data), 500);
    END IF;
    UPDATE customer
       SET c_balance = c_balance - v_amt,
           c_ytd_payment = c_ytd_payment + v_amt,
           c_payment_cnt = c_payment_cnt + 1,
           c_data = v_data
     WHERE c_w_id = v_c_w AND c_d_id = v_c_d AND c_id = v_c;

    INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
    VALUES (v_c, v_c_d, v_c_w, v_d, p_w, now(), v_amt, left(v_w_name || '    ' || v_d_name, 24));
    RETURN v_c;
END $$;

-- §2.6 Order-Status（只读）。返回订单行数
CREATE OR REPLACE FUNCTION tpcc.order_status(p_w int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_d   int := tpcc.rand_int(1, 10);
    v_c   int;
    v_o   int;
    v_n   int;
    v_bal numeric;
BEGIN
    PERFORM tpcc.begin_global();     -- 必须先于任何分布表访问
    IF tpcc.rand_int(1, 100) <= 60 THEN
        v_c := tpcc.customer_by_name(p_w, v_d,
                   tpcc.lastname(tpcc.nurand(255, 0, 999, tpcc.c_last_run())));
        IF v_c IS NULL THEN
            RETURN 0;
        END IF;
    ELSE
        v_c := tpcc.nurand(1023, 1, 3000, tpcc.c_id_c());
    END IF;
    SELECT c_balance INTO v_bal FROM customer WHERE c_w_id = p_w AND c_d_id = v_d AND c_id = v_c;
    SELECT o_id INTO v_o FROM oorder
     WHERE o_w_id = p_w AND o_d_id = v_d AND o_c_id = v_c ORDER BY o_id DESC LIMIT 1;
    SELECT count(*) INTO v_n FROM (
        SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line
         WHERE ol_w_id = p_w AND ol_d_id = v_d AND ol_o_id = v_o) s;
    RETURN v_n;
END $$;

-- §2.7 Delivery（一次处理本仓 10 个区）。返回实际发货的区数
CREATE OR REPLACE FUNCTION tpcc.delivery(p_w int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_carrier int := tpcc.rand_int(1, 10);
    v_d       int;
    v_o       int;
    v_c       int;
    v_sum     numeric;
    v_done    int := 0;
BEGIN
    PERFORM tpcc.begin_global();     -- 必须先于任何分布表访问
    FOR v_d IN 1..10 LOOP
        SELECT no_o_id INTO v_o FROM new_order
         WHERE no_w_id = p_w AND no_d_id = v_d ORDER BY no_o_id LIMIT 1 FOR UPDATE;
        CONTINUE WHEN NOT FOUND;
        DELETE FROM new_order WHERE no_w_id = p_w AND no_d_id = v_d AND no_o_id = v_o;
        UPDATE oorder SET o_carrier_id = v_carrier
         WHERE o_w_id = p_w AND o_d_id = v_d AND o_id = v_o
        RETURNING o_c_id INTO v_c;
        UPDATE order_line SET ol_delivery_d = now()
         WHERE ol_w_id = p_w AND ol_d_id = v_d AND ol_o_id = v_o;
        SELECT sum(ol_amount) INTO v_sum FROM order_line
         WHERE ol_w_id = p_w AND ol_d_id = v_d AND ol_o_id = v_o;
        UPDATE customer
           SET c_balance = c_balance + coalesce(v_sum, 0),
               c_delivery_cnt = c_delivery_cnt + 1
         WHERE c_w_id = p_w AND c_d_id = v_d AND c_id = v_c;
        v_done := v_done + 1;
    END LOOP;
    RETURN v_done;
END $$;

-- §2.8 Stock-Level（只读）。返回低库存商品数
CREATE OR REPLACE FUNCTION tpcc.stock_level(p_w int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_d         int := tpcc.rand_int(1, 10);
    v_threshold int := tpcc.rand_int(10, 20);
    v_next      int;
    v_n         int;
BEGIN
    PERFORM tpcc.begin_global();     -- 必须先于任何分布表访问
    SELECT d_next_o_id INTO v_next FROM district WHERE d_w_id = p_w AND d_id = v_d;
    SELECT count(DISTINCT s_i_id) INTO v_n
      FROM order_line JOIN stock ON s_w_id = ol_w_id AND s_i_id = ol_i_id
     WHERE ol_w_id = p_w AND ol_d_id = v_d
       AND ol_o_id >= v_next - 20 AND ol_o_id < v_next
       AND s_quantity < v_threshold;
    RETURN v_n;
END $$;
