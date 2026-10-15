# 用法：python3 reorder_txn.py <05_txn_funcs.sql>  就地改写 new_order / payment（幂等，标准/迷你规模都适用）
import sys, re
p = sys.argv[1]
s = open(p).read()
if 'TXN_REORDER_V1' in s:
    print('already'); sys.exit(0)

def rep(old, new):
    global s
    assert s.count(old) == 1, old[:80]
    s = s.replace(old, new)

# ---- new_order：商品一条查；库存在锁 district 之前用 UPDATE ... RETURNING 扣减 ----
rep('''    v_p        numeric;
    r          record;''', '''    v_p        numeric;
    v_found    int;
    v_dists    text[] := '{}';
    v_sdist    text[];
    r          record;''')
rep('''    -- 先查全部商品（只读）：有无效的 ⇒ 零写入返回（见文件头）
    FOR v_i IN 1..v_ol_cnt LOOP
        SELECT i_price INTO v_p FROM item WHERE i_id = v_items[v_i];
        IF NOT FOUND THEN
            RETURN -1;
        END IF;
        v_price := v_price || v_p;
    END LOOP;
''', '''    -- TXN_REORDER_V1：先查全部商品（只读，一条语句）：有无效的 ⇒ 零写入返回（见文件头）
    SELECT array_agg(i.i_price ORDER BY u.ord), count(i.i_price)
      INTO v_price, v_found
      FROM unnest(v_items) WITH ORDINALITY AS u(id, ord)
      LEFT JOIN item i ON i.i_id = u.id;
    IF v_found < v_ol_cnt THEN
        RETURN -1;
    END IF;
''')
# 把库存循环（SELECT FOR UPDATE + UPDATE）挪到 district 之前，合成一条 UPDATE ... RETURNING
old_stock_loop = s[s.index('    FOR v_i IN 1..v_ol_cnt LOOP\n        v_sw := v_supply[v_i];'):s.index('    RETURN v_o_id;\nEND $$;')]
rep(old_stock_loop, '''    FOR v_i IN 1..v_ol_cnt LOOP
        INSERT INTO order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
                                ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES (p_w, v_d, v_o_id, v_i, v_items[v_i], NULL,
                (v_qty[v_i] * v_price[v_i] * (1 + v_w_tax + v_d_tax) * (1 - v_disc))::numeric(6,2),
                v_supply[v_i], v_qty[v_i], v_dists[v_i]);
    END LOOP;
''')
rep('''    SELECT w_tax INTO v_w_tax FROM warehouse WHERE w_id = p_w;''', '''    -- 库存扣减放在锁 district 之前：district 行锁一直持有到提交，锁内语句越少越好
    -- （TPC-C 不规定语句次序，事务效果相同）。读改写合成一条 UPDATE ... RETURNING。
    FOR v_i IN 1..v_ol_cnt LOOP
        v_sw := v_supply[v_i];
        UPDATE stock
           SET s_quantity   = CASE WHEN s_quantity - v_qty[v_i] >= 10 THEN s_quantity - v_qty[v_i]
                                   ELSE s_quantity - v_qty[v_i] + 91 END,
               s_ytd        = s_ytd + v_qty[v_i],
               s_order_cnt  = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt + CASE WHEN v_sw <> p_w THEN 1 ELSE 0 END
         WHERE s_w_id = v_sw AND s_i_id = v_items[v_i]
        -- 不在 RETURNING 里写 CASE v_d WHEN ...：Citus 反解析带参数的简单 CASE 会出错
        RETURNING ARRAY[s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                        s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10]::text[]
          INTO v_sdist;
        v_dists := v_dists || v_sdist[v_d];
    END LOOP;

    SELECT w_tax INTO v_w_tax FROM warehouse WHERE w_id = p_w;''')
rep('''    SELECT d_next_o_id, d_tax INTO v_o_id, v_d_tax FROM district
     WHERE d_w_id = p_w AND d_id = v_d FOR UPDATE;
    UPDATE district SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = p_w AND d_id = v_d;
''', '''    UPDATE district SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = p_w AND d_id = v_d
    RETURNING d_next_o_id - 1, d_tax INTO v_o_id, v_d_tax;
''')

# ---- payment：客户（可能远程）先做；warehouse / district 合成 UPDATE ... RETURNING 放后面 ----
blk_wd = '''    SELECT w_name INTO v_w_name FROM warehouse WHERE w_id = p_w FOR UPDATE;
    UPDATE warehouse SET w_ytd = w_ytd + v_amt WHERE w_id = p_w;
    SELECT d_name INTO v_d_name FROM district WHERE d_w_id = p_w AND d_id = v_d FOR UPDATE;
    UPDATE district SET d_ytd = d_ytd + v_amt WHERE d_w_id = p_w AND d_id = v_d;

'''
rep(blk_wd, '')
rep('''     WHERE c_w_id = v_c_w AND c_d_id = v_c_d AND c_id = v_c;

    INSERT INTO history''', '''     WHERE c_w_id = v_c_w AND c_d_id = v_c_d AND c_id = v_c;

    -- TXN_REORDER_V1：warehouse / district 是全库最热的行，行锁持有到提交；
    -- 放到客户（15% 远程）之后再锁，读改写各合成一条 UPDATE ... RETURNING。
    UPDATE warehouse SET w_ytd = w_ytd + v_amt WHERE w_id = p_w RETURNING w_name INTO v_w_name;
    UPDATE district SET d_ytd = d_ytd + v_amt WHERE d_w_id = p_w AND d_id = v_d
    RETURNING d_name INTO v_d_name;

    INSERT INTO history''')
open(p, 'w').write(s)
print('ok')
