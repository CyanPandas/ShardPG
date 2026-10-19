\set a random(1, 72)
\set d random(1, 71)
\set b 1 + (:a - 1 + :d) % 72
\set x random(1, 100)
SELECT tpcc.iso_xfer('@TBL@', :a, :b, :x);
