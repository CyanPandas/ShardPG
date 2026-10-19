BEGIN;
SELECT tpcc.begin_global();
SELECT sum(bal) AS s FROM @TBL@ \gset
COMMIT;
\if :s != 7200000
\shell echo :s >> @BADLOG@
\endif
