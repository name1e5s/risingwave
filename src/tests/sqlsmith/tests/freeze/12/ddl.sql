CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT 'sgnli5BQ9E' AS col_0, t_0.c14 AS col_1 FROM alltypes1 AS t_0 FULL JOIN part AS t_1 ON t_0.c9 = t_1.p_mfgr AND (t_0.c3 > t_0.c5) WHERE (t_0.c2 = t_1.p_retailprice) GROUP BY t_0.c10, t_0.c8, t_0.c14, t_0.c11, t_0.c16, t_0.c7, t_1.p_comment, t_1.p_size, t_1.p_brand, t_1.p_type, t_0.c9, t_0.c4;
CREATE MATERIALIZED VIEW m1 AS SELECT ((BIGINT '1') - (INT '258')) AS col_0 FROM hop(bid, bid.date_time, INTERVAL '60', INTERVAL '5280') AS hop_0 GROUP BY hop_0.auction;
CREATE MATERIALIZED VIEW m2 AS SELECT sq_2.col_0 AS col_0 FROM (SELECT (char_length('j6vi9ZBxcW')) AS col_0, DATE '2022-12-02' AS col_1 FROM part AS t_0 RIGHT JOIN customer AS t_1 ON t_0.p_type = t_1.c_address GROUP BY t_1.c_custkey HAVING false) AS sq_2 WHERE false GROUP BY sq_2.col_0 HAVING false;
CREATE MATERIALIZED VIEW m3 AS SELECT (CAST(NULL AS STRUCT<a INT>)) AS col_0, ARRAY[(INT '692'), (INT '190'), (INT '1'), (INT '-2147483648')] AS col_1, t_0.c3 AS col_2, t_0.c11 AS col_3 FROM alltypes2 AS t_0 WHERE (t_0.c2 <= (((BIGINT '768') + t_0.c2) + t_0.c2)) GROUP BY t_0.c15, t_0.c13, t_0.c9, t_0.c1, t_0.c3, t_0.c11, t_0.c10, t_0.c14;
CREATE MATERIALIZED VIEW m4 AS SELECT t_0.c3 AS col_0, 'VN17c4PoI9' AS col_1, 'uK48sHYdIP' AS col_2, 'mvAlxNWqNq' AS col_3 FROM alltypes2 AS t_0 FULL JOIN supplier AS t_1 ON t_0.c3 = t_1.s_suppkey GROUP BY t_0.c1, t_0.c3, t_1.s_comment, t_1.s_nationkey, t_1.s_address;
CREATE MATERIALIZED VIEW m5 AS SELECT ((INT '812') + DATE '2022-12-02') AS col_0, (TRIM('PXcvtGJDrp')) AS col_1 FROM m4 AS t_0 GROUP BY t_0.col_3, t_0.col_1 HAVING false;
CREATE MATERIALIZED VIEW m6 AS SELECT (ARRAY[(INT '893'), (INT '987'), (INT '814')]) AS col_0 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '56') AS tumble_0 WHERE CAST((INT '289') AS BOOLEAN) GROUP BY tumble_0.c13, tumble_0.c8, tumble_0.c15, tumble_0.c1, tumble_0.c7, tumble_0.c11, tumble_0.c14 HAVING (true);
CREATE MATERIALIZED VIEW m7 AS SELECT t_0.p_comment AS col_0, t_0.p_comment AS col_1 FROM part AS t_0 FULL JOIN person AS t_1 ON t_0.p_mfgr = t_1.name AND (true) WHERE false GROUP BY t_1.email_address, t_1.credit_card, t_0.p_partkey, t_0.p_type, t_0.p_brand, t_0.p_mfgr, t_0.p_container, t_0.p_comment, t_1.id;
CREATE MATERIALIZED VIEW m9 AS SELECT TIMESTAMP '2022-12-02 01:22:33' AS col_0, t_0.col_3 AS col_1, CAST(NULL AS STRUCT<a INT>) AS col_2 FROM m3 AS t_0 GROUP BY t_0.col_3, t_0.col_0;