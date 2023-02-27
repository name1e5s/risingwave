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
CREATE MATERIALIZED VIEW m0 AS SELECT hop_0.date_time AS col_0, CAST(NULL AS STRUCT<a TIMESTAMP>) AS col_1, hop_0.date_time AS col_2 FROM hop(bid, bid.date_time, INTERVAL '1', INTERVAL '57') AS hop_0 WHERE false GROUP BY hop_0.channel, hop_0.date_time, hop_0.auction HAVING true;
CREATE MATERIALIZED VIEW m1 AS SELECT (82) AS col_0, t_1.ps_comment AS col_1, (TIMESTAMP '2022-02-26 12:27:55') AS col_2, t_0.s_address AS col_3 FROM supplier AS t_0 LEFT JOIN partsupp AS t_1 ON t_0.s_nationkey = t_1.ps_partkey GROUP BY t_0.s_acctbal, t_1.ps_partkey, t_1.ps_suppkey, t_0.s_address, t_1.ps_comment;
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (SELECT t_1.channel AS col_0 FROM bid AS t_1 WHERE true GROUP BY t_1.extra, t_1.date_time, t_1.channel HAVING true) SELECT TIMESTAMP '2022-02-26 12:27:56' AS col_0, 'K34M18F4Dh' AS col_1 FROM with_0;
CREATE MATERIALIZED VIEW m3 AS SELECT (TRIM(LEADING t_0.col_1 FROM t_0.col_1)) AS col_0, t_0.col_1 AS col_1, (substr('1o5LWxqh1E', (INT '280'), (INT '605'))) AS col_2 FROM m1 AS t_0 GROUP BY t_0.col_1 HAVING (CASE WHEN CAST((INT '346') AS BOOLEAN) THEN false WHEN false THEN true WHEN ((BIGINT '-4553122909529289624') >= ((752) * (SMALLINT '-29330'))) THEN false ELSE true END);
CREATE MATERIALIZED VIEW m4 AS SELECT ARRAY['vy3BMybHjg', 'zzlc6jD9mb'] AS col_0, t_0.c10 AS col_1 FROM alltypes2 AS t_0 GROUP BY t_0.c15, t_0.c16, t_0.c10, t_0.c2;
CREATE MATERIALIZED VIEW m5 AS SELECT (SMALLINT '32767') AS col_0, tumble_0.c2 AS col_1, false AS col_2, TIMESTAMP '2022-02-26 13:27:57' AS col_3 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '46') AS tumble_0 WHERE tumble_0.c1 GROUP BY tumble_0.c11, tumble_0.c16, tumble_0.c7, tumble_0.c2;
CREATE MATERIALIZED VIEW m6 AS WITH with_0 AS (SELECT ((INTERVAL '-86400') / (INT '414')) AS col_0, 'tXA5GL0hpH' AS col_1, tumble_1.state AS col_2, 'I9UfyeRJxE' AS col_3 FROM tumble(person, person.date_time, INTERVAL '49') AS tumble_1 GROUP BY tumble_1.id, tumble_1.state) SELECT (SMALLINT '626') AS col_0 FROM with_0;
CREATE MATERIALIZED VIEW m7 AS WITH with_0 AS (SELECT t_3.col_0 AS col_0, ARRAY['jwj0uKLOhj', 'zzvgW8GnLA', 'DmWvPNYKav'] AS col_1, t_3.col_0 AS col_2, (ARRAY['vPb08u7flk', 'Bx2xo2VHP2']) AS col_3 FROM m4 AS t_3 GROUP BY t_3.col_0 HAVING false) SELECT TIMESTAMP '2022-02-26 13:27:57' AS col_0, (substr((coalesce(NULL, NULL, NULL, NULL, NULL, 'wcpmSkChLF', NULL, NULL, NULL, NULL)), ((SMALLINT '34') # (INT '909')))) AS col_1, (DATE '2022-02-19' + (INTERVAL '-3600')) AS col_2, ((FLOAT '397') - (FLOAT '1185822401')) AS col_3 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m8 AS SELECT t_0.state AS col_0 FROM person AS t_0 WHERE false GROUP BY t_0.state;
CREATE MATERIALIZED VIEW m9 AS SELECT tumble_0.col_1 AS col_0 FROM tumble(m1, m1.col_2, INTERVAL '70') AS tumble_0 WHERE false GROUP BY tumble_0.col_1, tumble_0.col_3 HAVING false;