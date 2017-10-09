drop table if exists adl_tianchi_portscan_trainset_connect_part2;
create table if not exists adl_tianchi_portscan_trainset_connect_part2 as select * from odps_tc_257100_f673506e024.adl_tianchi_portscan_trainset_connect_part2;
drop table if exists adl_tianchi_portscan_trainset_label_part2;
create table if not exists adl_tianchi_portscan_trainset_label_part2 as select * from odps_tc_257100_f673506e024.adl_tianchi_portscan_trainset_label_part2;

DROP TABLE IF EXISTS portscan_feature_part1_p2;
CREATE TABLE IF NOT EXISTS portscan_feature_part1_p2
AS
SELECT client_port, source_ip, ds, CAST(client_port AS BIGINT) AS port
	, COUNT(client_ip) AS sip_client_ip_n, COUNT(DISTINCT client_ip) AS sip_client_ip_dn_cnt
	, SUM(counts) AS sip_connect_n, MAX(counts) AS sip_connect_n_max
	, AVG(counts) AS sip_connect_n_avg, COUNT(DISTINCT hashuserid) AS sip_hashuserid_dn --连接云主机拥有者个数
FROM adl_tianchi_portscan_trainset_connect_part2
WHERE ds >= '20170625'
	AND ds <= '20170701'
	AND counts != 0
	AND client_port != '58108'
GROUP BY client_port, 
	source_ip, 
	ds;

DROP TABLE IF EXISTS portscan_feature_part2_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_part2_p2
AS
SELECT source_ip, ds, COUNT(DISTINCT client_port) AS sip_client_port2
	, COUNT(client_ip) AS sip_client_ip_n2, COUNT(DISTINCT client_ip) AS sip_client_ip_dn_cnt2
	, SUM(counts) AS sip_connect_n2, MAX(counts) AS sip_connect_n_max2
	, AVG(counts) AS sip_connect_n_avg2, COUNT(DISTINCT hashuserid) AS sip_hashuserid_dn2 --连接云主机拥有者个数
FROM adl_tianchi_portscan_trainset_connect_part2
WHERE ds >= '20170625'
	AND ds <= '20170701'
	AND counts != 0
	AND client_port != '58108'
GROUP BY source_ip, 
	ds;

DROP TABLE IF EXISTS portscan_feature_all1_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_all1_p2
AS
SELECT a.*, b.sip_client_port2, b.sip_client_ip_n2, b.sip_client_ip_dn_cnt2, b.sip_connect_n2
	, b.sip_connect_n_max2, b.sip_connect_n_avg2, b.sip_hashuserid_dn2
FROM portscan_feature_part1_p2 a
LEFT OUTER JOIN portscan_feature_part2_p2 b
ON a.source_ip = b.source_ip
	AND a.ds = b.ds;

DROP TABLE IF EXISTS portscan_feature_all2_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_all2_p2
AS
SELECT *, sip_client_ip_n2 / sip_client_ip_n AS sip_client_ip_n_ratio, sip_client_ip_dn_cnt2 / sip_client_ip_dn_cnt AS sip_client_ip_dn_cnt_ratio
	, sip_connect_n2 / sip_connect_n AS sip_connect_n_ratio, sip_connect_n_max2 / sip_connect_n_max AS sip_connect_n_max_ratio
	, sip_connect_n_avg2 / sip_connect_n_avg AS sip_connect_n_avg_ratio
FROM portscan_feature_all1_p2;

DROP TABLE IF EXISTS portscan_train_p2;

CREATE TABLE IF NOT EXISTS portscan_train_p2
AS
SELECT a.*
	, CASE 
		WHEN b.client_port IS NULL THEN 0
		ELSE 1
	END AS label
FROM portscan_feature_all2_p2 a
LEFT OUTER JOIN adl_tianchi_portscan_trainset_label_part2 b
ON a.client_port = b.client_port
	AND a.source_ip = b.source_ip
	AND a.ds = b.ds;
-------------
drop table if exists adl_tianchi_portscan_testset_connect_part2;
create table if not exists adl_tianchi_portscan_testset_connect_part2 as select * from odps_tc_257100_f673506e024.adl_tianchi_portscan_testset_connect_part2;

DROP TABLE IF EXISTS portscan_feature_test_part1_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_test_part1_p2
AS
SELECT client_port, source_ip, ds, CAST(client_port AS BIGINT) AS port
	, COUNT(client_ip) AS sip_client_ip_n, COUNT(DISTINCT client_ip) AS sip_client_ip_dn_cnt
	, SUM(counts) AS sip_connect_n, MAX(counts) AS sip_connect_n_max
	, AVG(counts) AS sip_connect_n_avg, COUNT(DISTINCT hashuserid) AS sip_hashuserid_dn --连接云主机拥有者个数
FROM adl_tianchi_portscan_testset_connect_part2
WHERE counts != 0
	AND client_port != 58108
GROUP BY client_port, 
	source_ip, 
	ds;

DROP TABLE IF EXISTS portscan_feature_test_part2_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_test_part2_p2
AS
SELECT source_ip, ds, COUNT(DISTINCT client_port) AS sip_client_port2
	, COUNT(client_ip) AS sip_client_ip_n2, COUNT(DISTINCT client_ip) AS sip_client_ip_dn_cnt2
	, SUM(counts) AS sip_connect_n2, MAX(counts) AS sip_connect_n_max2
	, AVG(counts) AS sip_connect_n_avg2, COUNT(DISTINCT hashuserid) AS sip_hashuserid_dn2 --连接云主机拥有者个数
FROM adl_tianchi_portscan_testset_connect_part2
WHERE counts != 0
	AND client_port != 58108
GROUP BY source_ip, 
	ds;

DROP TABLE IF EXISTS portscan_feature_test_all1_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_test_all1_p2
AS
SELECT a.*, b.sip_client_port2, b.sip_client_ip_n2, b.sip_client_ip_dn_cnt2, b.sip_connect_n2
	, b.sip_connect_n_max2, b.sip_connect_n_avg2, b.sip_hashuserid_dn2
FROM portscan_feature_test_part1_p2 a
LEFT OUTER JOIN portscan_feature_test_part2_p2 b
ON a.source_ip = b.source_ip
	AND a.ds = b.ds;

DROP TABLE IF EXISTS portscan_test_p2;

CREATE TABLE IF NOT EXISTS portscan_test_p2
AS
SELECT *, sip_client_ip_n2 / sip_client_ip_n AS sip_client_ip_n_ratio, sip_client_ip_dn_cnt2 / sip_client_ip_dn_cnt AS sip_client_ip_dn_cnt_ratio
	, sip_connect_n2 / sip_connect_n AS sip_connect_n_ratio, sip_connect_n_max2 / sip_connect_n_max AS sip_connect_n_max_ratio
	, sip_connect_n_avg2 / sip_connect_n_avg AS sip_connect_n_avg_ratio
FROM portscan_feature_test_all1_p2;



drop table if exists connect_stats_groupby_client_ip_client_port_part1_p2;
create table if not exists connect_stats_groupby_client_ip_client_port_part1_p2 as select client_ip,ds,client_port,sum(counts) as sum_counts_cip_crt,max(counts) as max_counts_cip_crt,avg(counts) as avg_counts_cip_crt,count(distinct source_ip) as counts_distinct_cip_crt from adl_tianchi_portscan_trainset_connect_part2 group by client_ip,ds,client_port;
drop table if exists connect_stats_groupby_client_ip_client_port_part2_p2;
create table if not exists connect_stats_groupby_client_ip_client_port_part2_p2 as select client_ip,ds,sum(counts) as sum_counts_cip,max(counts) as max_counts_cip,avg(counts) as avg_counts_cip,count(distinct source_ip) as counts_distinct_cip from adl_tianchi_portscan_trainset_connect_part2 group by client_ip,ds;

drop table if exists join_new_trainset_p1_p2;
create table if not exists join_new_trainset_p1_p2 as select a.*,b.sum_counts_cip_crt,b.max_counts_cip_crt,b.avg_counts_cip_crt,b.counts_distinct_cip_crt from adl_tianchi_portscan_trainset_connect_part2 a left outer join connect_stats_groupby_client_ip_client_port_part1_p2 b on a.client_ip=b.client_ip and a.ds=b.ds and a.client_port=b.client_port;
drop table if exists porstan_new_trainset_p2;
create table if not exists porstan_new_trainset_p2 as select a.*,b.sum_counts_cip,b.max_counts_cip,b.counts_distinct_cip,b.avg_counts_cip from join_new_trainset_p1_p2 a left outer join connect_stats_groupby_client_ip_client_port_part2_p2 b on a.client_ip=b.client_ip and a.ds=b.ds;





drop table if exists connect_stats_groupby_client_ip_client_port_part1_p2;
create table if not exists connect_stats_groupby_client_ip_client_port_part1_p2 as select client_ip,ds,client_port,sum(counts) as sum_counts_cip_crt,max(counts) as max_counts_cip_crt,avg(counts) as avg_counts_cip_crt,count(distinct source_ip) as counts_distinct_cip_crt from adl_tianchi_portscan_testset_connect_part2 group by client_ip,ds,client_port;
drop table if exists connect_stats_groupby_client_ip_client_port_part2_p2;
create table if not exists connect_stats_groupby_client_ip_client_port_part2_p2 as select client_ip,ds,sum(counts) as sum_counts_cip,max(counts) as max_counts_cip,avg(counts) as avg_counts_cip,count(distinct source_ip) as counts_distinct_cip from adl_tianchi_portscan_testset_connect_part2 group by client_ip,ds;

drop table if exists join_new_testset_p1_p2;
create table if not exists join_new_testset_p1_p2 as select a.*,b.sum_counts_cip_crt,b.max_counts_cip_crt,b.avg_counts_cip_crt,b.counts_distinct_cip_crt from adl_tianchi_portscan_testset_connect_part2 a left outer join connect_stats_groupby_client_ip_client_port_part1_p2 b on a.client_ip=b.client_ip and a.ds=b.ds and a.client_port=b.client_port;
drop table if exists porstan_new_testset_p2;
create table if not exists porstan_new_testset_p2 as select a.*,b.sum_counts_cip,b.max_counts_cip,b.counts_distinct_cip,b.avg_counts_cip from join_new_testset_p1_p2 a left outer join connect_stats_groupby_client_ip_client_port_part2_p2 b on a.client_ip=b.client_ip and a.ds=b.ds;




DROP TABLE IF EXISTS portscan_feature_part1_new_p2;
CREATE TABLE IF NOT EXISTS portscan_feature_part1_new_p2 AS SELECT client_port, source_ip, ds, CAST(client_port AS BIGINT) AS port
	, max(sum_counts_cip_crt) as sum_counts_cip_crt_m,avg(sum_counts_cip_crt) as sum_counts_cip_crt_a
	, max(max_counts_cip_crt) as max_counts_cip_crt_m,avg(max_counts_cip_crt) as max_counts_cip_crt_a
	, max(avg_counts_cip_crt) as avg_counts_cip_crt_m,avg(avg_counts_cip_crt) as avg_counts_cip_crt_a
	, max(counts_distinct_cip_crt) as counts_distinct_cip_crt_m,avg(counts_distinct_cip_crt) as counts_distinct_cip_crt_a
	, max(sum_counts_cip) as sum_counts_cip_m,avg(sum_counts_cip) as sum_counts_cip_a
	, max(max_counts_cip) as max_counts_cip_m,avg(max_counts_cip) as max_counts_cip_a
	, max(counts_distinct_cip) as counts_distinct_cip_m,avg(counts_distinct_cip) as counts_distinct_cip_a
	, max(avg_counts_cip) as avg_counts_cip_m,avg(avg_counts_cip) as avg_counts_cip_a
FROM porstan_new_trainset_p2
WHERE ds >= '20170625'
	AND ds <= '20170701'
	AND counts != 0
	AND client_port != '58108'
GROUP BY client_port, source_ip, ds;

DROP TABLE IF EXISTS portscan_feature_part2_new_p2;

CREATE TABLE IF NOT EXISTS portscan_feature_part2_new_p2
AS
SELECT source_ip, ds
	, max(sum_counts_cip_crt) as sum_counts_cip_crt_m2,avg(sum_counts_cip_crt) as sum_counts_cip_crt_a2
	, max(max_counts_cip_crt) as max_counts_cip_crt_m2,avg(max_counts_cip_crt) as max_counts_cip_crt_a2
	, max(avg_counts_cip_crt) as avg_counts_cip_crt_m2,avg(avg_counts_cip_crt) as avg_counts_cip_crt_a2
	, max(counts_distinct_cip_crt) as counts_distinct_cip_crt_m2,avg(counts_distinct_cip_crt) as counts_distinct_cip_crt_a2
	, max(sum_counts_cip) as sum_counts_cip_m2,avg(sum_counts_cip) as sum_counts_cip_a2
	, max(max_counts_cip) as max_counts_cip_m2,avg(max_counts_cip) as max_counts_cip_a2
	, max(counts_distinct_cip) as counts_distinct_cip_m2,avg(counts_distinct_cip) as counts_distinct_cip_a2
	, max(avg_counts_cip) as avg_counts_cip_m2,avg(avg_counts_cip) as avg_counts_cip_a2
FROM porstan_new_trainset_p2
WHERE ds >= '20170625'
	AND ds <= '20170701'
	AND counts != 0
	AND client_port != '58108'
GROUP BY source_ip, ds;


---- poortscan_train_v3 是由两阶段的训练集合并而成

--portscan_train_p2_final
--portscan_test_p2_final
drop table if exists portscan_train_all;
create table if not exists portscan_train_all as select * from (select * from poortscan_train_v3 union all select * from portscan_train_p2_final) tmp
----
drop table if exists cv_portscan_train_xgboost_21;
create table if not exists cv_portscan_train_xgboost_21 as select * from portscan_train_all where port=21;
drop table if exists cv_portscan_test_xgboost_21;
create table if not exists cv_portscan_test_xgboost_21 as select * from portscan_test_p2_final where port=21;
drop table if exists cv_portscan_train_xgboost_21_label_1;
create table if not exists cv_portscan_train_xgboost_21_label_1 as select * from cv_portscan_train_xgboost_21 where label=1;
drop table if exists cv_portscan_train_xgboost_21_label_0;
create table if not exists cv_portscan_train_xgboost_21_label_0 as select * from cv_portscan_train_xgboost_21 where label=0;
drop table if exists cv_portscan_train_xgboost_21_label_0_L;
drop table if exists cv_portscan_train_xgboost_21_label_0_R;
drop table if exists cv_portscan_train_xgboost_21_label_0_P1;
drop table if exists cv_portscan_train_xgboost_21_label_0_P2;
drop table if exists cv_portscan_train_xgboost_21_label_0_P3;
drop table if exists cv_portscan_train_xgboost_21_label_0_P4;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_21_label_0 -Doutput1TableName=cv_portscan_train_xgboost_21_label_0_L -Doutput2TableName=cv_portscan_train_xgboost_21_label_0_R -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_21_label_0_L -Doutput1TableName=cv_portscan_train_xgboost_21_label_0_P1 -Doutput2TableName=cv_portscan_train_xgboost_21_label_0_P2 -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_21_label_0_R -Doutput1TableName=cv_portscan_train_xgboost_21_label_0_P3 -Doutput2TableName=cv_portscan_train_xgboost_21_label_0_P4 -Dfraction=0.5;
drop table if exists cv_portscan_train_xgboost_21_P1;
drop table if exists cv_portscan_train_xgboost_21_P2;
drop table if exists cv_portscan_train_xgboost_21_P3;
drop table if exists cv_portscan_train_xgboost_21_P4;
create table if not exists cv_portscan_train_xgboost_21_P1 as select * from (select * from cv_portscan_train_xgboost_21_label_0_P1 union all select * from cv_portscan_train_xgboost_21_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_21_P2 as select * from (select * from cv_portscan_train_xgboost_21_label_0_P2 union all select * from cv_portscan_train_xgboost_21_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_21_P3 as select * from (select * from cv_portscan_train_xgboost_21_label_0_P3 union all select * from cv_portscan_train_xgboost_21_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_21_P4 as select * from (select * from cv_portscan_train_xgboost_21_label_0_P4 union all select * from cv_portscan_train_xgboost_21_label_1) tmp;
drop offlinemodel if exists cv_xgboost_21_t1_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_21_P1" -DmodelName="cv_xgboost_21_t1_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_21_t2_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_21_P2" -DmodelName="cv_xgboost_21_t2_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_21_t3_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_21_P3" -DmodelName="cv_xgboost_21_t3_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_21_t4_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_21_P4" -DmodelName="cv_xgboost_21_t4_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";

drop table if exists cv_portscan_result_xgboost_21_P1;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_21" -DoutputTableName="cv_portscan_result_xgboost_21_P1" -DmodelName="cv_xgboost_21_t1_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_21_P2;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_21" -DoutputTableName="cv_portscan_result_xgboost_21_P2" -DmodelName="cv_xgboost_21_t2_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_21_P3;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_21" -DoutputTableName="cv_portscan_result_xgboost_21_P3" -DmodelName="cv_xgboost_21_t3_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_21_P4;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_21" -DoutputTableName="cv_portscan_result_xgboost_21_P4" -DmodelName="cv_xgboost_21_t4_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";

----------
drop table if exists cv_portscan_train_xgboost_22;
create table if not exists cv_portscan_train_xgboost_22 as select * from portscan_train_all where port=22;
drop table if exists cv_portscan_test_xgboost_22;
create table if not exists cv_portscan_test_xgboost_22 as select * from portscan_test_p2_final where port=22;
drop table if exists cv_portscan_train_xgboost_22_label_1;
create table if not exists cv_portscan_train_xgboost_22_label_1 as select * from cv_portscan_train_xgboost_22 where label=1;
drop table if exists cv_portscan_train_xgboost_22_label_0;
create table if not exists cv_portscan_train_xgboost_22_label_0 as select * from cv_portscan_train_xgboost_22 where label=0;
drop table if exists cv_portscan_train_xgboost_22_label_0_L;
drop table if exists cv_portscan_train_xgboost_22_label_0_R;
drop table if exists cv_portscan_train_xgboost_22_label_0_P1;
drop table if exists cv_portscan_train_xgboost_22_label_0_P2;
drop table if exists cv_portscan_train_xgboost_22_label_0_P3;
drop table if exists cv_portscan_train_xgboost_22_label_0_P4;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_22_label_0 -Doutput1TableName=cv_portscan_train_xgboost_22_label_0_L -Doutput2TableName=cv_portscan_train_xgboost_22_label_0_R -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_22_label_0_L -Doutput1TableName=cv_portscan_train_xgboost_22_label_0_P1 -Doutput2TableName=cv_portscan_train_xgboost_22_label_0_P2 -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_22_label_0_R -Doutput1TableName=cv_portscan_train_xgboost_22_label_0_P3 -Doutput2TableName=cv_portscan_train_xgboost_22_label_0_P4 -Dfraction=0.5;
drop table if exists cv_portscan_train_xgboost_22_P1;
drop table if exists cv_portscan_train_xgboost_22_P2;
drop table if exists cv_portscan_train_xgboost_22_P3;
drop table if exists cv_portscan_train_xgboost_22_P4;
create table if not exists cv_portscan_train_xgboost_22_P1 as select * from (select * from cv_portscan_train_xgboost_22_label_0_P1 union all select * from cv_portscan_train_xgboost_22_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_22_P2 as select * from (select * from cv_portscan_train_xgboost_22_label_0_P2 union all select * from cv_portscan_train_xgboost_22_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_22_P3 as select * from (select * from cv_portscan_train_xgboost_22_label_0_P3 union all select * from cv_portscan_train_xgboost_22_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_22_P4 as select * from (select * from cv_portscan_train_xgboost_22_label_0_P4 union all select * from cv_portscan_train_xgboost_22_label_1) tmp;
drop offlinemodel if exists cv_xgboost_22_t1_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_22_P1" -DmodelName="cv_xgboost_22_t1_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_22_t2_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_22_P2" -DmodelName="cv_xgboost_22_t2_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_22_t3_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_22_P3" -DmodelName="cv_xgboost_22_t3_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_22_t4_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_22_P4" -DmodelName="cv_xgboost_22_t4_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";

drop table if exists cv_portscan_result_xgboost_22_P1;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_22" -DoutputTableName="cv_portscan_result_xgboost_22_P1" -DmodelName="cv_xgboost_22_t1_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_22_P2;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_22" -DoutputTableName="cv_portscan_result_xgboost_22_P2" -DmodelName="cv_xgboost_22_t2_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_22_P3;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_22" -DoutputTableName="cv_portscan_result_xgboost_22_P3" -DmodelName="cv_xgboost_22_t3_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_22_P4;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_22" -DoutputTableName="cv_portscan_result_xgboost_22_P4" -DmodelName="cv_xgboost_22_t4_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
------

drop table if exists cv_portscan_train_xgboost_3306;
create table if not exists cv_portscan_train_xgboost_3306 as select * from portscan_train_all where port=3306;
drop table if exists cv_portscan_test_xgboost_3306;
create table if not exists cv_portscan_test_xgboost_3306 as select * from portscan_test_p2_final where port=3306;
drop table if exists cv_portscan_train_xgboost_3306_label_1;
create table if not exists cv_portscan_train_xgboost_3306_label_1 as select * from cv_portscan_train_xgboost_3306 where label=1;
drop table if exists cv_portscan_train_xgboost_3306_label_0;
create table if not exists cv_portscan_train_xgboost_3306_label_0 as select * from cv_portscan_train_xgboost_3306 where label=0;
drop table if exists cv_portscan_train_xgboost_3306_label_0_L;
drop table if exists cv_portscan_train_xgboost_3306_label_0_R;
drop table if exists cv_portscan_train_xgboost_3306_label_0_P1;
drop table if exists cv_portscan_train_xgboost_3306_label_0_P2;
drop table if exists cv_portscan_train_xgboost_3306_label_0_P3;
drop table if exists cv_portscan_train_xgboost_3306_label_0_P4;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3306_label_0 -Doutput1TableName=cv_portscan_train_xgboost_3306_label_0_L -Doutput2TableName=cv_portscan_train_xgboost_3306_label_0_R -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3306_label_0_L -Doutput1TableName=cv_portscan_train_xgboost_3306_label_0_P1 -Doutput2TableName=cv_portscan_train_xgboost_3306_label_0_P2 -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3306_label_0_R -Doutput1TableName=cv_portscan_train_xgboost_3306_label_0_P3 -Doutput2TableName=cv_portscan_train_xgboost_3306_label_0_P4 -Dfraction=0.5;
drop table if exists cv_portscan_train_xgboost_3306_P1;
drop table if exists cv_portscan_train_xgboost_3306_P2;
drop table if exists cv_portscan_train_xgboost_3306_P3;
drop table if exists cv_portscan_train_xgboost_3306_P4;
create table if not exists cv_portscan_train_xgboost_3306_P1 as select * from (select * from cv_portscan_train_xgboost_3306_label_0_P1 union all select * from cv_portscan_train_xgboost_3306_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3306_P2 as select * from (select * from cv_portscan_train_xgboost_3306_label_0_P2 union all select * from cv_portscan_train_xgboost_3306_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3306_P3 as select * from (select * from cv_portscan_train_xgboost_3306_label_0_P3 union all select * from cv_portscan_train_xgboost_3306_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3306_P4 as select * from (select * from cv_portscan_train_xgboost_3306_label_0_P4 union all select * from cv_portscan_train_xgboost_3306_label_1) tmp;
drop offlinemodel if exists cv_xgboost_3306_t1_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3306_P1" -DmodelName="cv_xgboost_3306_t1_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3306_t2_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3306_P2" -DmodelName="cv_xgboost_3306_t2_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3306_t3_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3306_P3" -DmodelName="cv_xgboost_3306_t3_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3306_t4_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3306_P4" -DmodelName="cv_xgboost_3306_t4_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";

drop table if exists cv_portscan_result_xgboost_3306_P1;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3306" -DoutputTableName="cv_portscan_result_xgboost_3306_P1" -DmodelName="cv_xgboost_3306_t1_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3306_P2;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3306" -DoutputTableName="cv_portscan_result_xgboost_3306_P2" -DmodelName="cv_xgboost_3306_t2_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3306_P3;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3306" -DoutputTableName="cv_portscan_result_xgboost_3306_P3" -DmodelName="cv_xgboost_3306_t3_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3306_P4;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3306" -DoutputTableName="cv_portscan_result_xgboost_3306_P4" -DmodelName="cv_xgboost_3306_t4_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
------
drop table if exists cv_portscan_train_xgboost_3389;
create table if not exists cv_portscan_train_xgboost_3389 as select * from portscan_train_all where port=3389;
drop table if exists cv_portscan_test_xgboost_3389;
create table if not exists cv_portscan_test_xgboost_3389 as select * from portscan_test_p2_final where port=3389;
drop table if exists cv_portscan_train_xgboost_3389_label_1;
create table if not exists cv_portscan_train_xgboost_3389_label_1 as select * from cv_portscan_train_xgboost_3389 where label=1;
drop table if exists cv_portscan_train_xgboost_3389_label_0;
create table if not exists cv_portscan_train_xgboost_3389_label_0 as select * from cv_portscan_train_xgboost_3389 where label=0;
drop table if exists cv_portscan_train_xgboost_3389_label_0_L;
drop table if exists cv_portscan_train_xgboost_3389_label_0_R;
drop table if exists cv_portscan_train_xgboost_3389_label_0_P1;
drop table if exists cv_portscan_train_xgboost_3389_label_0_P2;
drop table if exists cv_portscan_train_xgboost_3389_label_0_P3;
drop table if exists cv_portscan_train_xgboost_3389_label_0_P4;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3389_label_0 -Doutput1TableName=cv_portscan_train_xgboost_3389_label_0_L -Doutput2TableName=cv_portscan_train_xgboost_3389_label_0_R -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3389_label_0_L -Doutput1TableName=cv_portscan_train_xgboost_3389_label_0_P1 -Doutput2TableName=cv_portscan_train_xgboost_3389_label_0_P2 -Dfraction=0.5;
pai -name split -project algo_public -DinputTableName=cv_portscan_train_xgboost_3389_label_0_R -Doutput1TableName=cv_portscan_train_xgboost_3389_label_0_P3 -Doutput2TableName=cv_portscan_train_xgboost_3389_label_0_P4 -Dfraction=0.5;
drop table if exists cv_portscan_train_xgboost_3389_P1;
drop table if exists cv_portscan_train_xgboost_3389_P2;
drop table if exists cv_portscan_train_xgboost_3389_P3;
drop table if exists cv_portscan_train_xgboost_3389_P4;
create table if not exists cv_portscan_train_xgboost_3389_P1 as select * from (select * from cv_portscan_train_xgboost_3389_label_0_P1 union all select * from cv_portscan_train_xgboost_3389_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3389_P2 as select * from (select * from cv_portscan_train_xgboost_3389_label_0_P2 union all select * from cv_portscan_train_xgboost_3389_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3389_P3 as select * from (select * from cv_portscan_train_xgboost_3389_label_0_P3 union all select * from cv_portscan_train_xgboost_3389_label_1) tmp;
create table if not exists cv_portscan_train_xgboost_3389_P4 as select * from (select * from cv_portscan_train_xgboost_3389_label_0_P4 union all select * from cv_portscan_train_xgboost_3389_label_1) tmp;
drop offlinemodel if exists cv_xgboost_3389_t1_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3389_P1" -DmodelName="cv_xgboost_3389_t1_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3389_t2_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3389_P2" -DmodelName="cv_xgboost_3389_t2_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3389_t3_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3389_P3" -DmodelName="cv_xgboost_3389_t3_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";
drop offlinemodel if exists cv_xgboost_3389_t4_p2;
PAI -name xgboost -project algo_public -Deta="0.01" -DinputTableName="cv_portscan_train_xgboost_3389_P4" -DmodelName="cv_xgboost_3389_t4_p2" -Dobjective="binary:logistic" -DitemDelimiter="," -Dseed="0" -Dnum_round="500" -DlabelColName="label" -DenableSparse="false" -Dmax_depth="5" -Dsubsample="0.8" -Dcolsample_bytree="0.8" -Dgamma="0" -Dlambda="0" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -Dmin_child_weight="1" -DkvDelimiter=":";

drop table if exists cv_portscan_result_xgboost_3389_P1;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3389" -DoutputTableName="cv_portscan_result_xgboost_3389_P1" -DmodelName="cv_xgboost_3389_t1_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3389_P2;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3389" -DoutputTableName="cv_portscan_result_xgboost_3389_P2" -DmodelName="cv_xgboost_3389_t2_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3389_P3;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3389" -DoutputTableName="cv_portscan_result_xgboost_3389_P3" -DmodelName="cv_xgboost_3389_t3_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
drop table if exists cv_portscan_result_xgboost_3389_P4;
PAI -name prediction -project algo_public -DinputTableName="cv_portscan_test_xgboost_3389" -DoutputTableName="cv_portscan_result_xgboost_3389_P4" -DmodelName="cv_xgboost_3389_t4_p2" -DdetailColName="prediction_detail" -DappendColNames="client_port,source_ip,ds" -DitemDelimiter="," -DresultColName="prediction_result"  -DscoreColName="prediction_score" -DfeatureColNames="sip_client_ip_n,sip_client_ip_dn_cnt,sip_connect_n,sip_connect_n_max,sip_hashuserid_dn,sip_client_port2,sip_client_ip_n2,sip_client_ip_dn_cnt2,sip_connect_n2,sip_connect_n_max2,sip_hashuserid_dn2,sum_counts_cip_crt_m,max_counts_cip_crt_m,counts_distinct_cip_crt_m,sum_counts_cip_m,max_counts_cip_m,counts_distinct_cip_m,sum_counts_cip_crt_m2,max_counts_cip_crt_m2,counts_distinct_cip_crt_m2,sum_counts_cip_m2,max_counts_cip_m2,counts_distinct_cip_m2,sip_connect_n_avg,sip_connect_n_avg2,sip_client_ip_n_ratio,sip_client_ip_dn_cnt_ratio,sip_connect_n_ratio,sip_connect_n_max_ratio,sip_connect_n_avg_ratio,sum_counts_cip_crt_a,max_counts_cip_crt_a,avg_counts_cip_crt_m,avg_counts_cip_crt_a,counts_distinct_cip_crt_a,sum_counts_cip_a,max_counts_cip_a,counts_distinct_cip_a,avg_counts_cip_m,avg_counts_cip_a,sum_counts_cip_crt_a2,max_counts_cip_crt_a2,avg_counts_cip_crt_m2,avg_counts_cip_crt_a2,counts_distinct_cip_crt_a2,sum_counts_cip_a2,max_counts_cip_a2,counts_distinct_cip_a2,avg_counts_cip_m2,avg_counts_cip_a2" -DenableSparse="false";
----
drop table if exists phase2_q1_answer;
create table if not exists phase2_q1_answer as select source_ip,ds,client_port,count(*) as counts from
(select source_ip,ds,client_port from cv_portscan_result_xgboost_21_P1 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_21_P2 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_21_P3 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_21_P4 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_22_P1 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_22_P2 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_22_P3 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_22_P4 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3306_P1 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3306_P2 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3306_P3 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3306_P4 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3389_P1 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3389_P2 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3389_P3 where prediction_result=1 union all
 select source_ip,ds,client_port from cv_portscan_result_xgboost_3389_P4 where prediction_result=1 
) tmp group by source_ip,ds,client_port;



--create table if not exists adl_tianchi_portscan_trainset_login_part2 as select * from odps_tc_257100_f673506e024.adl_tianchi_portscan_trainset_login_part2;
--drop table if exists adl_tianchi_portscan_trainset_login_part2_ss;
--create table if not exists adl_tianchi_portscan_trainset_login_part2_ss as select client_port,source_ip,ds from adl_tianchi_portscan_trainset_login_part2 group by client_port,source_ip,ds;

drop table if exists blacklist;
create table if not exists blacklist as 
select * from (select source_ip,client_port,count(*) as counts from (select a.ds,a.source_ip,a.client_port,case when b.source_ip is not null and b.client_port is not null and a.ds is not null then 1 else 0 end as isright from portscan_train_all a  left outer join adl_tianchi_portscan_trainset_login_part2_ss b on a.source_ip=b.source_ip and a.client_port=b.client_port and a.ds=b.ds where label=1 and a.ds>="20170625") tmp  group by source_ip,client_port ) tmp2 where counts>2;

--drop table if exists whitelist;
--create table if not exists whitelist as 
--select * from (select source_ip,sum(label) as sumlabel,(count(*)-sum(label)) as counts from (select label,a.ds,a.source_ip,a.client_port,case when b.source_ip is not null and b.client_port is not null and a.ds is not null then 1 else 0 end as isright from portscan_train_all a  left outer join adl_tianchi_portscan_trainset_login_part2_ss b on a.source_ip=b.source_ip and a.client_port=b.client_port and a.ds=b.ds) tmp  where isright=1 group by source_ip ) tmp2 where sumlabel=0;



--drop table if exists blacklist2;
--create table if not exists blacklist2 as 
--select * from (select source_ip,client_port,count(*) as counts from (select a.ds,a.source_ip,a.client_port,case when b.source_ip is not null and b.client_port is not null and a.ds is not null then 1 else 0 end as isright from portscan_train_all a  left outer join adl_tianchi_portscan_trainset_login_part2_ss b on a.source_ip=b.source_ip and a.client_port=b.client_port and a.ds=b.ds where label=1) tmp  group by source_ip,client_port ) tmp2 where counts>3;


--select count(*) from (
--select source_ip,ds,client_port from (
	--select * from tianchi_portscan_answer union all
	--select a.source_ip,a.ds,a.client_port from portscan_test_p2_final a inner join blacklist b on a.source_ip=b.source_ip and a.client_port=b.client_port
--) tmp group by source_ip,ds,client_port ) tmp2;
--select count(*) from tianchi_portscan_answer;

--drop table if exists adl_tianchi_portscan_trainset_login_part1_ss;
--create table if not exists adl_tianchi_portscan_trainset_login_part1_ss as select client_port,source_ip,ds from adl_tianchi_portscan_trainset_login_part1 group by client_port,source_ip,ds;

--drop table if exists adl_tianchi_portscan_trainset_label_part1_grouped;
--create table if not exists adl_tianchi_portscan_trainset_label_part1_grouped as select * from (select source_ip,client_port,count(*) as counts from (
--select a.source_ip,a.client_port,a.ds,case when b.source_ip is not null and b.client_port is not null and a.ds is not null then 1 else 0 end as isright from adl_tianchi_portscan_trainset_label_part1 a left outer join adl_tianchi_portscan_trainset_login_part1_ss b on a.source_ip=b.source_ip and  a.client_port=b.client_port and a.ds=b.ds ) tmp group by source_ip,client_port) tmp2 where counts>1;
--drop table if exists adl_tianchi_portscan_trainset_label_part2_tt;
--create table if not exists adl_tianchi_portscan_trainset_label_part2_tt as select source_ip,ds,client_port from adl_tianchi_portscan_trainset_label_part2 where ds="20170625" or ds="20170626";
--drop table if exists poortscan_test_v3_tt;
--create table if not exists poortscan_test_v3_tt as select source_ip,client_port,ds from (select a.source_ip,a.client_port,a.ds from poortscan_test_v3 a inner join adl_tianchi_portscan_trainset_label_part1_grouped b on a.source_ip=b.source_ip and a.client_port=b.client_port ) tmp  where ds="20170625" or ds="20170626";
--drop table if exists check_poortscan_test_v3_tt;
--create table if not exists check_poortscan_test_v3_tt as select a.source_ip,a.client_port,a.ds , case when b.source_ip is not null and b.client_port is not null and a.ds is not null then 1 else 0 end as isright from poortscan_test_v3_tt a left outer join adl_tianchi_portscan_trainset_label_part2_tt b on a.source_ip=b.source_ip and a.client_port=b.client_port and a.ds=b.ds;
--select sum(isright)/count(isright) from check_poortscan_test_v3_tt;
--select sum(isright) from check_poortscan_test_v3_tt;
--drop table if exists tianchi_portscan_answer_new;

--drop table if exists tianchi_portscan_answerblacklist;
--create table if not exists tianchi_portscan_answerblacklist as 
--select source_ip,ds,client_port from (
	--select source_ip,ds,client_port from phase2_q1_answer union all
	--select a.source_ip,a.ds,a.client_port from portscan_test_p2_final a inner join blacklist b on a.source_ip=b.source_ip and a.client_port=b.client_port
--) tmp group by source_ip,ds,client_port;
DROP TABLE IF EXISTS whitelist;

CREATE TABLE IF NOT EXISTS whitelist
AS
SELECT *
FROM (
	SELECT source_ip, SUM(label) AS sumlabel
		, COUNT(*) - SUM(label) AS counts
	FROM (
		SELECT label, a.ds, a.source_ip, a.client_port
			, CASE 
				WHEN b.source_ip IS NOT NULL
				AND b.client_port IS NOT NULL
				AND a.ds IS NOT NULL THEN 1
				ELSE 0
			END AS isright
		FROM portscan_train_all a
		LEFT OUTER JOIN adl_tianchi_portscan_trainset_login_part2_ss b
		ON a.source_ip = b.source_ip
			AND a.client_port = b.client_port
			AND a.ds = b.ds
	) tmp
	WHERE isright = 1
	GROUP BY source_ip
) tmp2
WHERE sumlabel = 0;
--drop table if exists tianchi_portscan_answerblacklistwhitelist;
--create table if not exists tianchi_portscan_answerblacklistwhitelist as 
--select source_ip,ds,client_port from (
	----select a.*,case when b.source_ip is not null then 1 else 0 end as label from tianchi_portscan_answerblacklist a left outer join whitelist b on a.source_ip=b.source_ip
--) tmp where label=0 group by source_ip,ds,client_port;
--select client_port,count(*) from tianchi_portscan_answer_before group by client_port;
--select client_port,count(*) from tianchi_portscan_answerblacklist group by client_port;
--select client_port,count(*) from tianchi_portscan_answerblacklistwhitelist group by client_port;
--create table if not exists tianchi_portscan_answer923 as select * from tianchi_portscan_answer;
--drop table if exists tianchi_portscan_answer;
--create table if not exists tianchi_portscan_answer as select * from tianchi_portscan_answerblacklistwhitelist;

--select client_port,count(*) from tianchi_portscan_answer group by client_port;

--drop table if exists tianchi_portscan_answer_before;
--create table if not exists tianchi_portscan_answer_before as select * from tianchi_portscan_answer;
--drop table if exists tianchi_portscan_answer;
--create table if not exists tianchi_portscan_answer as select * from tianchi_portscan_answer_new;
--select client_port,count(*) from tianchi_portscan_answer group by client_port;

--PAI -name prediction -project algo_public -DoutputTableName="Q2_all_title_model_result3" -DinputTableName="testset_q2_new" -DmodelName="ps_smart_q3" -DdetailColName="prediction_detail" -DappendColNames="id" -DitemDelimiter="," -DresultColName="prediction_result" -DscoreColName="prediction_score" -DkvDelimiter=":" -DfeatureColNames="kv" -DenableSparse="true";
--select prediction_result,count(*) from (select * from Q2_all_title_model_result3  where prediction_score>0.7) a group by prediction_result;
--select prediction_result,count(*) from Q2_all_title_model_result2 group by prediction_result;
