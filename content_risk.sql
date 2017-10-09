--网页预处理，函数webparse为jar正则，仅仅获取中文

drop table if exists bodytrainsetPhase1;
-reate table if not exists bodytrainsetPhase1 as select id,webparse(html),risk from odps_tc_257100_f673506e024.adl_tianchi_content_risk_testing_phase1_with_answer;

drop table if exists bodytrainsetPhase1_splited_word;
PAI -name split_word -project algo_public -DenableCheckNumber="true" -DenablePosTagger="false" -DenableSemanticTagger="false" -Dlifecycle="30" -DenableCheckCharSequence="true" -DoutputTableName="bodytrainsetPhase1_splited_word" -Dtokenizer="INTERNET_CHN" -DinputTableName="bodytrainsetPhase1" -DselectedColNames="_c1" -DenableCheckPunctuation="true";

pai -name doc_word_stat -project algo_public -DinputTableName="bodytrainsetPhase1_splited_word" -DdocId="id" -DdocContent="_c1" -DoutputTableNameMulti="bodytrainsetPhase1_splited_word_Multi" -DoutputTableNameTriple="bodytrainsetPhase1_splited_word_Triple";


------特征选择,allkeywords 来源于卡方检验（取前KAI值TOP100000并要求数量大于500）
create table pai_temp_67013_939605_1 lifecycle 30as select word,doc_count from pai_temp_67013_938936_1 group by word,doc_count;

create table pai_temp_67013_938854_1 lifecycle 30as select a.*,b.risk from bodytrainsetphase1_splited_word_triple a inner join odps_tc_257100_f673506e024.adl_tianchi_content_risk_testing_phase1_with_answer b on a.id=b.id;
create table pai_temp_67013_938624_1 lifecycle 30as select * from (select a.*,case when b.word is null then 0 else 1 end as isin from pai_temp_67013_938854_1 a left outer join stopwords_new b on a.word=b.word ) tmp where isin=0;
create table pai_temp_67013_938936_1 lifecycle 30as select id,word,count(word) over (partition by word) as doc_count from pai_temp_67013_938624_1;
create table pai_temp_67013_939605_1 lifecycle 30as select word,doc_count from pai_temp_67013_938936_1 group by word,doc_count;
create table pai_temp_67013_938569_1 lifecycle 30as select id,word,count(word) over (partition by word) as doc_count from pai_temp_67013_938624_1 where risk="sexy";
create table pai_temp_67013_938587_1 lifecycle 30as select id,word,count(word) over (partition by word) as doc_count from pai_temp_67013_938624_1 where risk="gambling";
create table pai_temp_67013_938588_1 lifecycle 30as select id,word,count(word) over (partition by word) as doc_count from pai_temp_67013_938624_1 where risk="fake_card";
create table pai_temp_67013_938589_1 lifecycle 30as select id,word,count(word) over (partition by word) as doc_count from pai_temp_67013_938624_1 where risk="normal";
create table pai_temp_67013_938986_1 lifecycle 30as select word,risk,counts from (select word,"sexy" as risk,doc_count as counts from pai_temp_67013_938569_1 union allselect word,"gambling" as risk,doc_count as counts from pai_temp_67013_938587_1 union allselect word,"fake_card" as risk,doc_count as counts from pai_temp_67013_938588_1 union allselect word,"normal" as risk,doc_count as counts frompai_temp_67013_938589_1) tmp;
create table pai_temp_67013_939604_1 lifecycle 30as selectword,risk,counts from pai_temp_67013_938986_1 group by word,risk,counts;
create table pai_temp_67013_939606_1 lifecycle 30as select a.*,b.doc_count from pai_temp_67013_939604_1 a left outer join pai_temp_67013_939605_1 b on a.word=b.word;
create table pai_temp_67013_939071_1 lifecycle 30as select *,counts as sexycount from pai_temp_67013_939606_1 where risk="sexy";
create table pai_temp_67013_939124_1 lifecycle 30as select *,counts as gamblingcounts from pai_temp_67013_939606_1 where risk="gambling";
create table pai_temp_67013_939130_1 lifecycle 30as select *,counts as fakecardcounts from pai_temp_67013_939606_1 where risk="fake_card";
create table pai_temp_67013_939133_1 lifecycle 30as select *,counts as normalcount from pai_temp_67013_939606_1 where risk="normal";
create table pai_temp_67013_939545_1 lifecycle 30as select word, (normalcount)/348595 as normalA, (doc_count-normalcount)/348595 a snormalB, (312547-normalcount)/348595 as normalC, (348595-doc_count-312547+normalcount)/348595 as normalD from pai_temp_67013_939133_1;
create table pai_temp_67013_939533_1 lifecycle 30as select word, fakecardcounts / 348595 as fakeA, (doc_count - fakecardcounts) / 348595 as fakeB, (7045 - fakecardcounts) / 348595 as fakeC, (348595 - doc_count - 7045 + fakecardcounts) / 348595 as fakeD from pai_temp_67013_939130_1;
create table pai_temp_67013_939499_1 lifecycle 30as select word, (gamblingcounts) / 348595 as gambleA, (doc_count - gamblingcounts) / 348595 as gambleB, (23013 - gamblingcounts) / 348595 as gambleC, (348595 - doc_count - 23013 + gamblingcounts) / 348595 as gambleD from pai_temp_67013_939124_1;
create table pai_temp_67013_939472_1 lifecycle 30as select word, sexycount / 348595 as sexyA, (doc_count - sexycount) / 348595 as sexyB, (5990 - sexycount) / 348595 as sexyC, (348595 - doc_count - 5990 + sexycount) / 348595 as sexyD from pai_temp_67013_939071_1;
create table pai_temp_67013_939580_1 lifecycle 30as select *, (sexyA * sexyD - sexyC * sexyB) * (sexyA * sexyD - sexyC * sexyB) / ( (sexyA + sexyC) * (sexyB + sexyD) * (sexyA + sexyB) * (sexyC + sexyD) ) as sexy_x2 from pai_temp_67013_939472_1;
create table pai_temp_67013_939613_1 lifecycle 30as select *, (gambleA * gambleD - gambleC * gambleB) * (gambleA * gambleD - gambleC * gambleB) / ( (gambleA + gambleC) * (gambleB + gambleD) * (gambleA + gambleB) * (gambleC + gambleD) ) as gamble_x2 from pai_temp_67013_939499_1;
create table pai_temp_67013_939616_1 lifecycle 30as select *, (fakeA * fakeD - fakeC * fakeB) * (fakeA * fakeD - fakeC * fakeB) / ( (fakeA + fakeC) * (fakeB + fakeD) * (fakeA + fakeB) * (fakeC + fakeD) ) as fake_x2from pai_temp_67013_939533_1;
create table pai_temp_67013_939617_1 lifecycle 30as select *, (normalA * normalD - normalC * normalB) * (normalA * normalD - normalC * normalB) / ( (normalA + normalC) * (normalB + normalD) * (normalA + normalB) * (normalC + normalD) ) as normal_x2 from pai_temp_67013_939545_1;
create table pai_temp_67013_939592_1 lifecycle 30as select word,max(kai) as maxkai from (select word,normal_x2 as kai from pai_temp_67013_939617_1 union all select word,fake_x2 as kai from pai_temp_67013_939616_1 union all select word,gamble_x2 as kai from pai_temp_67013_939613_1 union all select word,sexy_x2 as kai from pai_temp_67013_939580_1) tmp group by word;
create table pai_temp_67013_939638_1 lifecycle 30as select a.*,b.doc_count from pai_temp_67013_939592_1 a left outer join pai_temp_67013_939605_1 b on a.word=b.word;
create table pai_temp_67013_939634_1 lifecycle 30as select * from pai_temp_67013_939638_1 cluster by -1*maxkai limit 10000;
create table allkeywords lifecycle 30as select * from pai_temp_67013_939634_1 where doc_count>500;
-------------



drop table if exists bodytestset;
create table if not exists bodytestset as select id,webparse(html) from odps_tc_257100_f673506e024.adl_tianchi_content_risk_testing_phase2;

drop table if exists bodytestset_splited_word;
PAI -name split_word -project algo_public -DenableCheckNumber="true" -DenablePosTagger="false" -DenableSemanticTagger="false" -Dlifecycle="30" -DenableCheckCharSequence="true" -DoutputTableName="bodytestset_splited_word" -Dtokenizer="INTERNET_CHN" -DinputTableName="bodytestset" -DselectedColNames="_c1" -DenableCheckPunctuation="true";

pai -name doc_word_stat -project algo_public -DinputTableName="bodytestset_splited_word" -DdocId="id" -DdocContent="_c1" -DoutputTableNameMulti="bodytestset_splited_word_Multi" -DoutputTableNameTriple="bodytestset_splited_word_Triple";

create table if not exists bodytestset_splited_word_TripleIn_keywords as select id,a.word,count from bodytestset_splited_word_Triple a inner join allkeywords b on a.word=b.word;
create table if not exists bodytrainsetPhase1_splited_word_TripleIn_keywords as select id,a.word,count from bodytestset_splited_word_Triple a inner join allkeywords b on a.word=b.word;

drop table if exists allset;
create table if not exists allset as select * from ( select id,word,count from bodytestset_splited_word_TripleIn_keywords union all select id,word,count from bodytrainset_splited_word_TripleIn_keywords union all  select id,word,count from bodytrainsetPhase1_splited_word_TripleIn_keywords) tmp;
pai -name tfidf -project algo_public -DinputTableName=allset -DdocIdCol=id -DwordCol=word -DcountCol=count -DoutputTableName=allset_tfidf;
PAI -name triple_to_kv -project algo_public -DvalueColName="tfidf" -Dlifecycle="30" -DindexOutputTableName="pai_temp_67013_939679_2" -DoutputTableName="allset_tfidfkv" -DkvDelimiter=":" -DidColName="id" -DinputTableName="allset_tfidf" -DpairDelimiter="," -DkeyColName="word";
create table if not exists trainset as select a.*,case when b.risk="normal" then 0 when b.risk="sexy" then 1 when b.risk="gambling" then 2 else 3 end as label from allset_tfidfkv a inner join bodytrainsetPhase1 b on a.id=b.id;
create table if not exists testset as select a.* from allset_tfidfkv a inner join bodytestset b on a.id=b.id;
PAI -name logisticregression_multi -project algo_public -DmodelName="xlab_m_logisticregress_939707_v0" -DitemDelimiter="," -DregularizedLevel="1" -DmaxIter="100" -DregularizedType="None" -Depsilon="0.000001" -DlabelColName="label" -DkvDelimiter=":" -DfeatureColNames="key_value" -DenableSparse="true" -DinputTableName="trainset";
PAI -name prediction -project algo_public -DdetailColName="prediction_detail" -DappendColNames="id" -DmodelName="xlab_m_logisticregress_939707_v0" -DitemDelimiter="," -DresultColName="prediction_result" -Dlifecycle="30" -DoutputTableName="testsetresult" -DscoreColName="prediction_score" -DkvDelimiter=":" -DfeatureColNames="key_value" -DinputTableName="testset" -DenableSparse="true";
----模型预测生成的表
create table adl_tianchi_content_risk_testing_phase2_answer lifecycle 30as select id,case when prediction_result=1 then "sexy" when prediction_result=2 then "gambling" when prediction_result=3 then "fake_card" else "normal" end as risk from testsetresult where prediction_result!=0;
------------- 最终版本有加一些人工关键词的干预，用来适当提高精度 代码如下 函数 titleparse2为jar包，用于提取title
create table if not exists hand_modifed1
as select a.id,titleparse2(b.html) as content,a.risk from adl_tianchi_content_risk_testing_phase2_answer a left outer join odps_tc_257100_f673506e024.adl_tianchi_content_risk_testing_phase1_with_answer b on a.id=b.id;
drop table if exists adl_tianchi_content_risk_testing_phase2_answer;
create table if not exists adl_tianchi_content_risk_testing_phase2_answer 
as select id,case when (content like "%小姐%" or content like "%小妹%" or content like"%夜总会%") and risk!="sexy" then "sexy" else risk end as risk from hand_modifed1 where content not like "%育德教育%" and content not like "%三徒弟网%" and content not like "%企业观察网%" and content not like "%乐山杂谈%" and content not like "%云雀书院手机阅读%" and content not like "%代办网%" and content not like "%四川幼师学校%" and content not like "%北京免费发布昌平区%" and content not like "%招聘网%";



