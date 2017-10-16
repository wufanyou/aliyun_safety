##第二届阿里云安全算法挑战赛

吴凡优（铁球）<br>
fanyou.wu@outlook.com<br>
https://github.com/wufanyou/aliyun_safety

#### 端口扫描爆破


1.端口扫描特征表

|特征名前后缀|特征含义|
|-----|-------|
|sip_client~|按照souce_ip，端口，日期汇总的连接统计数据|
|sip_connect~|按照souce_ip，日剧汇总的连接统计数据|
|~ratio|sip\_client/sip\_connect 前2者的比值|
|~counts_crt|按照client_ip,端口，日期汇总的连接统计数据（非最终特征）|
|~counts_cip|按照client_ip，日期汇总的连接数据（非最终特征）|
|~counts_crt~|按照source\_ip,端口，日期汇总的~counts\_crt特征|
|~counts_cip~|按照source\_ip,端口，日期汇总的~counts\_cip特征|

2.模型参数

|模型参数名|模型参数值|
|-----|-------|
|num_round|500|
|max_depth|0.8|
|colsample_bytree|0.8|
|min\_child\_weight|1|
|eta|0.01|
|gamma|0|
|lambda|0|

3.模型特征及参数

每个端口的四个xgboost模型特征完全一致，仅仅在训练集的选取上有差异（正例和1/4负例），结果取四个模型并集。

4.黑名单规则

新数据中没有被标注为恶意的用户且连接天数大于2。


#### 网页风险识别

1.网页预处理处理

* 提取网页全文中文
* 提取网页标题中文

2.分词&停用词过滤

* 分词方法：PAI 分词 互联网词汇
* 停用词表：网上随便下的

3.特征工程及特征选择

* 卡方检验选取TOP10000词汇（方法来自FMMM团队)
* TF-IDF 构造向量

4.模型训练

* Logistic regression 单模型



