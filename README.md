# 181098118-金可乔-实验4
[toc]
## 任务目标
1. 分别编写MapReduce程序和Spark程序统计双十一最热门的商品和最受年轻人(age<30)关注的商家（“添加购物⻋+购买+添加收藏夹”前100名）；
2. 编写Spark程序统计双十一购买了商品的男女比例，以及购买了商品的买家年龄段的比例；
3. 基于Hive或者Spark SQL查询双十一购买了商品的男女比例，以及购买了商品的买家年龄段的比例；
4. 预测给定的商家中，哪些新消费者在未来会成为忠实客户，即需要预测这些新消费者在6个月内再次购买的概率。基于Spark MLlib编写程序预测回头客，评估实验结果的准确率。

## 文件夹目录
* FindHottestItemsAndPopularMerchants：任务1
* FindGenderRatioAndAgeRangeRatio：任务2、3
* RepurchasePrediction：任务4

## 说明

* MapReduce使用Java语言，用Maven管理
* Spark使用Scala语言，用sbt管理

## 1. 统计双十一最热门的商品和最受年轻人(age<30)关注的商家
### MapReduce
#### 输入、输出
* 输入：input文件夹（仅包含`user_log_format1.csv`和`user_info_format1.csv`）
* 输出：FindHottestItemsAndPopularMerchants/MapReduce/output
#### 设计思路
##### 统计最热门商品
1. 新建job`measureItemsPopularityJob`，读取数据文件`user_log_format1.csv`，对于数据文件中`action_type`为1、2或3且`time_stamp`为1111的样本的`item_id`字段进行计数，输出<key, value>=<item_id, 出现次数>，保存至临时文件夹`tempDir`
2. 新建job`sortItemsPopularityJob`，读取`tempDir`中的数据，利用InverseMapper将键和值调换位置，再根据新的键进行降序排序，保留前100条数据作为输出
##### 统计最受年轻人(age<30)关注的商家
1. 自定义数据类型UserLog，属性为商家ID`sellerId`（String，默认为""）和买家年龄是否小于30`userAge`（Boolean，默认为false）
2. 新建job`mergeTableJob`
    1. 在Mapper中读取数据文件`user_log_format1.csv`和`user_info_format1.csv`，根据读取行按照","分割的数据长度判断该行来自于哪个csv。
        * 对于来自`user_log_format1.csv`的样本，若其`action_type`为1、2或3且`time_stamp`为1111，则新建`UserLog`对象`userLog`，并设置其`sellerId`属性为样本的`seller_id`字段值，输出<key, value>=<user_id, userLog>；
        * 对于来自`user_info_format1.csv`的样本，若其`age_range`为1、2或3，则新建`UserLog`对象，并设置其`userAge`属性为true，输出<key, value>=<user_id, userLog>
    2. 在Reducer中，检测同一user_id的一系列UserLog对象，
        * 若对象的`sellerId`属性不为空字符串，则认定该user在双十一当天买了商品，并将该sellerId加入列表`sellers`；
        * 若对象的`userAge`属性为true，则认定该user为年轻人。

        遍历完所有UserLog对象，若该user既买了商品，又是年轻人，则将列表`sellers`中的每一个seller的ID作为key输出，即输出<key, value>=<seller_id, NullWritable>，删除原有的tempDir，并将输出写入临时文件夹tempDir。
3. 新建job`measureMerchantsPopularityJob`，读取tempDir中的数据，对每一行的seller_id进行计数，输出<key, value>=<seller_id, 出现次数>，保存至临时文件夹`tempDir2`
4. 新建job`sortMerchantsPopularityJob`，除了输出的文字，其他均与`sortItemsPopularityJob`相同
#### 运行方法
`hadoop jar <FindHottestItemsAndPopularMerchants-1.0-SNAPSHOT.jar路径> <input> <output>`

* \<input>中仅含user_log_format1.csv和user_info_format1.csv
* \<output>将含文件夹hottest items和popular merchants among young，分别存储双十一最热门的商品和最受年轻人(age<30)关注的商家
### Spark
#### 输入、输出
* 输入：input文件夹（仅包含`user_log_format1.csv`和`user_info_format1.csv`）
* 输出：FindHottestItemsAndPopularMerchants/Spark/output
#### 设计思路
1. 读取数据文件`user_log_format1.csv`和`user_info_format1.csv`，根据读取行按照","分割的数据长度判断该行来自于哪个csv
##### 统计最热门商品
2. 从log表（user_log_format1.csv）中用map筛选出所有`action_type`为1、2或3且`time_stamp`为1111的行，对这些行的`item_id`字段进行计数并降序排序
##### 统计最受年轻人(age<30)关注的商家
3. 从info表（user_info_format1.csv）中用map筛选出所有`age_range`为1、2或3的行所对应的`user_id`，将这些user与统计最热门商品中获得的双十一当天购买了商品的users取交集
4. 对于交集中user对应的`seller_id`字段进行计数并降序排序

#### 运行方法

`spark-submit --class "FindHottestItemsAndPopularMerchants" --master local <find-hottest-items-and-popular-merchants_2.11-1.0.jar路径> <input> <output>`

* \<input>中仅含user_log_format1.csv和user_info_format1.csv
* \<output>将含文件夹hottest items和popular merchants among young，分别存储双十一最热门的商品和最受年轻人(age<30)关注的商家

## 2、3. 统计双十一购买了商品的男女比例，以及购买了商品的买家年龄段的比例
### Spark
#### 输入、输出
* 输入：input文件夹（仅包含`user_log_format1.csv`和`user_info_format1.csv`）
* 输出：FindGenderRatioAndAgeRangeRatio/Spark/output
#### 设计思路
1. 读取数据文件`user_log_format1.csv`和`user_info_format1.csv`，根据读取行按照","分割的数据长度判断该行来自于哪个csv
2. 从log表（user_log_format1.csv）中用map筛选出所有`action_type`为1、2或3且`time_stamp`为1111的users
3. 从info表（user_info_format1.csv）中用map筛选出所有对应性别或年龄的users与上述users取交集，再计算对应的人数及比例

#### 运行方法

`spark-submit --class "FindGenderRatioAndAgeRangeRatio" --master local <find-gender-ratio-and-age-range-ratio_2.11-1.0.jar路径> <input> <output>`

* \<input>中仅含user_log_format1.csv和user_info_format1.csv
* \<output>将含文件夹age range和gender，分别存储对性别及年龄段的人数、比例的统计

### Spark SQL

#### 输入、输出

* 输入：input文件夹（仅包含`user_log_format1.csv`和`user_info_format1.csv`）
* 输出：FindGenderRatioAndAgeRangeRatio/Spark SQL/output

#### 设计思路

与普通Spark做法的区别是：使用DataFrame作为基础数据结构，用where语句进行条件的筛选，用join语句进行DataFrame的合并

#### 运行方法

`spark-submit --class "FindGenderRatioAndAgeRangeRatioBySQL" --master local <find-gender-ratio-and-age-range-ratio-by-sql_2.11-1.0.jar路径> <input> <output>`

* \<input>中仅含user_log_format1.csv和user_info_format1.csv
* \<output>将含文件夹age range和gender，分别存储对性别及年龄段的人数、比例的统计

### 运行结果

#### 性别

购买了商品的男性人数：121670，比例：29.87%
购买了商品的女性人数：285638，比例：70.13%

#### 年龄段

年龄区间为<18的人数：24，比例：0.01%
年龄区间为[18, 24]的人数：52420，比例：16.03%
年龄区间为[25, 29]的人数：110952，比例：33.92%
年龄区间为[30, 34]的人数：79649，比例：24.35%
年龄区间为[35, 39]的人数：40601，比例：12.41%
年龄区间为[40, 49]的人数：35257，比例：10.78%
年龄区间为≥50的人数：8167，比例：2.50%

## 4. MLlib预测新消费者在6个月内再次购买的概率

#### 输入

* 输入：data_format1文件夹

#### 设计思路

##### 数据读取


利用`sparkSession.read`进行数据的分别读取（指定Schema），存储为DataFrame

##### 特征工程

特征的选取参考baseline

参数说明：

* infoDf：user_info_format1.csv读入的DataFrame
* logDf：user_log_format1.csv读入的DataFrame
* trainDf：train_format1.csv读入的DataFrame

###### 用户年龄范围、性别

1. 将trainDf和infoDf进行左内连接，获得infoLogDf

###### 用户在该商家日志的总条数

1. 对logDf关于user_id和seller_id进行groupBy与count操作，得到特征`total_tags`
2. 将特征以join的方式加入为infoLogDf的列

###### 用户浏览的该商家商品的数目、用户浏览的该商家商品种类的数目、用户浏览的天数

以用户浏览的该商家商品的数目为例：

1. 对logDf关于user_id、seller_id和item_id进行groupBy与count操作，得到browseDf
2. 对browseDf关于user_id和seller_id进行groupBy与count操作，得到特征`browse_item_id`
3. 将特征以join的方式加入为infoLogDf的列

###### 用户对该商家单击、添加购物车、购买、添加收藏夹的次数

1. 对logDf关于user_id、seller_id和action_type进行groupBy与count操作，得到browseDf
2. 遍历action 0、1、2、3，对于每个action进行如下操作：新增flag列，定义udf聚合函数，若action_type == action，则flag为1，否则为0。新增click/shopping_cart/buy/favourite列，定义udf聚合函数，若flag == 1，则click/shopping_cart/buy/favourite为count值，否则为0。
3. 将特征以join的方式加入为infoLogDf的列

##### 模型的训练与预测

* 按照8:2的比例将数据划分为训练集和测试集

* 使用MLlib中的LogisticRegressionWithSGD进行分类

* (在预测前执行`lrModel.clearThreshold()`可以获得分类概率，而非获得类别标签)

##### 模型评价

* 准确率：94.12%

#### 运行方法

`spark-submit --class "RepurchasePrediction" --master local <repurchase-prediction_2.11-1.0.jar路径> <input>`

* \<input>为data_format1文件夹路径

## 实验中遇到的问题

1. 在bdkit中遇到进入其他同学控制台的情况，并且发现错乱时按Ctrl+L可以在我自己和其他同学的控制台间进行切换

2. 在MLlib的任务中执行`sbt package`一直卡在update

   ![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/error.png)

   换过源依然几千秒都没update完，后来发现居然是因为宿舍校园网太慢了，换个网络就成功了

3. 任务2、3一开始我没有筛选双十一当天，但得到的性别、年龄段人数都与筛选过双十一当天得到的结果完全一样。一开始我以为有bug，但是在多番测试后发现结果确实是一样的，我能想到的解释就是所有在双十一买了东西的人都在双十一前6个月买过东西。

