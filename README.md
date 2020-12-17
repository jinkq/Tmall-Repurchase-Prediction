# 181098118-金可乔-实验4
[toc]
## 任务目标
1. 分别编写MapReduce程序和Spark程序统计双⼗⼀最热⻔的商品和最受年轻⼈(age<30)关注的商家
（“添加购物⻋+购买+添加收藏夹”前100名）；
2. 编写Spark程序统计双⼗⼀购买了商品的男⼥⽐例，以及购买了商品的买家年龄段的⽐例；
3. 基于Hive或者Spark SQL查询双⼗⼀购买了商品的男⼥⽐例，以及购买了商品的买家年龄段的⽐
例；
4. 预测给定的商家中，哪些新消费者在未来会成为忠实客户，即需要预测这些新消费者在6个⽉内再
次购买的概率。基于Spark MLlib编写程序预测回头客，评估实验结果的准确率。

## 统计双⼗⼀最热⻔的商品和最受年轻⼈(age<30)关注的商家
### MapReduce
#### 统计最热门商品
1. 新建job`measureItemsPopularityJob`，读取数据文件`user_log_format1.csv`，对于数据文件中`action_type`为1、2或3的样本的`item_id`字段进行计数，输出<key, value>=<item_id, 出现次数>，保存至临时文件夹`tempDir`
2. 新建job`sortItemsPopularityJob`，读取`tempDir`中的数据，利用InverseMapper将键和值调换位置，再根据新的键进行降序排序，保留前100条数据作为输出
#### 统计最受年轻⼈(age<30)关注的商家
1. 自定义数据类型UserLog，属性为商家ID`sellerId`（String，默认为""）和买家年龄是否小于30`userAge`（Boolean，默认为false）
2. 新建job`mergeTableJob`
    1. 在Mapper中读取数据文件`user_log_format1.csv`和`user_info_format1.csv`，根据读取行按照","分割的数据长度判断该行来自于哪个csv。对于来自`user_log_format1.csv`的样本，若其`action_type`为1、2或3，则新建`UserLog`对象`userLog`，并设置其`sellerId`属性为样本的`seller_id`字段值，输出<key, value>=<user_id, userLog>；对于来自`user_info_format1.csv`的样本，若其`age_range`为1、2或3，则新建`UserLog`对象，并设置其`userAge`属性为true，输出<key, value>=<user_id, userLog>
    2. 在Reducer中，检测同一user_id的一系列UserLog对象，若对象的`sellerId`属性不为空字符串，则认定该user买了商品，并将该sellerId加入列表`sellers`；若对象的'userAge'属性为true，则认定该user为年轻人。遍历完所有UserLog对象，若该user既买了商品，又是年轻人，则将列表`sellers`中的每一个seller的ID作为key输出，即输出<key, value>=<seller_id, NullWritable>，删除原有的tempDir，并将输出写入临时文件夹tempDir。
3. 新建job`measureMerchantsPopularityJob`，读取tempDir中的数据，对每一行的seller_id进行计数，输出<key, value>=<seller_id, 出现次数>
4. 新建job`sortMerchantsPopularityJob`，除了输出的文字，其他均与`sortItemsPopularityJob`相同
### Spark
（“添加购物⻋+购买+添加收藏夹”前100名）
hadoop jar /home/jkq/FBDP/Tmall\ repurchase\ prediction/RepurchasePrediction/target/RepurchasePrediction-1.0-SNAPSHOT.jar input output

spark-submit --class "ScalaWordCount" --master spark://jkq181098118-master:7077 target/scala-2.11/find-hottest-items-and-popular-merchants_2.11-1.0.jar hdfs://jkq181098118-master:9000/user/root/input_test/test.txt hdfs://jkq181098118-master:9000/user/root/output