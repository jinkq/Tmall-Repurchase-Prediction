# 统计双⼗⼀最热⻔的商品和最受年轻⼈(age<30)关注的商家
（“添加购物⻋+购买+添加收藏夹”前100名）
hadoop jar /home/jkq/FBDP/Tmall\ repurchase\ prediction/RepurchasePrediction/target/RepurchasePrediction-1.0-SNAPSHOT.jar input output

spark-submit --class "ScalaWordCount" --master spark://jkq181098118-master:7077 target/scala-2.11/find-hottest-items-and-popular-merchants_2.11-1.0.jar hdfs://jkq181098118-master:9000/user/root/input_test/test.txt hdfs://jkq181098118-master:9000/user/root/output