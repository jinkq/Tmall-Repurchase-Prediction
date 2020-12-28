import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType


object GetFeatures {
  def getFeatures(trainDf: DataFrame, infoDf: DataFrame, logDf: DataFrame, target: String): Unit = {
    //特征：用户年龄范围、性别
    var infoLogDf = trainDf.join(infoDf, trainDf("user_id") === infoDf("user_id"), "left").drop(infoDf("user_id"))
    
    //特征：用户在该商家日志的总条数
    var totalLogCountDf = logDf.groupBy(logDf("user_id"),logDf("seller_id")).count()
    totalLogCountDf = totalLogCountDf.withColumnRenamed("count", "total_logs").withColumnRenamed("seller_id", "merchant_id")
    infoLogDf = infoLogDf.join(totalLogCountDf, infoLogDf("user_id") === totalLogCountDf("user_id") && infoLogDf("merchant_id") === totalLogCountDf("merchant_id"), "left").drop(totalLogCountDf("user_id")).drop(totalLogCountDf("merchant_id"))

    //特征：用户浏览的该商家商品的数目
    val browseItemDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("item_id")).count()
    var browseItemCountDf = browseItemDf.groupBy(browseItemDf("user_id"), browseItemDf("seller_id")).count()
    browseItemCountDf = browseItemCountDf.withColumnRenamed("count", "browse_items").withColumnRenamed("seller_id", "merchant_id")
    infoLogDf = infoLogDf.join(browseItemCountDf, infoLogDf("user_id") === browseItemCountDf("user_id") && infoLogDf("merchant_id") === browseItemCountDf("merchant_id"), "left").drop(browseItemCountDf("user_id")).drop(browseItemCountDf("merchant_id"))

    //特征：用户浏览的该商家商品的种类的数目
    val browseCategoryDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("cat_id")).count()
    var browseCategoryCountDf = browseCategoryDf.groupBy(browseCategoryDf("user_id"), browseCategoryDf("seller_id")).count()
    browseCategoryCountDf = browseCategoryCountDf.withColumnRenamed("count", "browse_categories").withColumnRenamed("seller_id", "merchant_id")
    infoLogDf = infoLogDf.join(browseCategoryCountDf, infoLogDf("user_id") === browseCategoryCountDf("user_id") && infoLogDf("merchant_id") === browseCategoryCountDf("merchant_id"), "left").drop(browseCategoryCountDf("user_id")).drop(browseCategoryCountDf("merchant_id"))

    //特征：用户浏览的天数
    val browseDayDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("time_stamp")).count()
    var browseDayCountDf = browseDayDf.groupBy(browseDayDf("user_id"), browseDayDf("seller_id")).count()
    browseDayCountDf = browseDayCountDf.withColumnRenamed("count", "browse_days").withColumnRenamed("seller_id", "merchant_id")
    infoLogDf = infoLogDf.join(browseDayCountDf, infoLogDf("user_id") === browseDayCountDf("user_id") && infoLogDf("merchant_id") === browseDayCountDf("merchant_id"), "left").drop(browseDayCountDf("user_id")).drop(browseDayCountDf("merchant_id"))

    //特征：用户对该商家单击、添加购物车、购买、添加收藏夹的次数
    var actionDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("action_type")).count()

    for(i <- 0 to 3){
      val action = i match  {
        case 0 => "click"
        case 1 => "shopping_cart"
        case 2 => "buy"
        case 3=> "favourite"
      }
      val addFlagCol = udf((arg: String) => {if (arg == String.valueOf(i)) 1 else 0})
      actionDf = actionDf.withColumn("flag", addFlagCol(actionDf("action_type")))

      val addActionCountCol = udf((flag: Int, count: Long) => {if (flag == 1) count else 0.toLong})
      actionDf = actionDf.withColumn(action, addActionCountCol(actionDf("flag"), actionDf("count")))
      actionDf = actionDf.drop("flag")
    }
    actionDf = actionDf.drop("count").drop("action_type")
    val actionFeatureDf = actionDf.groupBy(actionDf("user_id"),actionDf("seller_id")).sum()

    infoLogDf = infoLogDf.join(actionFeatureDf, infoLogDf("user_id") === actionFeatureDf("user_id") && infoLogDf("merchant_id") === actionFeatureDf("seller_id"), "left").drop(actionFeatureDf("user_id")).drop(actionFeatureDf("seller_id"))

    val rawData = infoLogDf.rdd.map(line => line.toString).map(line => line.substring(1, line.size - 1))
    rawData.saveAsTextFile(target)
  }

  def main(args: Array[String]) {
   if (args.length < 2) {
     System.err.println("Usage: <input path> <output path>")
     System.exit(1)
   }

    val conf = new SparkConf().setAppName("GetFeatures")
    val sc = new SparkContext(conf)

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("RepurchasePrediction").getOrCreate()

    val infoSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("age_range", DoubleType),
        StructField("gender", DoubleType))
    )

    val logSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("item_id", StringType),
        StructField("cat_id", StringType),
        StructField("seller_id", StringType),
        StructField("brand_id", StringType),
        StructField("time_stamp", StringType),
        StructField("action_type", StringType))
    )

    val trainSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("merchant_id", StringType),
        StructField("label", DoubleType)))

    val testSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("merchant_id", StringType)))
    
    //数据读取
    val infoPath = args(0) + "/user_info_format1.csv"
    val logPath = args(0) +"/user_log_format1.csv"
    val trainPath = args(0) +"/train_format1.csv"
    val testPath = args(0) +"/test_format1.csv"
    val infoDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(infoSchema).load(infoPath)
    val logDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(logSchema).load(logPath)
    val trainDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(trainSchema).load(trainPath)
    val testDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(testSchema).load(testPath)

    //特征工程
    getFeatures(trainDf, infoDf, logDf, "train")
    getFeatures(testDf, infoDf, logDf, "test")

    sparkSession.stop()
  }
}