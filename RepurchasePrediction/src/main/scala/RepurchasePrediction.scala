import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType


object RepurchasePrediction {
  def getCountFeature(target: String, logDf: DataFrame, infoLogDf: DataFrame): DataFrame = {
    val browseDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf(target)).count()
    var browseCountDf = browseDf.groupBy(browseDf("user_id"), browseDf("seller_id")).count()
    browseCountDf = browseCountDf.withColumnRenamed("count", "browse_"+target).withColumnRenamed("seller_id", "merchant_id")
    val newInfoLogDf = infoLogDf.join(browseCountDf, infoLogDf("user_id") === browseCountDf("user_id") && infoLogDf("merchant_id") === browseCountDf("merchant_id"), "left").drop(browseCountDf("user_id")).drop(browseCountDf("merchant_id"))
    return newInfoLogDf
  }
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }

//    val conf = new SparkConf().setAppName("FindGenderRatioAndAgeRangeRatio")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    val rawData = sc.textFile("../data_format1/train_format1.csv")
//    val header = rawData.first() //csv的标题行

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
        StructField("label", DoubleType))
    )

    val infoPath = args(0) + "/user_info_format1.csv"
    val logPath = args(0) +"/user_log_format1.csv"
    val trainPath = args(0) +"/train_format1.csv"
    val infoDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(infoSchema).load(infoPath)
    val logDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(logSchema).load(logPath)
    val trainDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(trainSchema).load(trainPath)
//    trainDf.printSchema()

    //特征：用户年龄范围、性别
    var infoLogDf = trainDf.join(infoDf, trainDf("user_id") === infoDf("user_id"), "left").drop(infoDf("user_id"))

    //特征：用户在该商家日志的总条数
    var totalLogCountDf = logDf.groupBy(logDf("user_id"),logDf("seller_id")).count()
    totalLogCountDf = totalLogCountDf.withColumnRenamed("count", "total_logs").withColumnRenamed("seller_id", "merchant_id")
    infoLogDf = infoLogDf.join(totalLogCountDf, infoLogDf("user_id") === totalLogCountDf("user_id") && infoLogDf("merchant_id") === totalLogCountDf("merchant_id"), "left").drop(totalLogCountDf("user_id")).drop(totalLogCountDf("merchant_id"))

    infoLogDf = getCountFeature("item_id", logDf, infoLogDf)
    infoLogDf = getCountFeature("cat_id", logDf, infoLogDf)
    infoLogDf = getCountFeature("time_stamp", logDf, infoLogDf)
//    //特征：用户浏览的该商家商品的数目
//    val browseItemDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("item_id")).count()
//    var browseItemCountDf = browseItemDf.groupBy(browseItemDf("user_id"), browseItemDf("seller_id")).count()
//    browseItemCountDf = browseItemCountDf.withColumnRenamed("count", "browse_items").withColumnRenamed("seller_id", "merchant_id")
//    infoLogDf = infoLogDf.join(browseItemCountDf, infoLogDf("user_id") === browseItemCountDf("user_id") && infoLogDf("merchant_id") === browseItemCountDf("merchant_id"), "left").drop(browseItemCountDf("user_id")).drop(browseItemCountDf("merchant_id"))
//
//    //特征：用户浏览的该商家商品的种类的数目
//    val browseCategoryDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("cat_id")).count()
//    var browseCategoryCountDf = browseCategoryDf.groupBy(browseCategoryDf("user_id"), browseCategoryDf("seller_id")).count()
//    browseCategoryCountDf = browseCategoryCountDf.withColumnRenamed("count", "browse_categories").withColumnRenamed("seller_id", "merchant_id")
//    infoLogDf = infoLogDf.join(browseCategoryCountDf, infoLogDf("user_id") === browseCategoryCountDf("user_id") && infoLogDf("merchant_id") === browseCategoryCountDf("merchant_id"), "left").drop(browseCategoryCountDf("user_id")).drop(browseCategoryCountDf("merchant_id"))
//
//    //特征：用户浏览的天数
//    val browseDayDf = logDf.groupBy(logDf("user_id"),logDf("seller_id"),logDf("time_stamp")).count()
//    var browseDayCountDf = browseDayDf.groupBy(browseDayDf("user_id"), browseDayDf("seller_id")).count()
//    browseDayCountDf = browseDayCountDf.withColumnRenamed("count", "browse_days").withColumnRenamed("seller_id", "merchant_id")
//    infoLogDf = infoLogDf.join(browseDayCountDf, infoLogDf("user_id") === browseDayCountDf("user_id") && infoLogDf("merchant_id") === browseDayCountDf("merchant_id"), "left").drop(browseDayCountDf("user_id")).drop(browseDayCountDf("merchant_id"))


    infoLogDf.show()
    val rawData = infoLogDf.rdd
//    rawData.foreach(println)

//    val assembler: VectorAssembler = new VectorAssembler().setInputCols(Array("sepal_length","sepal_width","petal_length","petal_width")).setOutputCol("features")
//    val assmblerDf: DataFrame = assembler.transform(df)
//    assmblerDf.show(false)
//    //4-2将类别型class转变为数值型
//    val stringIndex: StringIndexer = new StringIndexer().setInputCol("class").setOutputCol("label")
//    val stingIndexModel: StringIndexerModel = stringIndex.fit(assmblerDf)
//    val indexDf: DataFrame = stingIndexModel.transform(assmblerDf)



//    val data = rawData.filter(line => line != header).map(_.split(",")).map{line =>
//      val label = line(line.size - 1).toDouble
//      val features = line.slice(0, line.size - 1).map(_.toDouble)
//      LabeledPoint(label, Vectors.dense(features))}.cache
//
//    //分隔训练集和测试集
//    val Array(trainData,testData): Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.8,0.2))
//    //val numData = trainDf.count
//
//    //训练模型
//    val numIterations = 2 //迭代次数
//    val lrModel = LogisticRegressionWithSGD.train(trainData, numIterations)
//
//    //测试
//    val result: RDD[(Double, Double)] = testData.map(
//      x=> {
//        val prediction: Double = lrModel.predict(x.features)
//        (x.label,prediction)
//      }
//    )
//
//    //模型评价
//    val accuracy: Double = result.filter(x=>x._1==x._2).count().toDouble /result.count()
//    println(accuracy)
//    println("error", (1-accuracy))

//    sc.stop()
  }
}