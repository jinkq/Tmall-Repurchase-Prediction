import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType


object TrainAndPredict {

  def main(args: Array[String]) {
   if (args.length < 2) {
     System.err.println("Usage: <input path> <output path>")
     System.exit(1)
   }

    val conf = new SparkConf().setAppName("RepurchasePrediction")
    val sc = new SparkContext(conf)

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("RepurchasePrediction").getOrCreate()

    val trainSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("merchant_id", StringType),
        StructField("label", DoubleType),
        StructField("age_range", StringType),
        StructField("gender", StringType),
        StructField("total_tags", DoubleType),
        StructField("browse_item_id", DoubleType),
        StructField("browse_cat_id", DoubleType),
        StructField("browse_time_stamp", DoubleType),
        StructField("click", DoubleType),
        StructField("shopping_cart", DoubleType),
        StructField("buy", DoubleType),
        StructField("favourite", DoubleType))
    )

    val testSchema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("merchant_id", StringType),
        StructField("age_range", StringType),
        StructField("gender", StringType),
        StructField("total_tags", DoubleType),
        StructField("browse_item_id", DoubleType),
        StructField("browse_cat_id", DoubleType),
        StructField("browse_time_stamp", DoubleType),
        StructField("click", DoubleType),
        StructField("shopping_cart", DoubleType),
        StructField("buy", DoubleType),
        StructField("favourite", DoubleType))
    )

    //数据读取
    val trainPath = args(0) + "/train.csv"
    val testPath = args(0) + "/test.csv"
    val trainDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(trainSchema).load(trainPath)
    val testDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(testSchema).load(testPath)

    val rawTrainData = trainDf.rdd.map(line => line.toString).map(line => line.substring(1, line.size - 1))
    val rawTestData = testDf.rdd.map(line => line.toString).map(line => line.substring(1, line.size - 1))

    //年龄和性别的null值用均值替代
    val avgAgeStr = testDf.select(avg("age_range")).first()(0).toString
    var avgAge = avgAgeStr.take(1).toInt
    if(avgAgeStr.size > 2 && avgAgeStr.substring(2, 3).toInt >= 5){
      avgAge += 1
    }

    val avgGenderStr = testDf.select(avg("gender")).first()(0).toString
    var avgGender:Int = 0
    if(avgGenderStr.size > 2 && avgGenderStr.substring(2, 3).toInt >= 5){
      avgGender += 1
    }

    //数据处理
    val trainData = rawTrainData.map(_.split(",")).map{words =>
      if(words(3) == "null"){
        words(3) = avgAge.toString
      }
      if(words(4) == "null"){
        words(4) = avgGender.toString
      }
      words
    }.map{line =>
      val label = line(2).toDouble
      val features = line.slice(3, line.size).map(_.toDouble)
      LabeledPoint(label, Vectors.dense(features))}.cache

    var testData = rawTestData.map(_.split(",")).map{words =>
      if(words(2) == "null"){
        words(2) = avgAge.toString
      }
      if(words(3) == "null"){
        words(3) = avgGender.toString
      }
      words
    }.map{line =>
      val label = -1.toDouble
      val features = line.slice(2, line.size).map(_.toDouble)
      val user_id = line(0)
      val merchant_id = line(1)
      (user_id, merchant_id, Vectors.dense(features))}.cache

    //训练模型.clearThreshold()
    val numIterations = 100 //迭代次数
    val lrModel = LogisticRegressionWithSGD.train(trainData, numIterations)
    lrModel.clearThreshold()

    //测试
    val result: RDD[String] = testData.map(
      x=> {
        val prediction: Double = lrModel.predict(x._3)
        x._1 + "," + x._2 + "," + prediction.formatted("%.16f")
      }
    )

    result.saveAsTextFile(args(1))

    sparkSession.stop()
  }
}