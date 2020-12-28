import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object TrainAndTest {
  def getAvgAge(testDf: DataFrame): Int = {
    val avgAgeStr = testDf.select(avg("age_range")).first()(0).toString
    var avgAge = avgAgeStr.take(1).toInt
    if (avgAgeStr.size > 2 && avgAgeStr.substring(2, 3).toInt >= 5) {
      avgAge += 1
    }
    return avgAge
  }

  def getAvgGender(testDf: DataFrame): Int = {
    val avgGenderStr = testDf.select(avg("gender")).first()(0).toString
    var avgGender:Int = 0
    if(avgGenderStr.size > 2 && avgGenderStr.substring(2, 3).toInt >= 5){
      avgGender += 1
    }
    return avgGender
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
     System.err.println("Usage: <input path> <output path>")
     System.exit(1)
   }

    val conf = new SparkConf().setAppName("TrainAndTest")
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

    //数据读取
    val trainPath = args(0) + "/train.csv"
    val trainDf: DataFrame = sparkSession.read.format("csv").option("applySchema", "true").option("header", "true").option("sep", ",").schema(trainSchema).load(trainPath)

    val rawTrainData = trainDf.rdd.map(line => line.toString).map(line => line.substring(1, line.size - 1))

    //年龄和性别的null值用均值替代
    val avgAgeTrain = getAvgAge(trainDf)
    val avgGenderTrain = getAvgGender(trainDf)

    //数据处理
    val data = rawTrainData.map(_.split(",")).map{words =>
      if(words(3) == "null"){
        words(3) = avgAgeTrain.toString
      }
      if(words(4) == "null"){
        words(4) = avgGenderTrain.toString
      }
      words
    }.map{line =>
      val label = line(2).toDouble
      val features = line.slice(3, line.size).map(_.toDouble)
      LabeledPoint(label, Vectors.dense(features))}.cache

    val Array(trainData, testData): Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.8,0.2))

    //训练模型
    val numIterations = 10 //迭代次数
    val lrModel = LogisticRegressionWithSGD.train(trainData, numIterations)
//    lrModel.clearThreshold()

    //测试
    val result: RDD[(Double, Double)] = testData.map(
      x=> {
        val prediction: Double = lrModel.predict(x.features)
        (x.label, prediction)
      }
    )

//    result.saveAsTextFile("result")

    //模型评价
    val accuracy: Double = result.filter(x => x._1 == x._2).count().toDouble / result.count()
    println("Accuracy: ", accuracy)
    println("Error: ", (1-accuracy))

    val acc = sc.parallelize(Seq("Accuracy: " + accuracy))
    acc.saveAsTextFile(args(1)+"/accuracy")

    sparkSession.stop()
  }
}