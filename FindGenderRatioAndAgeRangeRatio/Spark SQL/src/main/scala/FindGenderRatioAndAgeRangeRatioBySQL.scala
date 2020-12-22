import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


object FindGenderRatioAndAgeRangeRatioBySQL {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }

    val infoPath = "/user_info_format1.csv"
    val logPath = "/user_log_format1.csv"

    val conf = new SparkConf().setAppName("FindGenderRatioAndAgeRangeRatioBySQL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val infoDf = sqlContext.load("com.databricks.spark.csv", Map("path" -> (args(0) + infoPath), "header"-> "true"))
    val logDf = sqlContext.load("com.databricks.spark.csv", Map("path" -> (args(0) + logPath), "header"-> "true"))
    
    val joinExpression = infoDf.col("user_id") === logDf.col("user_id")
    val infoLogDf = infoDf.join(logDf, joinExpression)
    infoLogDf.show()

    //统计购买了商品的男女比例
    val maleDf = infoLogDf.where("gender = \"1\" and action_type = \"2\"").dropDuplicates("user_id")
    val maleCount = maleDf.count().toFloat
    val femaleDf = infoLogDf.where("gender = \"0\" and action_type = \"2\"").dropDuplicates("user_id")
    val femaleCount = femaleDf.count().toFloat
    val maleRatio = (maleCount / (maleCount + femaleCount)) * 100
    val femaleRatio = (femaleCount / (maleCount + femaleCount)) * 100
    val genderRatio = sc.parallelize(Seq("购买了商品的男性比例："+String.valueOf(maleRatio.formatted("%.2f"))+"%", "购买了商品的女性比例："+String.valueOf(femaleRatio.formatted("%.2f"))+"%"))

    genderRatio.saveAsTextFile(args(1) + "/gender")

    //统计购买了商品的买家年龄段的⽐例
    val validAgeRangeArray = Array(("<18", "1"), ("[18, 24]", "2"), ("[25, 29]", "3"), ("[30, 34]", "4"), ("[35, 39]", "5"), ("[40, 49]", "6"), ("≥50", "7"), ("≥50", "8"))

    var ageCountPairList:List[(String, Long)]=List()
    var ageRatioList:List[String]=List()

    for(ageRange <- validAgeRangeArray){
      val ageDf = infoLogDf.where("age_range = " + ageRange._2 + " and action_type = \"2\"").dropDuplicates("user_id")
      val ageCount = ageDf.count()
      val ageCountPair = (ageRange._1, ageCount)
      ageCountPairList = ageCountPair :: ageCountPairList
    }

    var sum:Long = 0
    for(count <- ageCountPairList){
      sum += count._2
    }

    for(count <- ageCountPairList){
      var ageRangeRatio = "年龄区间为" + count._1 + "的比例: " + String.valueOf(((count._2.toFloat / sum.toFloat).toFloat * 100).formatted("%.2f")) + "%"
      ageRatioList = ageRangeRatio :: ageRatioList
    }

    val ageRangeRatioRdd = sc.parallelize(ageRatioList)
    ageRangeRatioRdd.saveAsTextFile(args(1)+"/age range")

    sc.stop()
  }
}