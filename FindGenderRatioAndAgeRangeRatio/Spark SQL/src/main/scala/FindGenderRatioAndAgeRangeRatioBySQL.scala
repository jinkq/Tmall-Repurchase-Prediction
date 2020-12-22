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

    val conf = new SparkConf().setAppName("FindGenderRatioAndAgeRangeRatioBySQL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val infoDf = sqlContext.load("com.databricks.spark.csv", Map("path" -> (args(0) + "/user_info_format1.csv"), "header"-> "true"))
    val logDf = sqlContext.load("com.databricks.spark.csv", Map("path" -> (args(0) + "/test.csv"), "header"-> "true"))
    
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


    sc.stop()
  }
}