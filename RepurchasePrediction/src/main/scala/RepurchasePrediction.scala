import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object RepurchasePrediction {
  def getGenderUsers(line: String, gender: String): String = { //获得男users
    val words = line.split(",")
    if(gender == "male" && words(2) == "1"){
      return words(0)
    }
    else if(gender == "female" && words(2) == "0"){
      return words(0)
    }
    else{
      return ""
    }
  }
  
  def getAgeUsers(line: String, ageRange: String): String = { //获得对应ageRange的users
    val words = line.split(",")
    if(ageRange == words(1)){
      return words(0)
    }
    else{
      return ""
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("FindGenderRatioAndAgeRangeRatio")
    val sc = new SparkContext(conf)
    val train = sc.textFile(args(0))

    //val trainDf = train.map(_.split(",")).map { r => val trimmed = r.map(_.replaceAll("\"", "")) val label = trimmed(r.size - 1).toInt val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble) LabeledPoint(label, Vectors.dense(features)) } data.cache

    //val numData = trainDf.count
    
    sc.stop()
  }
}