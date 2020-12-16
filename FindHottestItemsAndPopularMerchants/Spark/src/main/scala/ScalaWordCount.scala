import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ScalaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Scala_WordCount_Demo")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    
    // val word = line.map(_.split(" "))
    val counts = line.flatMap(_.split(",")(0)).map((_, 1)).reduceByKey(_+_)
    // val counts = line.map((_, 1)).reduceByKey(_+_)
    counts.collect()
  
    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}