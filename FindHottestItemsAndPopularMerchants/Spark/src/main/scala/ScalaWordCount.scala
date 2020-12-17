import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FindHottestItems {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("FindHottestItems")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    // val line = sc.textFile("../../../input_test/test.txt")
    
    val itemPairs = line.map(_.split(",")(1)).map((_, 1)).reduceByKey(_+_)
    val sortedItemPairs = itemPairs.map(line =>(line._2,line._1)).sortByKey(false).map(line =>(line._2,line._1))
    sortedItemPairs.collect()
  
    sortedItemPairs.saveAsTextFile(args(1))
    // itemPairs.saveAsTextFile("../../../output")

    sc.stop()
  }
}