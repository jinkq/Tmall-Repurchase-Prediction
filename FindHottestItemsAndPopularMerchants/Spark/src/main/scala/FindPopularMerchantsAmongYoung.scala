import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object FindPopularMerchantsAmongYoung {
  def action123(line: String): String = {
    val words = line.split(",")
    if(words(6) == "1" || words(6) == "2" || words(6) == "3"){ //添加购物⻋、购买、添加收藏夹
      return words(1)
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

    val conf = new SparkConf().setAppName("FindHottestItems")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    // val line = sc.textFile("../../../input_test/test.txt")
    
    // val itemPairs = line.map(_.split(",")(1)).map((_, 1)).reduceByKey(_+_)
    val itemPairs = line.map(line => action123(line)).map((_, 1)).filter{case (key, value) => key != ""}.reduceByKey(_+_)
    val sortedItemPairs = itemPairs.map(line =>(line._2,line._1)).sortByKey(false).map(line =>(line._2,line._1)).take(100)
    
    val sortedItems = sc.parallelize(sortedItemPairs)//将数组转化为RDD
    val formatedSortedItems = sortedItems.map{case (key, value) => ("item_id="+key+", 添加购物⻋+购买+添加收藏夹="+value, "")}
  
    formatedSortedItems.saveAsTextFile(args(1))
    // itemPairs.saveAsTextFile("../../../output")

    sc.stop()
  }
}