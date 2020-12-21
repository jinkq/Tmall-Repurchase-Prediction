import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._


object FindHottestItemsAndPopularMerchants {
  def getActionItems(line: String): String = { //获得action_type符合要求的items
    val words = line.split(",")
    if(words(6) == "1" || words(6) == "2" || words(6) == "3"){ //添加购物⻋、购买、添加收藏夹
      return words(1)
    }
    else{
      return ""
    }
  }

  def getActionUserSellerPairs(line: String): (String, String) = { //获得action_type符合要求的(user, seller)
    val words = line.split(",")
    if(words(6) == "1" || words(6) == "2" || words(6) == "3"){ //添加购物⻋、购买、添加收藏夹
      return (words(0), words(3))
    }
    else{
      return ("", "")
    }
  }

  def getAgeUsers(line: String): String = { //获得age<30的users
    val words = line.split(",")
    if(words(1) == "1" || words(1) == "2" || words(1) == "3"){ //年龄小于30
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

    val conf = new SparkConf().setAppName("FindHottestItems")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    val info = line.filter(x => x.split(",").length==3)//来自info表
    val log = line.filter(x => x.split(",").length==7)//来自log表
    
    //统计最热门商品
    val itemPairs = log.map(log => getActionItems(log)).map((_, 1)).filter{case (key, value) => key != ""}.reduceByKey(_+_)
    val sortedItemPairs = itemPairs.map(itemPair =>(itemPair._2,itemPair._1)).sortByKey(false).map(itemPair =>(itemPair._2,itemPair._1)).take(100)
    
    val sortedItems = sc.parallelize(sortedItemPairs)//将数组转化为RDD
    val formatedSortedItems = sortedItems.map{case (key, value) => ("item_id="+key, "添加购物⻋+购买+添加收藏夹="+value)}
  
    formatedSortedItems.saveAsTextFile(args(1)+"/hottest items")

    //统计最受年轻人(age<30)关注的商家
    val youngUsers = info.map(info => getAgeUsers(info)).filter(user => user != "")
    val actionUserSellerPairs = log.map(log => getActionUserSellerPairs(log)).filter{case (key, value) => key != ""}
    val youngActionUsers = youngUsers.intersection(actionUserSellerPairs.keys).distinct().map((_,"hh"))//符合actionType的年轻人

    val sellerPairs = youngActionUsers.join(actionUserSellerPairs).map{case (key, value) => (value._2, 1)}.reduceByKey(_+_)
    val sortedSellerPairs = sellerPairs.map(sellerPair =>(sellerPair._2,sellerPair._1)).sortByKey(false).map(sellerPair =>(sellerPair._2,sellerPair._1)).take(100)
    val sortedSellers = sc.parallelize(sortedSellerPairs)
    val formatedSortedSellers = sortedSellers.map{case (key, value) => ("seller_id="+key, "添加购物⻋+购买+添加收藏夹="+value)}
    
    formatedSortedSellers.saveAsTextFile(args(1)+"/popular merchants among young")
    
    sc.stop()
  }
}