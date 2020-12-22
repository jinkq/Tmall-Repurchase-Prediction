import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._


object FindGenderRatioAndAgeRangeRatio {
  def getBuyUsers(line: String): String = { //获得购买了商品的user
    val words = line.split(",")
    if(words(6) == "2"){ //购买
      return (words(0))
    }
    else{
      return ("")
    }
  }

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

    val conf = new SparkConf().setAppName("FindHottestItems")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    val info = line.filter(x => x.split(",").length==3)//来自info表
    val log = line.filter(x => x.split(",").length==7)//来自log表
    
    //统计购买了商品的男女比例
    val maleUsers = info.map(info => getGenderUsers(info, "male")).map((_, 1)).filter{case (key, value) => key != ""}
    val femaleUsers = info.map(info => getGenderUsers(info, "female")).map((_, 1)).filter{case (key, value) => key != ""}
    
    val buyUsers = log.map(log => getBuyUsers(log)).filter(user => user != "").distinct().map((_, 1))
    val maleBuyUsers = maleUsers.join(buyUsers).keys
    val femaleBuyUsers = femaleUsers.join(buyUsers).keys

    val maleBuyUsersRatio = maleBuyUsers.count()/(maleBuyUsers.count() + femaleBuyUsers.count()).toFloat * 100
    val femaleBuyUsersRatio = femaleBuyUsers.count()/(maleBuyUsers.count() + femaleBuyUsers.count()).toFloat *100

    val genderCount = sc.parallelize(Seq("购买了商品的男性比例："+String.valueOf(maleBuyUsersRatio.formatted("%.2f"))+"%", "购买了商品的女性比例："+String.valueOf(femaleBuyUsersRatio.formatted("%.2f"))+"%"))
    
    genderCount.saveAsTextFile(args(1) + "/gender")

    //统计购买了商品的买家年龄段的⽐例
    // val validAgeRange = Array("1", "2", "3", "4", "5", "6", "7", "8")
    val validAgeRangeArray = Array(("<18", "1"), ("[18, 24]", "2"), ("[25, 29]", "3"), ("[30, 34]", "4"), ("[35, 39]", "5"), ("[40, 49]", "6"), ("≥50", "7"), ("≥50", "8"))
    
    // val ageRangeArray = info.filter(info => (validAgeRange contains info.split(",")(1))).map(info => info.split(",")(1)).distinct().collect()

    var list:List[String]=List()

    for(ageRange <- validAgeRangeArray){
      val ageUsers = info.map(info => getAgeUsers(info, ageRange._2)).map((_, 1)).filter{case (key, value) => key != ""}
      val ageBuyUsers = ageUsers.join(buyUsers).keys
      // val ageBuyUserCountPair = "Age Range = " + String.valueOf(ageRange)+": " + String.valueOf(ageBuyUsers.count())
      val ageBuyUserCountPair = "年龄区间" + String.valueOf(ageRange._1) + ": " + String.valueOf(ageBuyUsers.count())
      // val ageBuyUserCountPairs = ageBuyUsers.map(ageBuyUser => (ageRange, ageBuyUsers.count()))
      // val formattedAgeBuyUserCountPairs = ageBuyUserCountPairs.map(ageBuyUserCountPair => "Age Range = " + String.valueOf(ageBuyUserCountPair._1)+": "+String.valueOf(ageBuyUserCountPair._2))
      // val ageBuyUsersCount = "Age Range = " + String.valueOf(ageRange)+": " + String.valueOf(ageUsers.join(buyUsers).keys.count())
      list = ageBuyUserCountPair :: list
    }

    // for(i <- list){
    //   println(i)
    //   println("\n")
    // }
    
    val list3=sc.parallelize(list.reverse)
    list3.saveAsTextFile(args(1)+"/age range")
    // val youngUsers = info.map(info => getAgeUsers(info)).filter(user => user != "")
    // val actionUserSellerPairs = log.map(log => getActionUserSellerPairs(log)).filter{case (key, value) => key != ""}
    // val youngActionUsers = youngUsers.intersection(actionUserSellerPairs.keys).distinct().map((_,1))//符合actionType的年轻人

    // val sellerPairs = youngActionUsers.join(actionUserSellerPairs).map{case (key, value) => (value._2, 1)}.reduceByKey(_+_)
    // val sortedSellerPairs = sellerPairs.map(sellerPair =>(sellerPair._2,sellerPair._1)).sortByKey(false).map(sellerPair =>(sellerPair._2,sellerPair._1)).take(100)
    // val sortedSellers = sc.parallelize(sortedSellerPairs)
    // val formatedSortedSellers = sortedSellers.map{case (key, value) => ("seller_id="+key, "添加购物⻋+购买+添加收藏夹="+value)}
    
    // formatedSortedSellers.saveAsTextFile(args(1)+"/popular merchants among young")
    
    sc.stop()
  }
}