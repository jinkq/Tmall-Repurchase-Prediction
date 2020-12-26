import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object FindGenderRatioAndAgeRangeRatio {
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
    val line = sc.textFile(args(0))

    val info = line.filter(x => x.split(",").length==3)//来自info表
    val log = line.filter(x => x.split(",").length==7).filter(x => (x.split(",")(5) == "1111")&&(x.split(",")(6)=="2"))//来自log表
    
    //统计购买了商品的男女比例
    val maleUsers = info.map(info => getGenderUsers(info, "male")).map((_, 1)).filter{case (key, value) => key != ""}
    val femaleUsers = info.map(info => getGenderUsers(info, "female")).map((_, 1)).filter{case (key, value) => key != ""}
    
    val buyUsers = log.map(log => log.split(",")(0)).filter(user => user != "").distinct().map((_, 1))
    val maleBuyUsers = maleUsers.join(buyUsers).keys
    val femaleBuyUsers = femaleUsers.join(buyUsers).keys

    val maleBuyUsersRatio = maleBuyUsers.count()/(maleBuyUsers.count() + femaleBuyUsers.count()).toFloat * 100
    val femaleBuyUsersRatio = femaleBuyUsers.count()/(maleBuyUsers.count() + femaleBuyUsers.count()).toFloat *100

    val maleStr = "购买了商品的男性人数：" + String.valueOf(maleBuyUsers.count()) + "，比例："+String.valueOf(maleBuyUsersRatio.formatted("%.2f"))+"%"
    val femaleStr = "购买了商品的女性人数：" + String.valueOf(femaleBuyUsers.count()) + "，比例："+String.valueOf(femaleBuyUsersRatio.formatted("%.2f"))+"%"
    val genderRatio = sc.parallelize(Seq(maleStr, femaleStr))
    
    genderRatio.saveAsTextFile(args(1) + "/gender")

    //统计购买了商品的买家年龄段的⽐例
    val validAgeRangeArray = Array(("<18", "1"), ("[18, 24]", "2"), ("[25, 29]", "3"), ("[30, 34]", "4"), ("[35, 39]", "5"), ("[40, 49]", "6"), ("≥50", "7"), ("≥50", "8"))
    
    var ageRangeCountPairList:List[(String, Long)]=List()
    var ageRangeRatioList:List[String]=List()

    for(ageRange <- validAgeRangeArray){
      val ageUsers = info.map(info => getAgeUsers(info, ageRange._2)).map((_, 1)).filter{case (key, value) => key != ""}
      val ageBuyUsers = ageUsers.join(buyUsers).keys
      val ageRangeCountPair = (ageRange._1, ageBuyUsers.count())
      ageRangeCountPairList = ageRangeCountPair :: ageRangeCountPairList
    }

    val moreThan50List = ageRangeCountPairList.take(2)

    var moreThan50:Long = 0
    for(item <- moreThan50List){
      moreThan50 += item._2
    }

    val newAgeRangeCountPairList = ("≥50", moreThan50) :: ageRangeCountPairList.takeRight(6)

    var sum:Long = 0//总人数
    for(count <- newAgeRangeCountPairList){
      sum += count._2
    }

    for(count <- newAgeRangeCountPairList){
      val ageRangeRatio = "年龄区间为" + count._1 + "的人数："+ String.valueOf(count._2) + "，比例：" + String.valueOf(((count._2.toFloat / sum.toFloat).toFloat * 100).formatted("%.2f")) + "%"
      ageRangeRatioList = ageRangeRatio :: ageRangeRatioList
    }
    
    val ageRangeRatioRdd = sc.parallelize(ageRangeRatioList)
    ageRangeRatioRdd.saveAsTextFile(args(1)+"/age range")
    
    sc.stop()
  }
}