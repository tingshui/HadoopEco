import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

    val businessFile = sc.textFile("yelp/business/business.csv")
    val reviewFile = sc.textFile("yelp/review/review.csv")
    val userFile = sc.textFile("yelp/user/user.csv")
    val business = businessFile.map(_.split("\\^")).map(line => (line(0), line(1)))
    val review = reviewFile.map(_.split("\\^")).map(line => (line(2), 1))

    val reviewCount = review.reduceByKey((a,b)=>a+b)
    val filterBusiness = business.filter(line => line._2.contains("TX"))

    val result = filterBusiness.join(reviewCount).collect()
    result.foreach(line => println(line._1, line._2._2))

