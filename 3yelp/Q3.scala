import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

    val businessFile = sc.textFile("yelp/business/business.csv")
    val reviewFile = sc.textFile("yelp/review/review.csv")
    val userFile = sc.textFile("yelp/user/user.csv")
    val business = businessFile.map(_.split("\\^")).map(line => (line(0), line(1)))
    //business.foreach(println)
    val review = reviewFile.map(_.split("\\^")).map(line => (line(2), (line(1), line(3))))
    //review.foreach(println)

    // fitler with Stanford
    val filterBusiness = business.filter(line => line._2.contains("Stanford"))
    //filterBusiness.foreach(println)
    val result = review.join(filterBusiness).distinct().collect()
    result.foreach(line => println(line._2._1))

