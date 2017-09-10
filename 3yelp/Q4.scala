import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

    val businessFile = sc.textFile("yelp/business/business.csv")
    val reviewFile = sc.textFile("yelp/review/review.csv")
    val userFile = sc.textFile("yelp/user/user.csv")

    val user = userFile.map(_.split("\\^")).map(line => (line(0), line(1)))
    val review = reviewFile.map(_.split("\\^")).map(line => (line(1), 1))

    val reviewCount = review.reduceByKey((a,b)=>a+b)
    // reviewCount.foreach(println)
    val result = user.join(reviewCount).sortBy(-_._2._2).take(10)
    result.foreach(line => println(line._1, line._2._1))

