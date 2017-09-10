import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

    val businessFile = sc.textFile("yelp/business/business.csv")
    val reviewFile = sc.textFile("yelp/review/review.csv")
    val userFile = sc.textFile("yelp/user/user.csv")

    // read in data line by line
    val business = businessFile.map(_.split("\\^")).map(line => (line(0), (line(1), line(2)))).distinct()
    //business.foreach(println)
    val review = reviewFile.map(_.split("\\^"))

    // calculate the average ratings for review data
    // sum and count
    val reviewSum = review.map(line => (line(2), line(3).toFloat)).reduceByKey((a,b) => a+b)
    val reviewCount = review.map(line => (line(2), 1)).reduceByKey((a,b) => a+b)
    val reviewJ1 = reviewSum.join(reviewCount)
    val reviewAverage = reviewJ1.map(a => (a._1, a._2._1/a._2._2))

    // join review and business data
    val result = reviewAverage.join(business).distinct()
    // sort and take top 10
    val resultQ1 = result.sortBy(-_._2._1).take(10)
    resultQ1.foreach(line => println(line._1, line._2._2, line._2._1))
