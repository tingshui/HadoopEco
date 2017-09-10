import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

    val businessFile = sc.textFile("yelp/business/business.csv")
    val reviewFile = sc.textFile("yelp/review/review.csv")
    val userFile = sc.textFile("yelp/user/user.csv")    

val inputName = sc.getConf.get("spark.driver.args")

    // read in data line by line
    val user = userFile.map(_.split("\\^")).map(line => (line(0), line(1)))
    val review = reviewFile.map(_.split("\\^"))

    // calculate the average ratings for review data
    // sum and count
    val reviewSum = review.map(line => (line(1), line(3).toFloat)).reduceByKey((a,b) => a+b)
    val reviewCount = review.map(line => (line(1), 1)).reduceByKey((a,b) => a+b)
    val reviewJ1 = reviewSum.join(reviewCount)
    val reviewAverage = reviewJ1.map(a => (a._1, a._2._1/a._2._2))
    //reviewAverage.foreach(println)

    val selectUser = user.filter(line => line._2.equalsIgnoreCase(inputName))
    //selectUser.foreach(println)

    val result = selectUser.join(reviewAverage).collect()
    result.foreach(println)

