import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
object test {
  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val sc = new SparkContext("local[*]" , "SparkDemo")
    val lines = sc.textFile("src/main/resources/input/bev_brancha.txt")
    val words = lines.flatMap(line => line.split(' '))
    val wordsKVRdd = words.map(x => (x,1))
    val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
    count.foreach(println)


    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    val df =spark.read.json("src/main/resources/input/test.txt")
    df.show()



    val lines1 = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines1
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)

    counts.foreach(println)
  }
}