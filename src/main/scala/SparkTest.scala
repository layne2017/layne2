import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile("/Users/mac4/IdeaProjects/layne2/input/1.txt")

    val wordRDD = lineRDD.flatMap(_.split(" "))

    val word2oneRDD = wordRDD.map((_, 1))

    val resultRDD = word2oneRDD.reduceByKey(_ + _)

    resultRDD.collect().foreach(println)

    sc.stop();
  }

}
