import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZSCDumin on 2018/1/24.
  * 作者邮箱：2712220318@qq.com
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("F:\\BigData\\test.log")
        val words = lines.flatMap(_.split(" ")).filter(word => word != " ")
        val pairs = words.map(word => (word, 1))
        val wordscount = pairs.reduceByKey(_ + _)
        wordscount.collect.foreach(println)
        Thread.sleep(100000)
        sc.stop()
    }
}
