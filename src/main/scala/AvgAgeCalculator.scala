/**
  * Created by ZSCDumin on 2018/4/2.
  * 作者邮箱：2712220318@qq.com
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AvgAgeCalculator {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Spark Exercise:Average Age Calculator").setMaster("local")
        val sc = new SparkContext(conf)
        val dataFile = sc.textFile("F:\\BigData\\sample_age_data.txt", 5)
        val count = dataFile.count()
        val ageData = dataFile.map(line => line.split(" ")(1))
        val totalAge = ageData.map(age => Integer.parseInt(
            String.valueOf(age))).collect().sum
        println("Total Age:" + totalAge + ";Number of People:" + count)
        val avgAge: Double = totalAge.toDouble / count.toDouble
        println("Average Age is " + avgAge)

        Thread.sleep(100000)
    }
}
