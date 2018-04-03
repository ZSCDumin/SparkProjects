import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZSCDumin on 2018/4/2.
  * 作者邮箱：2712220318@qq.com
  */
object PeopleInfoCalculator {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("Spark Exercise:People Info(Gender & Height) Calculator").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val dataFile = sc.textFile("F:\\BigData\\sample_people_info.txt", 5)
        val maleData = dataFile.filter(line => line.contains("M")).map(
            line => line.split(" ")(1) + " " + line.split(" ")(2))
        val femaleData = dataFile.filter(line => line.contains("F")).map(
            line => line.split(" ")(1) + " " + line.split(" ")(2))

        val maleHeightData = maleData.map(line => line.split(" ")(1).toInt)
        val femaleHeightData = femaleData.map(line => line.split(" ")(1).toInt)

        val lowestMale = maleHeightData.sortBy(x => x, ascending = true).first()
        val lowestFemale = femaleHeightData.sortBy(x => x, ascending = true).first()

        val highestMale = maleHeightData.sortBy(x => x, ascending = false).first()
        val highestFemale = femaleHeightData.sortBy(x => x, ascending = false).first()
        println("Number of Male Peole:" + maleData.count())
        println("Number of Female Peole:" + femaleData.count())
        println("Lowest Male:" + lowestMale)
        println("Lowest Female:" + lowestFemale)
        println("Highest Male:" + highestMale)
        println("Highest Female:" + highestFemale)
    }
}
