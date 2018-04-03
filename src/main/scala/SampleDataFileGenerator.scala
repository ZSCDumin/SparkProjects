import java.io.FileWriter
import java.io.File
import scala.util.Random

/**
  * Created by ZSCDumin on 2018/4/2.
  * 作者邮箱：2712220318@qq.com
  */
object SampleDataFileGenerator {
    def main(args: Array[String]) {
        val writer = new FileWriter(new File("F:\\BigData\\sample_age_data.txt"), false)
        val rand = new Random()
        for (i <- 1 to 10000000) {
            writer.write(i + " " + rand.nextInt(100))
            writer.write(System.getProperty("line.separator"))
        }
        writer.flush()
        writer.close()
    }
}
