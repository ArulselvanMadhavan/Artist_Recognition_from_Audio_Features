import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by ameyapandilwar on 11/20/15.
  */
object MLTest {

  def main(args: Array[String]) {
    val testFile = "/Users/ameyapandilwar/CS6240/data_info/FlightDataHeader.txt"
    val conf = new SparkConf().setAppName("ML Test").setMaster("local")
    val sc = new SparkContext(conf)
    val testData = sc.textFile(testFile, 2).cache()
    val numAs = testData.filter(line => line.contains("a")).count()
    val numBs = testData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }

}