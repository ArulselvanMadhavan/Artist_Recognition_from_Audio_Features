import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.{Vectors, Matrices, Matrix}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ameyapandilwar on 11/26/15.
  */
object MLDataMapper {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ML Data Mapper").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "timbre_sample"

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_ROW_START, "TRAAAAW128F429D538")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "TRAABYN12903CFD305")

    val hBaseRows = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("--- number of rows :- " + hBaseRows.count())

    var result: String = ""
    hBaseRows.foreach(keyVal => {
      var timbreStr: String = ""

      for (i <- 1 to 90) {
        val value = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes(i.toString())))
        timbreStr += value + " "
      }
      timbreStr = timbreStr.trim()

      val trackId = new String(keyVal._2.getRow())

      val artistId = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ArtistId")))
      val year = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("year")))
      result = trackId + "," + artistId + "," + year + ";" + timbreStr + "\n"
      print(result)

//      Utils.writeToFile("/Users/ameyapandilwar/CS6240/FINAL_PROJECT/MLPrediction/data/mllib/timbre_sample.txt", result)
    })

  }
}
