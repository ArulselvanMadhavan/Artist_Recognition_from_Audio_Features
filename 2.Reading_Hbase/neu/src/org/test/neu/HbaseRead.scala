package org.test.neu;
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * Created by erfangc on 6/7/15.
 */
object HbaseRead {

  def main(args: Array[String]) = {
    val logger = LoggerFactory.getLogger(getClass)
    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
   // val mgr = ConnectionFactory.createConnection(conf)
    conf.set(TableInputFormat.INPUT_TABLE, "timbre")
   // conf.set(TableInputFormat.SCAN_ROW_START, "100")
   // conf.set(TableInputFormat.SCAN_ROW_STOP, "105")
    
    val hbaseRows = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    hbaseRows.foreach(keyVal => {
      println(keyVal);
      val artistID = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ArtistId")))
      println("artistID: " + artistID)
    })

  }


}