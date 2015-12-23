package org.test.neu

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes


object HBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit

    val conf = HBaseConfiguration.create()

    if (args.length < 1) {
      System.err.println("Usage: HBaseTest <table_name>")
      System.exit(1)
    }

    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(args(0))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      admin.createTable(tableDesc)
    }

    val hbaseRows = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

      println("Count: " +   hbaseRows.count());
      
//     hbaseRows.foreach(keyVal => { 
//      val tmbr1 = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("default"), Bytes.toBytes("cf.1")))
//       
//     }) 
     
      import scala.collection.JavaConverters._
      
     val data = hbaseRows
        .map(tuple => tuple._2)
        .map(result => result.getColumn("cf".getBytes(), "3".getBytes()))
        .map(keyValues => {
        keyValues.asScala.reduceLeft {
          (a, b) => if (a.getTimestamp > b.getTimestamp) a else b
        }.getValue})
    
    data.foreach(println)
        
     
     sc.stop()
    admin.close()
  }
}
// scalastyle:on println