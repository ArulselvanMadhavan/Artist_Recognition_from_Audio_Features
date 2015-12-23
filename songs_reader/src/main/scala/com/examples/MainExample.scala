package com.examples
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object MainExample {

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())
    
        val sparkContext = new SparkContext("local", "Simple App");
    val hbaseConfiguration = (hbaseConfigFileName: String, tableName: String) => {
    	val hbaseConfiguration = HBaseConfiguration.create()
    	hbaseConfiguration.addResource(hbaseConfigFileName)
    	hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, tableName)
    	hbaseConfiguration
    }
    
    val hBaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration(arg(0), arg(1)),
    										classOf[TableInputFormat],
    										classOf[ImmutableBytesWritable],
    										classOf[Result])
//    rdd.map(tuple => tuple._2)
//    	.map(result => result.getColumn("columnFamily".getBytes(), "columnQualifier".getBytes()))
//    	.map(keyValues => {keyValues{(a, b) => if (a.getTimestamp > b.getTimestamp) a else b}.getValue})
    hBaseRDD.map(tuple => tuple._2).map(result => (result.getRow, result.getColumn("cf".getBytes(), "ArtistId".getBytes()))).map(row => {
    	(row._1.map(_.toChar).mkString, row._2.asScala.reduceLeft {(a, b) => if (a.getTimestamp > b.getTimestamp) a else b}.getValue)
    	}).take(10)
    

//    if (arg.length < 2) {
//      logger.error("=> wrong parameters number")
//      System.err.println("Usage: MainExample <path-to-files> <output-path>")
//      System.exit(1)
//    }
//
//    val jobName = "MainExample"
//
//    val conf = new SparkConf().setAppName(jobName)
//    val sc = new SparkContext(conf)
//
//    val pathToFiles = arg(0)
//    val outputPath = arg(1)
//
//    logger.info("=> jobName \"" + jobName + "\"")
//    logger.info("=> pathToFiles \"" + pathToFiles + "\"")
//
//    val files = sc.textFile(pathToFiles)
//
//    // do your work here
//    val rowsWithoutSpaces = files.map(_.replaceAll(" ", ","))
//
//    // and save the result
//    rowsWithoutSpaces.saveAsTextFile(outputPath)

  }
}
