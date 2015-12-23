package com.cs6240.msd

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io._
import com.cs6240.msd.utils.fileUtils
import com.cs6240.msd.utils.HbaseUtils
import com.cs6240.msd.utils.Constants

object test {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "Simple App");
    val hbaseConfigFilePath = args(0)
    val tableName = args(3)
    val hBaseRDD = sc.newAPIHadoopRDD(HbaseUtils.hbaseConfiguration(hbaseConfigFilePath, tableName),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val ResultRDD = hBaseRDD.map(tuple => tuple._2)
    println(ResultRDD.getClass())

    /**
     * Input: Result
     * Output: ListOfTuples that correspond to an artist
     * Example(ArtistId,[(ArtistId,SongId),(ArtistId,SongId),(ArtistId,SongId)])
     */
//    val ARTISTID="ArtistId"
    val listOfVals = ResultRDD.map(result => HbaseUtils.getColumnWithRowIdFromResult(result,Constants.ARTISTID)).collect().groupBy(f => f._1)

    /**
     * Input:
     */
    val id = 0.0;
    
    /*
     * Output: (Double,String,String)
     * _1 -> Id Counter as Double
     * _2 -> training Songs as String
     * _3 -> testSongs as String
     */
    val threshold = args(4).toInt
    val artist_Songs_tuples = listOfVals.filter(res => HbaseUtils.filterByCountOld(res,threshold))
      .foldLeft((id, "", "")) { (preparedTuple3,newVal) => trainAndTest(id, preparedTuple3, collectSongs(newVal)) }
    println("Filter done")
    //Write the train data
    fileUtils.writeToFile(args(1), artist_Songs_tuples._2)
    //Write the test data
    fileUtils.writeToFile(args(2), artist_Songs_tuples._3)
  }

  /**
   * Input: prepareString(String,String)
   * 		_1 ID
   * 		_2 contains the string prepared for training
   *   		_3 contains the string prepared for testing.
   *
   * Output:
   * Increment the Id and pass the updated ID, so that fold can keep track of the id counter
   */
  def trainAndTest(id:Double,preparedString: (Double, String, String), newString: (String, String)): (Double, String, String) = {
    val newId = preparedString._1 + 1.0;
    
    return (newId,
      preparedString._2 + "\n" + newId + "_" + newString._1,
      preparedString._3 + "\n" + newId + "_" + newString._2);
  }


  /**
   * Output: For each artist, return train and test songs.
   */
  def collectSongs(output: (String, Array[(String, String)])): (String, String) = {
    val songs = output._2.reduceLeft(concatenateSongs);
    val allSongs = songs._2.split(',')
    val testSong = allSongs.last
    val trainSongs = allSongs.dropRight(1)
    return (songs._1 + ":" + trainSongs.mkString(","), songs._1 + ":" + testSong);
  }

  /**
   * (ArtistId,SongId),(ArtistId,SongId) => (ArtistId,"SongId,SongId")
   */
  def concatenateSongs(x: (String, String), y: (String, String)): (String, String) = {
    return (x._1, x._2 + "," + y._2);
  }
}