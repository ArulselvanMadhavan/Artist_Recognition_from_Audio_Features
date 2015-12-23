package com.cs6240.msd

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import com.cs6240.msd.utils.HbaseUtils
import com.cs6240.msd.utils.Constants
import com.cs6240.msd.utils.fileUtils

object GenerateLibSVM {

  /*
   * args(0) -> HbaseConfig File name
   */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Simple App");
    val hbaseConfigFilePath = args(0)
    val tableName = args(1)
    val threshold = args(2).toInt
    val trainExtraFile = args(3)
    val testExtraFile = args(4)
    val trainLibSVMFile = args(5)
    val testLibSVMFile = args(6)
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
    val startTime = System.currentTimeMillis();
    val listOfVals = ResultRDD.map(result =>
      HbaseUtils.getArtistAndYearFromResult(result)).collect()
    val recordsWithValidYear = listOfVals.filter(record => (record._3 != "0"))
    val listOfRecords = recordsWithValidYear.groupBy(f => f._1);
    println("TimeTaken to group:" + (System.currentTimeMillis() - startTime))

    /**
     * 1. Parallelize the data
     * 2. filter the data
     */
    val artist_Song_Seq = listOfRecords.toSeq;
    val distData = sc.parallelize(artist_Song_Seq);
    val artist_Songs_tuples = distData.filter(res => HbaseUtils.filterByCount(res, threshold))
    println("TimeTaken to filter:" + (System.currentTimeMillis() - startTime))

    val artistsWithFullRows = artist_Songs_tuples.mapPartitions({ artistWithSongsIterator: Iterator[(String, Array[(String, String,String)])] =>
      HbaseUtils.getAllColumnsFromRow(artistWithSongsIterator, hbaseConfigFilePath, tableName)
    }, false).collect();

    /*
     * Fold ("","","","","")
     * _1 => Counter
     * _2 => trainExtra
     * _3 => testExtra
     * _4 => trainSVM
     * _5 => testSVM
     */
    val id = 1.0;
    val finalOutput = artistsWithFullRows
      .foldLeft((id, "","" ,"", "")) { (preparedSoFar: (Double, String, String, String,String), newVal: (String, Array[(String,String)])) =>
        {
          val uid = preparedSoFar._1;
          val testSong = newVal._2.last
          val testSong_features = testSong._1
          val testSong_additionDetails =  testSong._2
          val testSong_year = testSong_additionDetails.split(":")
          val testLibSVM = preparedSoFar._5 + uid + " " + testSong_features + "\n"
          val testExtra = preparedSoFar._3 + uid + "_" + newVal._1 + ":"+testSong_additionDetails+"\n";
          
          val trainSongs = newVal._2.dropRight(1)
          val trainSongsWithDetails = trainSongs.foldLeft(("","")) { (prepVal: (String,String), currentVal: (String,String)) =>
            val features = currentVal._1
            val additionalDetails = currentVal._2
            val result = (prepVal._1 + uid.toString + " " + features + "\n",prepVal._2+additionalDetails+",")
            result
          }
          val trainSong_features = trainSongsWithDetails._1;
          val trainSong_additionDetails = trainSongsWithDetails._2.dropRight(1); //Remove the last comma
          val trainLibSVM = preparedSoFar._4+trainSong_features
          val trainExtra = preparedSoFar._2+ uid + "_" + newVal._1 + ":"+trainSong_additionDetails+"\n";
          (uid + 1.0, trainExtra, testExtra,trainLibSVM, testLibSVM)
        }
      }

    //1. With the filtered artist and songs, get a row for each song
    //2. Prepare 2 text files
    //3. LibSVM formatted file.
    //4. UniqueID:ArtistId:Year
    //    val output = artist_Songs_tuples.mapValues(HbaseUtils.getRow)
    fileUtils.writeToFile(trainExtraFile, finalOutput._2.dropRight(1))
    //Write the train data
    fileUtils.writeToFile(testExtraFile, finalOutput._3.dropRight(1))
    //Write the test data
    fileUtils.writeToFile(trainLibSVMFile, finalOutput._4.dropRight(1))
    fileUtils.writeToFile(testLibSVMFile, finalOutput._5.dropRight(1))
  }
}