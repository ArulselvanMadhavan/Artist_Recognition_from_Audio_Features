package com.cs6240.msd.utils

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get

object HbaseUtils {

  /**
   * Input: Result
   * Output: Tuple2
   */
  val hbaseConfiguration = (hbaseConfigFileName: String, tableName: String) => {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.addResource(hbaseConfigFileName)
    hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConfiguration
  }

  def getColumnWithRowIdFromResult(result: Result, artistColumnId: String): (String, String) = {
    val keyValue = result.getColumnLatest(Constants.COLUMN_FAMILY.getBytes(), artistColumnId.getBytes);
    val artistId = keyValue.getValue();
    val songId = result.getRow();
    return (new String(artistId), new String(songId))
  }

  /*
   * 
   */
  def getArtistAndYearFromResult(result:Result):(String,String,String)={
    val artistId = getRecentColumn(result,Constants.ARTISTID)
    val year = getRecentColumn(result,Constants.YEAR)
    val songId = new String(result.getRow())
    (artistId,songId,year)
  }
  /*
   * 1. Make a connection to hbaseTable
   * 2. Get all rows for each value of the iterator
   */
  def getAllColumnsFromRow(artistWithSongsIterator: Iterator[(String, Array[(String, String,String)])],
    hbaseConfigFilePath: String,
    tableName: String) = {
    val config = hbaseConfiguration(hbaseConfigFilePath, tableName);
    val htable = new HTable(config, tableName);
    //For each artist get all his songs
    val artistsWithSongs = artistWithSongsIterator.map { artistWithSong: (String, Array[(String, String,String)]) =>
      //Apply map over all his songs
      val artistWithAllSongs = artistWithSong._2.map { art_song_tuple: (String, String,String) =>
        val rowId = art_song_tuple._2;
        val getObj = new Get(rowId.getBytes());
        getObj.addFamily(Constants.COLUMN_FAMILY.getBytes());
        val rowResult = htable.get(getObj);
        val rowAsString = getOneRow(rowResult)
        rowAsString
      }
      (artistWithSong._1, artistWithAllSongs)
    }
    artistsWithSongs
  }

  /**
   * Return All Columns as String
   */
  def getOneRow(rowResult: Result): (String,String) = {
    val row = new StringBuilder();
    for (i <- 1 to 90) {
      row.append(" " + i + ":" + getRecentColumn(rowResult,i.toString))
    }
    val year = getRecentColumn(rowResult,Constants.YEAR.toString)
    //Remove the below statement after validation.
    val cleanedYear = if(year == "") println("Something didn't get filtered properly") else year
    //Add Year as the last column
    row.append(" " + 91 + ":" + year)
    val additionalDetails = new String(rowResult.getRow())+":"+year
    (row.toString,additionalDetails)
  }
  
  def getRecentColumn(rowResult:Result,columnId:String):String ={
      val keyValue = rowResult.getColumnLatest(Constants.COLUMN_FAMILY.getBytes(), columnId.getBytes());
      new String(keyValue.getValue());
  }
  /*
   * Returns true iff the number of songs is greater than <count>
   */
  def filterByCount(res: (String, Array[(String,String,String)]), threshold: Int): Boolean = {
    if (res._2.length > threshold) {
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Used by our old api
   */
    def filterByCountOld(res: (String, Array[(String,String)]), threshold: Int): Boolean = {
    if (res._2.length > threshold) {
      return true;
    } else {
      return false;
    }
  }
}