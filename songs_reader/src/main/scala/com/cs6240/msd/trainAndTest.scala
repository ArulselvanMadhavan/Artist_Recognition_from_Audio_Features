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
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import com.cs6240.msd.utils.fileUtils
import com.cs6240.msd.utils.Constants

object trainAndTest {

  /**
   * Args[0] - train
   * Args[1] - test
   * Args[2] - tableName
   * Args[3] - Number of Classes for the classifier
   * Args[4] - Hbase config file path
   */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Simple App");
    //    val hbaseConfiguration = (hbaseConfigFileName: String, tableName: String) => {
    //      val hbaseConfiguration = HBaseConfiguration.create()
    //      //    	hbaseConfiguration.addResource(hbaseConfigFileName)
    //      hbaseConfiguration.set("hbase.zookeeper.quorum", "localhost");
    //      hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
    //      hbaseConfiguration.set("hbase.master", "localhost:60000");
    //      hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, tableName)
    //      hbaseConfiguration
    //    }

    //        val hBaseRDD = sc.newAPIHadoopRDD(hbaseConfiguration(args(0), args(2)),
    //    										classOf[TableInputFormat],
    //    										classOf[ImmutableBytesWritable],
    //    										classOf[Result]);
    //        val ResultRDD = hBaseRDD.map(tuple => tuple._2)
    //Load the train
    
    val modelPath = args(6)
    val trainData = sc.textFile(args(0));

    //FlatMap of Songs
    /*
     * Input: Line (ArtistId:SongId1,SongId2)
     * Output: "SongId1 SongId2...."
     */
    val label_song_tupleRDD = trainData.flatMap(getSongIds)
    val tableName = args(2)
    val hbaseConfigFilePath = args(4)
    val songsTrain = label_song_tupleRDD.mapPartitions({ songsIterator: Iterator[(Double, String)] =>
      getRecordFromSong(songsIterator, tableName, hbaseConfigFilePath)
    }, false).cache()
    
    println(songsTrain.count())
    val testData = sc.textFile(args(1)).cache();
    val test_label_song_tupleRDD = testData.flatMap(getSongIds)
    val songsTest = test_label_song_tupleRDD.mapPartitions({ songsIterator: Iterator[(Double, String)] =>
      getRecordFromSong(songsIterator, tableName, hbaseConfigFilePath)
    }, false)

    /*
     * Naive Bayes 
     */
    //    val model = NaiveBayes.train(songsTrain, lambda = 1.0, modelType = "multinomial")
    //    val predictionAndLabel = songsTest.map(p => (model.predict(p.features), p.label))
    //    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / songsTest.count()

    /*
     * KNN
     */
    val model = new LogisticRegressionWithLBFGS().setNumClasses(args(3).toInt).run(songsTrain)
//    model.save(sc, modelPath)
    val predictionAndLabels = songsTest.map(p => (model.predict(p.features), p.label))
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    predictionAndLabels.foreach(pred_label => println("Prediction:" + pred_label._1 + "\t" + "Actual:" + pred_label._2))
    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / songsTest.count();
    println("Precision = " + precision)
    println("Accuracy = " + accuracy)
    println("done");
    fileUtils.writeToFile(args(5), accuracy + "\n")
    //Write the test data
//    fileUtils.writeToFile(args(5), artist_Songs_tuples._3)
  }

  def getRecordFromSong(songsIterator: Iterator[(Double, String)], tableName: String, hbaseConfigFilePath: String) = {
    val config = new HBaseConfiguration();
    config.clear()
    val htable = new HTable(config, tableName);
    config.addResource(hbaseConfigFilePath)
    //    config.set("hbase.zookeeper.quorum", "localhost");
    //    config.set("hbase.zookeeper.property.clientPort", "2181");
    //    config.set("hbase.master", "localhost:60000");
    songsIterator.map { label_song_tuple =>
      val getObj = new Get(label_song_tuple._2.getBytes());
      getObj.addFamily(Constants.COLUMN_FAMILY.getBytes());
      val rowResult = htable.get(getObj);
      val vector = getVectorFromResult(rowResult)
      LabeledPoint(label_song_tuple._1, vector)
    }
  }

  def getSongIds(line: String): Array[(Double, String)] = {
    if (StringUtils.isNoneBlank(line)) {
      val label_artistSongs = line.split("_");
      val label = label_artistSongs(0).toDouble
      val artists_songs = line.split(":");
      artists_songs(1).split(",").map(song => (label, song))
    } else {
      Array[(Double, String)]()
    }
  }
  def getVectorFromResult(rowResult: Result): org.apache.spark.mllib.linalg.Vector = {
    val arr = new Array[Double](90);
    for (i <- 1 to 90) {
      val keyValue = rowResult.getColumnLatest("cf".getBytes(), i.toString().getBytes());
      val value = new String(keyValue.getValue());
      arr(i - 1) = value.toDouble;
    }
    return Vectors.dense(arr);
  }
}