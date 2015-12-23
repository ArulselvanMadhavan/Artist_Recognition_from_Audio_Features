
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Get, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ameyapandilwar on 11/26/15.
  */
object MLLibNaiveBayes {

  val conf = HBaseConfiguration.create()

  val trngTrcks = new Array[org.apache.spark.mllib.linalg.Vector](3130);
  var trngCount: Int = 0

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MLLib - Naive Bayes").setMaster("local")
    val sc = new SparkContext(sparkConf)

//    val conf = HBaseConfiguration.create()
    val tableName = "timbre_sample"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val trainingFile = sc.textFile("/Users/ameyapandilwar/CS6240/FINAL_PROJECT/MLPrediction/data/mllib/msd_sample_train_data.txt")
    val trainingRDD = trainingFile.flatMap(getSongIds)
    val trainingTracks = trainingRDD.mapPartitions({ songsIterator: Iterator[(Double, String)] =>
      getRecordFromSong(songsIterator, tableName)
    }, false)

//    val testingFile = sc.textFile("/Users/ameyapandilwar/CS6240/FINAL_PROJECT/MLPrediction/data/mllib/msd_sample_test_data.txt")
//    val testingRDD = testingFile.flatMap(getSongIds)
//    val testingTracks = testingRDD.mapPartitions({ songsIterator: Iterator[(Double, String)] =>
//      getRecordFromSong(songsIterator, tableName)
//    }, false)
    println(trainingTracks.map(_.features))

    println("===== training count :- " + trngCount)

//    val pca = new PCA(10).fit(trainingTracks.map(_.features))
//    val projected = trainingTracks.map(p => p.copy(features = pca.transform(p.features)))
//
//    println("===== projected :- " + projected)

//    val model = NaiveBayes.train(trainingTracks, lambda = 1.0, modelType = "multinomial")
//    val predictionAndLabel = testingTracks.map(p => (model.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testingTracks.count()
//    println("===== accuracy :- " + accuracy)
//    model.save(sc, "msdNaiveBayesModel")
//    val msdModel = NaiveBayesModel.load(sc, "msdNaiveBayesModel")
  }

  def getRecordFromSong(songsIterator: Iterator[(Double, String)], tableName: String) = {
    val htable = new HTable(conf, tableName);
    songsIterator.map { label_song_tuple =>
      val getObj = new Get(label_song_tuple._2.getBytes());
      getObj.addFamily("cf".getBytes());
      val rowResult = htable.get(getObj);
      val vector = getVectorFromResult(rowResult)
      LabeledPoint(label_song_tuple._1, vector)
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

}
