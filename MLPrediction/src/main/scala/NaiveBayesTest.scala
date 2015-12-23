import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable
import scala.io.Source

/**
  * Created by ameyapandilwar on 11/26/15.
  */
object NaiveBayesTest {

  var count: Int = 0
  val trngTrcks = new Array[org.apache.spark.mllib.linalg.Vector](3130);
  var trackTimbreMap: Map[String, String] = Map()

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MLLib - Naive Bayes").setMaster("local")
    val sc = new SparkContext(sparkConf)

//    val conf = HBaseConfiguration.create()
//    val tableName = "timbre_sample"
//    conf.set(TableInputFormat.INPUT_TABLE, tableName)

//    val trainingFile = sc.textFile("data/mllib/msd_sample_train_data.txt")
//    val trainingData = trainingFile.map { line =>
//      val parts = line.split(',')
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//    }

    val timbreSampleSmall = "/Users/ameyapandilwar/CS6240/FINAL_PROJECT/MLPrediction/data/mllib/timbre_sample.txt"
    for (line <- Source.fromFile(timbreSampleSmall).getLines()) {
      if (StringUtils.isNoneBlank(line)) {
        val info = line.split(';')
        val trackId = info(0).split(",")(0)
//        println(trackId)
        trackTimbreMap += (trackId -> info(1))
      }
    }


    val trainingFile = "/Users/ameyapandilwar/CS6240/FINAL_PROJECT/MLPrediction/data/mllib/msd_sample_train_data.txt"
    for (line <- Source.fromFile(trainingFile).getLines()) {
      if (StringUtils.isNoneBlank(line)) {
        val info = line.split(':')
        val artistInfo = info(0).split("_")
        val tracks = info(1).split(",")

        tracks.foreach(track => {
          trngTrcks(count) = Vectors.dense(trackTimbreMap(track).map(_.toDouble).toArray)
          count += 1
        })
//        count += tracks.length
//        println(artistInfo(0))
//        val artistId = info.split(';')
//        println(artistId)
      }
    }

    println(count + " tracks trained successfully!")

    val points = sc.parallelize(trngTrcks.toList)
    val mat: RowMatrix = new RowMatrix(points)


    val numOfClusters = 599
    val numOfIterations = 10
    val model = KMeans.train(points, numOfClusters, numOfIterations)

    println(" MODEL ==> " + model)
//    val testVector = Vectors.dense(arr)
//    val clusterID = model.predict(testVector)
//    print("the song's artist is :- " + clusterID)

//    val pc: Matrix = mat.computePrincipalComponents(10)
//    val projected = mat.multiply(pc).rows
//
//    println("===== projected :- " + projected)

//    trngTrcks.foreach(println)

//    val trainingData = trainingFile.map { line =>
//      val info = line.split(':')(0)
//      val artistId = info.split('_')(1)
//      println(artistId)
//      val tracks = info.split(',')
//      tracks.map { track =>
//        print(track + " | ")
//      }
//    }

//    val testingFile = sc.textFile("data/mllib/msd_sample_test_data.txt")
//    val testingData = trainingFile.map { line =>
//      val parts = line.split(',')
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//    }

//    val model = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
//
//    val predictionAndLabel = testingData.map(p => (model.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testingData.count()

//    model.save(sc, "msdNaiveBayesModel")
//    val msdModel = NaiveBayesModel.load(sc, "msdNaiveBayesModel")
  }

}
