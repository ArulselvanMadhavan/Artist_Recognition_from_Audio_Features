import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Vectors, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ameyapandilwar on 11/20/15.
  */
object MLDataLoader {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ML Data Loader").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "timbre"

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_ROW_START, "TRAAAAW128F429D538") // start range value
    conf.set(TableInputFormat.SCAN_ROW_STOP, "TRAABYN12903CFD305") // stop range value

    val hBaseRows = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]) // hBaseRows: org.apache.spark.rdd.NewHadoopRDD

//    val first = hBaseRows.map(_.toString()).first()
//    println(first)
    println("--- number of rows :- " + hBaseRows.count())

    val arr = new Array[Double](90)
    hBaseRows.foreach(keyVal => {

      for (i <- 1 to 90) {
        val value = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes(i.toString())))
        arr(i-1) = value.toDouble
      }

      val artistID = Bytes.toString(keyVal._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ArtistId")))
      print("Artist's ID :- " + artistID + " | ")

    })

    val m: Matrix = Matrices.dense(1, 90, arr)
    println("timbre matrix :- " + m)

    val v = Vectors.dense(arr) // v: org.apache.spark.mllib.linalg.DenseVector

    // Exception in thread "main" java.lang.RuntimeException: RowMatrix.computeCovariance called on matrix with only 1 rows.
    // Cannot compute the covariance of a RowMatrix with <= 1 row.
    val points = sc.parallelize(List(v, v)) // TODO - temporary workaround
    val mat: RowMatrix = new RowMatrix(points)
    val pc: Matrix = mat.computePrincipalComponents(10)
    val projected = mat.multiply(pc).rows // org.apache.spark.rdd.MapPartitionsRDD

    println("===== projected :- " + projected)

    val numOfClusters = 1 // probably should be the number of artists in the training data?
    val numOfIterations = 10 // maximum number of iterations
    val model = KMeans.train(projected, numOfClusters, numOfIterations)

    val testVector = Vectors.dense(arr)
    val clusterID = model.predict(testVector)
    print("the song's artist is :- " + clusterID)


//    TODO - approach #1
//    val mat: RowMatrix =
//    val pc: Matrix = mat.computePrincipalComponents(10) // computing top 10 principal components from dense matrix
//    val projected: RowMatrix = mat.multiply(pc) // projecting rows to linear space spanned by top 10 principal components

//    val model = KMeans.train(projected, 10) // train the model based on the projected data

//    TODO - approach #2
//    val data: RDD[LabeledPoint] = ... // RDD from hbaseRows
//    val pca = new PCA(10).fit(data.map(_.features)) // computing the top 10 principal components
//    val projected = data.map(p => p.copy(features = pca.transform(p.features))) // projecting vectors to linear space, keeping the label

    sc.stop()
  }

}
