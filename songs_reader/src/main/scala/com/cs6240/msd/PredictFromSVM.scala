package com.cs6240.msd

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import com.cs6240.msd.utils.fileUtils
import com.cs6240.msd.utils.Constants
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import java.util.UUID

object PredictFromSVM {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Simple App");
    
    val trainLibSVM = MLUtils.loadLibSVMFile(sc, args(0)).cache()
    val testLibSVM = MLUtils.loadLibSVMFile(sc, args(1)).cache()
    val numOfClasses = args(2)
    val modelsDirPath = args(3)+numOfClasses+"_"+UUID.randomUUID()
    val model = new LogisticRegressionWithLBFGS().setNumClasses(numOfClasses.toInt).run(trainLibSVM)
    model.save(sc, modelsDirPath)
  }
}