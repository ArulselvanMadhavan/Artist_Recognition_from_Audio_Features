package com.cs6240.msd

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import com.cs6240.msd.utils.fileUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import com.cs6240.msd.utils.Constants

object predictFromModel {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","SparkApp");
    val modelPath = args(0)
    val testPath = args(1)
    val resultsFile = args(2)
    val testLibSVM = MLUtils.loadLibSVMFile(sc, testPath)
    val model = LogisticRegressionModel.load(sc,modelPath);
    val testData = testLibSVM.collect().toList
    val testDataGroupedByYear = fileUtils.groupByYear(testData, Constants.YEAR_START)

    var i = -1;
    val accuracyForYearRange = testDataGroupedByYear.map { s =>
      i += 1;
      val testDataForTheYearRange = sc.parallelize(s)
      val predictionAndLabels = testDataForTheYearRange.map(p => (model.predict(p.features), p.label))
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      val totalData = testDataForTheYearRange.count()
      val accuracy = if(totalData != 0.0){
        1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / testDataForTheYearRange.count();
      }else{
        println("No test data found for this year range")
        0.0
      }
      val year = Constants.YEAR_START + (i * Constants.YEAR_RANGE)
      (year, accuracy,totalData)
    }

    var accuracyTotal = 0.0
    val row = new StringBuilder()
    for (i <- 0 until accuracyForYearRange.size) {
      val result = accuracyForYearRange(i)
      row.append(result._1.toInt + ":" + result._2+":"+result._3+"\n")
      accuracyTotal += result._2
    }
    
    val totalAccuracy = (accuracyTotal / accuracyForYearRange.size)
    val totalTestData = (accuracyForYearRange.foldLeft(0.0){(countSoFar,actualValue) =>
      countSoFar+actualValue._3
    })
    
    row.append("Total Accuracy:"+totalAccuracy+"\n")
    row.append("Total Test Data:"+totalTestData)
    
    fileUtils.writeToFile(resultsFile, row.toString)
  }
}