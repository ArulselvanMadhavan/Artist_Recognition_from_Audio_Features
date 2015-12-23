package com.cs6240.msd.utils

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.mllib.regression.LabeledPoint

object fileUtils {

  def writeToFile(path: String, content: String): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

  def sortLabeledPointByYear(lbledPoint1: LabeledPoint, lbledPoint2: LabeledPoint) = {
    lbledPoint1.features.toArray(90) > lbledPoint2.features.toArray(90)
  }

  /**
   * 
   */
  def groupByYear(lstOfLabeledPts: List[LabeledPoint], start: Double): List[List[LabeledPoint]] = {
    val end = start + Constants.YEAR_RANGE
    if (start >= Constants.YEAR_END) Nil else {
      val (b, a) = lstOfLabeledPts.partition{ p =>
        val year = p.features.toArray(90)
        val isGreaterThanEqualStart = (year >= start)
        val isLesserThanEnd = (year < end)
        (isGreaterThanEqualStart && isLesserThanEnd)
      }
      println(start+":"+end)
      println(b)
      b::groupByYear(a, start + Constants.YEAR_RANGE)
    }
  }
}