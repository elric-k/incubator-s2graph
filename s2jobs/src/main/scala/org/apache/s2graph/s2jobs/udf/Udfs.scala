package org.apache.s2graph.s2jobs.udf

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

object Udfs {
  def calculateTable(cnt: Long, dimvalCnt:Long, itemCnt:Long, totalCnt:Long) = {
    val k11 = cnt
    val k12 = dimvalCnt - k11
    val k21 = itemCnt - k11
    val k22 = totalCnt - dimvalCnt - itemCnt + k11

    (k11, k12, k21, k22)
  }
  def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long): Double = {
    def xLogX(x: Long) = if (x == 0) 0.0 else x * Math.log(x)

    def entropy2(a: Long, b: Long): Double = xLogX(a + b) - xLogX(a) - xLogX(b)

    def entropy4(a: Long, b: Long, c: Long, d: Long): Double = xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d)

    val rowEntropy = entropy2(k11 + k12, k21 + k22)
    val columnEntropy = entropy2(k11 + k21, k12 + k22)
    val matrixEntropy = entropy4(k11, k12, k21, k22)

    if (rowEntropy + columnEntropy < matrixEntropy) 0.0
    else 2.0 * (rowEntropy + columnEntropy - matrixEntropy)
  }
  def customLLR(cnt: Long, dimvalCnt:Long, itemCnt:Long, totalCnt:Long):Double = {
    val threshold = 0.0

    val (k11, k12, k21, k22) = calculateTable(cnt, dimvalCnt, itemCnt, totalCnt)
    val positiveRatio = if (k11 + k12 == 0) -1 else k11.toDouble / (k11 + k12)
    val negativeRatio = if (k12 + k22 == 0) -1 else k12.toDouble / (k12 + k22)

    if (positiveRatio / negativeRatio > threshold)
      logLikelihoodRatio(k11, k12, k21, k22)
    else
      0.0
  }

  def addHours(datetime : Timestamp, hours : Int) = new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 )

  /**
    * register udf
    * @param ss
    * @return
    */
  def register(ss: SparkSession) = {
    ss.udf.register("llr", customLLR _)
    ss.udf.register("add_hours", addHours _)
  }
}
