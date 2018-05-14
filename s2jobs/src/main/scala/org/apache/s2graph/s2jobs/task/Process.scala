/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.s2jobs.task

import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json

/**
  * Process
  * @param conf
  */
abstract class Process(override val conf:TaskConf) extends Task {
  def execute(ss:SparkSession, inputMap:Map[String, DataFrame]):DataFrame
}

/**
  * SqlProcess
  * @param conf
  */
class SqlProcess(conf:TaskConf) extends Process(conf) {
  override def mandatoryOptions: Set[String] = Set("sql")

  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    // create temp table
    inputMap.foreach { case (name, df) =>
      logger.debug(s"${LOG_PREFIX} create temp table : $name")
      df.printSchema()
      df.createOrReplaceTempView(name)
    }

    val sql = conf.options("sql")
    logger.debug(s"${LOG_PREFIX} sql : $sql")

    extraOperations(ss.sql(sql))
  }

  /**
    * extraOperations
    * @param df
    * @return
    */
  private def extraOperations(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    var resultDF = df

    // watermark
    val timeColumn = conf.options.get(s"time.column")
    logger.debug(s">> timeColumn:  ${timeColumn}")

    val waterMarkDelayTime = conf.options.get(s"watermark.delay.time")
    if (waterMarkDelayTime.isDefined){
      logger.debug(s">> waterMarkDelayTime : ${waterMarkDelayTime}")
      if (timeColumn.isDefined) {
        resultDF = resultDF.withWatermark(timeColumn.get, waterMarkDelayTime.get)
      } else logger.warn("time.column does not exists.. cannot apply watermark")
    }

    // drop duplication
    val dropDuplicateColumns = conf.options.get("drop.duplicate.columns")
    if (dropDuplicateColumns.isDefined) {
      logger.debug(s">> dropDuplicates : ${dropDuplicateColumns}")
      resultDF = resultDF.dropDuplicates(dropDuplicateColumns.get.split(","))
    }

    // groupBy
    val groupedKeysOpt = conf.options.get(s"grouped.keys")
    if (groupedKeysOpt.isDefined) {
      var groupedKeys = groupedKeysOpt.get.split(",").map{ key =>
        col(key.trim)
      }.toSeq

      val windowDurationOpt = conf.options.get(s"grouped.window.duration")
      val slideDurationOpt = conf.options.get(s"grouped.slide.duration")
      if (windowDurationOpt.isDefined && slideDurationOpt.isDefined){
        logger.debug(s">> using window operation : Duration ${windowDurationOpt}, slideDuration : ${slideDurationOpt}")
        groupedKeys = groupedKeys ++ Seq(window(col(timeColumn.get), windowDurationOpt.get, slideDurationOpt.get))
      }
      logger.debug(s">> groupedKeys: ${groupedKeys}")

      // aggregate options
      val aggExprs = Json.parse(conf.options.getOrElse(s"grouped.dataset.agg", "[\"count(1)\"]")).as[Seq[String]].map(expr(_))
      logger.debug(s">> aggr : ${aggExprs}")

      val groupedDF = resultDF.groupBy(groupedKeys: _*)

      resultDF = if (aggExprs.size > 1) {
        groupedDF.agg(aggExprs.head, aggExprs.tail: _*)
      } else {
        groupedDF.agg(aggExprs.head)
      }
    }

    resultDF
  }

}

class GroupedProcess(conf:TaskConf) extends Process(conf) {
  private val PREFIX = "grouped"
  private val DEFAULT_GROUPED_AGG = """{"*":"count"}"""

  override def mandatoryOptions: Set[String] = Set("sql", s"grouped.keys", s"grouped.time.column")

  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    import org.apache.spark.sql.functions._

    // create temp table
    inputMap.foreach { case (name, df) =>
      logger.debug(s"${LOG_PREFIX} create temp table : $name")
      df.printSchema()
      df.createOrReplaceTempView(name)
    }

    val sql = conf.options("sql")
    logger.debug(s"${LOG_PREFIX} sql : $sql")


    val timeColumn = conf.options(s"${PREFIX}.time.column")
    val waterMarkDelayTime = conf.options.getOrElse(s"${PREFIX}.watermark.delay.time", "10 minutes")
    val windowDurationOpt = conf.options.get(s"${PREFIX}.window.duration")
    val slideDurationOpt = conf.options.get(s"${PREFIX}.slide.duration")

    logger.debug(s">> timeColumn:  ${timeColumn}")
    logger.debug(s">> waterMarkDelayTime : ${waterMarkDelayTime}")

    var groupedKeys = conf.options(s"${PREFIX}.keys").split(",").map { key =>
      col(key.trim)
    }.toSeq

    if (windowDurationOpt.isDefined && slideDurationOpt.isDefined) {
      logger.debug(s">> using window operation : Duration ${windowDurationOpt}, slideDuration : ${slideDurationOpt}")
      groupedKeys = groupedKeys ++ Seq(window(col(timeColumn), windowDurationOpt.get, slideDurationOpt.get))
    }
    logger.debug(s">> groupedKeys: ${groupedKeys}")

    // aggregate options
    val aggExprs = Json.parse(conf.options.getOrElse(s"${PREFIX}.dataset.agg", "[\"count(1)\"]")).as[Seq[String]].map(expr(_))

    //    logger.debug(s">> aggMap: ${aggMap}")
    logger.debug(s">> aggr : ${aggExprs}")

    val groupedDF = ss.sql(sql).withWatermark(timeColumn, waterMarkDelayTime).groupBy(groupedKeys: _*)

    if (aggExprs.size > 1) {
      groupedDF.agg(aggExprs.head, aggExprs.tail: _*)
    } else {
      groupedDF.agg(aggExprs.head)
    }
  }
}

case class ItemStat(itemId:String, dimension:String, value:String, cnt:Long, itemDimCnt:Long, itemTotalCnt:Long)
class TopN(conf:TaskConf) extends Process(conf) {
  private val DELIMITER = "|||"
  private val ALL_VALUE = ""

  override def mandatoryOptions: Set[String] = Set("dimensions", "item")


  override def execute(ss: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {
    val dimensions = conf.options("dimensions").split(",").sorted
    val itemCol = conf.options("item")
    val countCol = conf.options.getOrElse("count", "cnt")

    val columns = Seq(itemCol) ++ dimensions ++ Seq(countCol)
    val limit = conf.options.getOrElse("limit", "100").toInt

    val topKFunc = conf.options.getOrElse("topkFunc", "countSum")

    // create temp table
    inputMap.foreach { case (name, df) =>
      logger.debug(s"${LOG_PREFIX} create temp table : $name")
      df.printSchema()
      df.createOrReplaceTempView(name)
    }

    val sql = conf.options("sql")
    logger.debug(s"${LOG_PREFIX} sql : $sql")
    val df = ss.sql(sql)

    /**
      * trasnform : item_id -> dim_val, cnt
      */
    import org.apache.spark.sql.functions._
    import ss.implicits._

    val itemStat = df.select(columns.head, columns.tail: _*).rdd.map{ row =>
      val itemId = row.getAs[String](itemCol)
      val count = row.getAs[Long](countCol)

      val dim = dimensions.mkString(DELIMITER)
      val value = dimensions.map{ dim => row.getAs[String](dim) }.mkString(DELIMITER)

      val dimVals = Seq(
        (dim, value),
        (dim, ALL_VALUE),
        (ALL_VALUE, ALL_VALUE)
      )

      itemId -> (dimVals, count)
    }.groupByKey().flatMap{ case (itemId, iters) =>
      val dimValRaws = iters.toSeq.flatMap{ case (dimVals, count) =>
        dimVals.map{ case (dim, value)  => (dim, value, count)}
      }
      val dimValGroups:Map[(String, String), Long] = dimValRaws.groupBy(x => (x._1, x._2)).mapValues(_.map(_._3).sum)
      val totalCnt = dimValGroups.getOrElse((ALL_VALUE, ALL_VALUE), 0L)

      dimValGroups.map{ case ((dim, value), count) =>
        val dimCnt = dimValGroups.getOrElse((dim, ALL_VALUE), 0L)
        ItemStat(itemId, dim, value, count, dimCnt, totalCnt)
      }
    }.toDS()


    val dimStat = itemStat.groupBy("dimension", "value").agg(sum("cnt").as("cnt"))
      .map{ row =>
        val dimension = row.getAs[String]("dimension")
        val value = row.getAs[String]("value")
        val cnt = row.getAs[Long]("cnt")

        (dimension, value) -> cnt
      }.collect().toMap

    itemStat.filter(i => i.dimension != ALL_VALUE && i.value != ALL_VALUE).map{ stat =>
      val dimValTotal = dimStat.getOrElse((stat.dimension, stat.value), 0L)
      val dimTotal = dimStat.getOrElse((stat.dimension, ALL_VALUE), 0L)

      val score = scoreFunc(topKFunc, stat, dimValTotal, dimTotal)

      (stat.dimension, stat.value) -> (stat.itemId, score, stat.cnt)
    }.rdd.groupByKey().flatMap{ case ((dimension, value), iters) =>
      iters.toSeq.sortBy(_._2).reverse.take(limit).zipWithIndex.map{ case ((itemId, score, cnt), rank) =>
        (dimension, value, itemId, score, rank, cnt)
      }
    }.toDF("dimension", "value", "itemId", "score", "rank", "cnt")
  }

  private def scoreFunc(topKFunc:String, stat: ItemStat, dimValTotal: Long, dimTotal: Long):Double = topKFunc match {
    case "llr" => customLLR(stat, dimValTotal, dimTotal)
    case _ =>
      if (topKFunc != "countSum") logger.warn(s"not supported function ${topKFunc}... using countSum")
      countSum(stat, dimValTotal, dimTotal)
  }

  private def customLLR(stat: ItemStat, dimValTotal: Long, dimTotal: Long):Double = {
    val threshold = 0.0

    val (k11, k12, k21, k22) = calculateTable(stat, dimValTotal, dimTotal)
    val positiveRatio = if (k11 + k12 == 0) -1 else k11.toDouble / (k11 + k12)
    val negativeRatio = if (k12 + k22 == 0) -1 else k12.toDouble / (k12 + k22)

    logger.debug(s"llr table: k11: $k11, k12: $k12, k21: $k21, k22: $k22, positive: ${positiveRatio}, negative: ${negativeRatio} (${stat.itemId}, ${stat.cnt})")

    if (positiveRatio / negativeRatio > threshold)
      logLikelihoodRatio(k11, k12, k21, k22)
    else
      0.0
  }

  private def calculateTable(stat: ItemStat, dimValTotal: Long, dimTotal: Long) = {
    val k11 = stat.cnt
    val k12 = dimValTotal - k11
    val k21 = stat.itemDimCnt - k11
    val k22 = dimTotal - dimValTotal - stat.itemDimCnt + k11

    (k11, k12, k21, k22)
  }

  private def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long): Double = {
    def xLogX(x: Long) = if (x == 0) 0.0 else x * Math.log(x)

    def entropy2(a: Long, b: Long): Double = xLogX(a + b) - xLogX(a) - xLogX(b)

    def entropy4(a: Long, b: Long, c: Long, d: Long): Double = xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d)

    val rowEntropy = entropy2(k11 + k12, k21 + k22)
    val columnEntropy = entropy2(k11 + k21, k12 + k22)
    val matrixEntropy = entropy4(k11, k12, k21, k22)

    if (rowEntropy + columnEntropy < matrixEntropy) 0.0
    else 2.0 * (rowEntropy + columnEntropy - matrixEntropy)
  }


  private def countSum(stat: ItemStat, dimValTotal: Long, dimTotal: Long):Double = {
    logger.debug(s"count sum : ${stat}")
    stat.cnt.toDouble
  }
}

