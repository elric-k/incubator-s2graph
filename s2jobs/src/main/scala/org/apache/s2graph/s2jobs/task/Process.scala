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

    ss.sql(sql)
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

    var groupedKeys = conf.options(s"${PREFIX}.keys").split(",").map{ key =>
      col(key.trim)
    }.toSeq

    if (windowDurationOpt.isDefined && slideDurationOpt.isDefined){
      logger.debug(s">> using window operation : Duration ${windowDurationOpt}, slideDuration : ${slideDurationOpt}")
      groupedKeys = groupedKeys ++ Seq(window(col(timeColumn), windowDurationOpt.get, slideDurationOpt.get))
    }
    logger.debug(s">> groupedKeys: ${groupedKeys}")

    // aggregate options
    val aggExprs = Json.parse(conf.options.getOrElse(s"${PREFIX}.dataset.agg", "[\"count(1)\"]")).as[Seq[String]].map(expr(_))

//    logger.debug(s">> aggMap: ${aggMap}")
    logger.debug(s">> aggr : ${aggExprs}")

    val groupedDF = ss.sql(sql)
      .withWatermark(timeColumn, waterMarkDelayTime)
      .groupBy(
        groupedKeys: _*
      )
      if (aggExprs.size > 1) {
        groupedDF.agg(aggExprs.head, aggExprs.tail: _*)
      } else {
        groupedDF.agg(aggExprs.head)
      }
  }

}

