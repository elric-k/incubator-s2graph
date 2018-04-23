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


/**
  * Source
  *
  * @param conf
  */
abstract class Source(override val conf:TaskConf) extends Task {
  def toDF(ss:SparkSession):DataFrame
}

class KafkaSource(conf:TaskConf) extends Source(conf) {
  val DEFAULT_FORMAT = "raw"
  override def mandatoryOptions: Set[String] = Set("kafka.bootstrap.servers", "subscribe")

  override def toDF(ss:SparkSession):DataFrame = {
    logger.info(s"${LOG_PREFIX} options: ${conf.options}")

    val format = conf.options.getOrElse("format", "raw")
    val df = ss.readStream.format("kafka").options(conf.options).load()

    format match {
      case "raw" => df
      case "json" => parseJsonSchema(ss, df)
//      case "custom" => parseCustomSchema(df)
      case _ =>
        logger.warn(s"${LOG_PREFIX} unsupported format '$format'.. use default schema ")
        df
    }
  }

  def parseJsonSchema(ss:SparkSession, df:DataFrame):DataFrame = {
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.types.DataType
    import ss.implicits._

    val schemaOpt = conf.options.get("schema")
    schemaOpt match {
      case Some(schemaAsJson:String) =>
        val dataType:DataType = DataType.fromJson(schemaAsJson)
        logger.debug(s"${LOG_PREFIX} schema : ${dataType.sql}")

        df.selectExpr("CAST(value AS STRING)")
          .select(from_json('value, dataType) as 'struct)
          .select("struct.*")

      case None =>
        logger.warn(s"${LOG_PREFIX} json format does not have schema.. use default schema ")


        ss.read.json(df.selectExpr("CAST(value AS STRING)").toJSON)
    }
  }
}

class FileSource(conf:TaskConf) extends Source(conf) {
  val DEFAULT_FORMAT = "parquet"
  override def mandatoryOptions: Set[String] = Set("paths")

  override def toDF(ss: SparkSession): DataFrame = {
    import org.apache.s2graph.s2jobs.Schema._
    val paths = conf.options("paths").split(",")
    val format = conf.options.getOrElse("format", DEFAULT_FORMAT)

    format match {
      case "edgeLog" =>
        ss.read.format("com.databricks.spark.csv").option("delimiter", "\t")
          .schema(BulkLoadSchema).load(paths: _*)
      case _ => ss.read.format(format).load(paths: _*)
    }
  }
}

class HiveSource(conf:TaskConf) extends Source(conf) {
  override def mandatoryOptions: Set[String] = Set("database", "table")

  override def toDF(ss: SparkSession): DataFrame = {
    val database = conf.options("database")
    val table = conf.options("table")

    val sql = conf.options.getOrElse("sql", s"SELECT * FROM ${database}.${table}")
    ss.sql(sql)
  }
}
