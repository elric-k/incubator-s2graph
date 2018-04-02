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

package org.apache.s2graph.spark.sql.streaming

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.{GraphElement, JSONParser}
import org.apache.s2graph.spark.sql.streaming.S2SinkConfigs._
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType
import play.api.libs.json.{JsObject, Json}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

private [sql] class S2StreamQueryWriter(
                                         serializedConf:String,
                                         schema: StructType ,
                                         commitProtocol: S2CommitProtocol
                                       ) extends Serializable with Logger {
  private val config = ConfigFactory.parseString(serializedConf)
  private val s2SinkContext = S2SinkContext(config)
  private val encoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()
  private val RESERVED_COLUMN = Set("timestamp", "from", "to", "label", "operation", "elem", "direction")


  def run(taskContext: TaskContext, iters: Iterator[InternalRow]): TaskCommit = {
    val taskId = s"stage-${taskContext.stageId()}, partition-${taskContext.partitionId()}, attempt-${taskContext.taskAttemptId()}"
    val partitionId= taskContext.partitionId()

    val s2Graph = s2SinkContext.getGraph
    val groupedSize = getConfigString(config, S2_SINK_GROUPED_SIZE, DEFAULT_GROUPED_SIZE).toInt
    val waitTime = getConfigString(config, S2_SINK_WAIT_TIME, DEFAULT_WAIT_TIME_SECONDS).toInt

    commitProtocol.initTask()
    try {
      var list = new ListBuffer[(String, Int)]()
      val rst = iters.flatMap(rowToEdge).grouped(groupedSize).flatMap{ elements =>
        logger.debug(s"[$taskId][elements] ${elements.size} (${elements.map(e => e.toLogString).mkString(",\n")})")
        elements.groupBy(_.serviceName).foreach{ case (service, elems) =>
          list += ((service, elems.size))
        }

        val mutateF = s2Graph.mutateElements(elements, true)
        Await.result(mutateF, Duration(waitTime, "seconds"))
      }

      val (success, fail) = rst.toSeq.partition(r => r.isSuccess)
      val counter = list.groupBy(_._1).map{ case (service, t) =>
        val sum = t.toList.map(_._2).sum
        (service, sum)
      }
      logger.info(s"[$taskId] success : ${success.size}, fail : ${fail.size} ($counter)")


      commitProtocol.commitTask(TaskState(partitionId, success.size, fail.size, counter))

    } catch {
      case t: Throwable =>
        commitProtocol.abortTask(TaskState(partitionId))
        throw t
    }
  }

  private def rowToEdge(internalRow:InternalRow): Option[GraphElement] = {
    val s2Graph = s2SinkContext.getGraph
    val row = encoder.fromRow(internalRow)

    val timestamp = row.getAs[Long]("timestamp")
    val operation = Try(row.getAs[String]("operation")).getOrElse("insert")
    val elem = Try(row.getAs[String]("elem")).getOrElse("e")

    val props: Map[String, Any] = Option(row.getAs[String]("props")) match {
      case Some(propsStr:String) =>
        JSONParser.fromJsonToProperties(Json.parse(propsStr).as[JsObject])
      case None =>
        schema.fieldNames.flatMap { field =>
          if (!RESERVED_COLUMN.contains(field)) {
            Seq(
              field -> getRowValAny(row, field)
            )
          } else Nil
        }.toMap
    }

    elem match {
      case "e" | "edge" =>
        val from = getRowValAny(row, "from")
        val to = getRowValAny(row, "to")
        val label = row.getAs[String]("label")
        val direction = Try(row.getAs[String]("direction")).getOrElse("out")
        Some(
          s2Graph.elementBuilder.toEdge(from, to, label, direction, props, timestamp, operation)
        )
      case "v" | "vertex" =>
        val id = getRowValAny(row, "id")
        val serviceName = row.getAs[String]("service")
        val columnName = row.getAs[String]("column")
        Some(
          s2Graph.elementBuilder.toVertex(serviceName, columnName, id, props, timestamp, operation)
        )
      case _ =>
        logger.warn(s"'$elem' is not GraphElement. skipped!! (${row.toString()})")
        None
    }
  }

  private def getRowValAny(row:Row, fieldName:String):Any = {
    val idx = row.fieldIndex(fieldName)
    row.get(idx)
  }
}