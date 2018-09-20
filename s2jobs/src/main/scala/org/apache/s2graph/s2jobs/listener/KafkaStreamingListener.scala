package org.apache.s2graph.s2jobs.listener

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.s2graph.s2jobs.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener
import java.util.Properties

class KafkaStreamingListener(options:Map[String, String]) extends StreamingQueryListener with Logger {
  def mandatoryOptions: Set[String] = Set("bootstrap.servers", "topic")
  def isValidate: Boolean = mandatoryOptions.subsetOf(options.keySet)

  require(isValidate,
    s"""${this.getClass.getName} does not exists mandatory options '${mandatoryOptions.mkString(",")}'
        in task options (${options.keySet.mkString(",")})
  """.stripMargin)


  lazy val topic = options("topic")
  lazy val producer = new KafkaProducer[String, String](toKafkaProps)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logger.info(s"QueryStarted : ${event.name}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val key = System.currentTimeMillis().toString
    val value = event.progress.json
    logger.debug(s"QueryProgress: $key -> $value")

    producer.send(new ProducerRecord[String, String](topic, key, value))
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info(s"QueryTerminated : ${event.exception}")
  }

  private def toKafkaProps = {
    val defaultProps = Map(
      "acks"                  -> "1",
      "buffer.memory"         -> "33554432",
      "compression.type"      -> "snappy",
      "retries"               ->  "0",
      "batch.size"            -> "16384",
      "linger.ms"             -> "100",
      "max.request.size"      -> "1048576",
      "receive.buffer.bytes"  ->"32768",
      "send.buffer.bytes"     -> "131072",
      "timeout.ms"            -> "30000",
      "block.on.buffer.full"  -> "false",
      "value.serializer"      -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.serializer"        -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val props = new Properties()
    (defaultProps ++ options).foreach{ case (k, v) => if (k != "topic") props.put(k, v)}
    props
  }
}
