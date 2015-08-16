package com.daumkakao.s2graph.core


import com.daumkakao.s2graph.core.KGraphExceptions.LabelAlreadyExistException
import com.daumkakao.s2graph.core.Management.Model.{Index, Prop}
import com.daumkakao.s2graph.core.mysqls._
import play.api.libs.json.Reads._

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.types2._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import play.api.Logger
import play.api.libs.json._

/**
 * This is designed to be bridge between rest to s2core.
 * s2core never use this for finding models.
 */
object Management extends JSONParser {
  object Model {
    case class Prop(name: String, defaultValue: String, datatType: String)
    object Prop extends ((String, String, String) => Prop)
    case class Index(name: String, propNames: Seq[String])
  }

  import HBaseType._
  val hardLimit = 10000
  val defaultLimit = 100

  //  def getSequence(tableName: String) = {
  //    HBaseModel.getSequence(tableName)
  //  }
  def createService(serviceName: String,
                    cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): Service = {
    val service = Service.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL)
    service
  }

  def findService(serviceName: String) = {
    Service.findByName(serviceName, useCache = false)
  }

  def deleteService(serviceName: String) = {
    Service.findByName(serviceName).foreach { service =>
      //      service.deleteAll()
    }
  }
  /**
   * label
   */
  /**
   * copy label when if oldLabel exist and newLabel do not exist.
   * copy label: only used by bulk load job. not sure if we need to parameterize hbase cluster.
   */
  def copyLabel(oldLabelName: String, newLabelName: String, hTableName: Option[String]) = {
    val old = Label.findByName(oldLabelName).getOrElse(throw new LabelAlreadyExistException(s"Old label $oldLabelName already exists."))
    if (Label.findByName(newLabelName).isDefined) throw new LabelAlreadyExistException(s"New label $newLabelName already exists.")

    val allProps = old.metas.map { labelMeta => Prop(labelMeta.name, labelMeta.defaultValue, labelMeta.dataType) }
    val allIndices = old.indices.map { index => Index(index.name, index.propNames) }

    createLabel(newLabelName, old.srcService.serviceName, old.srcColumnName, old.srcColumnType,
      old.tgtService.serviceName, old.tgtColumnName, old.tgtColumnType,
      old.isDirected, old.serviceName,
      allIndices, allProps,
      old.consistencyLevel, hTableName, old.hTableTTL, old.schemaVersion, old.isAsync)
  }

  def createLabel(label: String,
                  srcServiceName: String,
                  srcColumnName: String,
                  srcColumnType: String,
                  tgtServiceName: String,
                  tgtColumnName: String,
                  tgtColumnType: String,
                  isDirected: Boolean = true,
                  serviceName: String,
                  indices: Seq[Index],
                  props: Seq[Prop],
                  consistencyLevel: String,
                  hTableName: Option[String],
                  hTableTTL: Option[Int],
                  schemaVersion: String = DEFAULT_VERSION,
                  isAsync: Boolean): Label = {

    val labelOpt = Label.findByName(label, useCache = false)

    labelOpt match {
      case Some(l) =>
        throw new KGraphExceptions.LabelAlreadyExistException(s"Label name ${l.label} already exist.")
      case None =>
        Label.insertAll(label,
          srcServiceName, srcColumnName, srcColumnType,
          tgtServiceName, tgtColumnName, tgtColumnType,
          isDirected, serviceName, indices, props, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync)
        Label.findByName(label, useCache = false).get
    }
  }

  def createServiceColumn(serviceName: String,
                   columnName: String,
                   columnType: String,
                   props: Seq[(String, String, String)],
                   schemaVersion: String = DEFAULT_VERSION) = {
    val serviceOpt = Service.findByName(serviceName)
    serviceOpt match {
      case None => throw new RuntimeException(s"create service $serviceName has not been created.")
      case Some(service) =>
        val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType), schemaVersion)
        for {
          (propName, defaultValue, dataType) <- props
        } yield {
          ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
        }
    }
  }
  def deleteColumn(serviceName: String, columnName: String, schemaVersion: String = DEFAULT_VERSION) = {
    for {
      service <- Service.findByName(serviceName, useCache = false)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName, useCache = false)
    } yield {
      ServiceColumn.delete(serviceColumn.id.get)
    }
  }
  def findLabel(labelName: String): Option[Label] = {
    Label.findByName(labelName, useCache = false)
  }

  def deleteLabel(labelName: String) = {
    Label.findByName(labelName, useCache = false).foreach { label =>
      label.deleteAll()
    }
  }

  def addIndex(labelStr: String, indices: Seq[Index]): Label = {
    val result = for {
      label <- Label.findByName(labelStr)
    } yield {
        val labelMetaMap = label.metaPropsInvMap
        indices.foreach { index =>
          val metaSeq = index.propNames.map { name => labelMetaMap(name).seq }
          LabelIndex.findOrInsert(label.id.get, index.name, metaSeq.toList, "none")
        }
        label
      }

    result.getOrElse(throw new RuntimeException(s"add index failed"))
  }

  def addProp(labelStr: String, propName: String, defaultValue: JsValue, dataType: String): LabelMeta = {
    val result = for {
      label <- Label.findByName(labelStr)
    } yield {
        LabelMeta.findOrInsert(label.id.get, propName, defaultValue.toString, dataType)
      }
    result.getOrElse(throw new RuntimeException(s"add property on label failed"))
  }

  def addVertexProp(serviceName: String,
                    columnName: String,
                    propsName: String,
                    propsType: String,
                    schemaVersion: String = DEFAULT_VERSION): ColumnMeta = {
    val result = for {
      service <- Service.findByName(serviceName, useCache = false)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
        ColumnMeta.findOrInsert(serviceColumn.id.get, propsName, propsType)
      }
    result.getOrElse({
      throw new RuntimeException(s"add property on vertex failed")
    })
  }

  def getServiceLable(label: String): Option[Label] = {
    Label.findByName(label, useCache = true)
  }

  /**
   *
   */

  def toLabelWithDirectionAndOp(label: Label, direction: String): Option[LabelWithDirection] = {
    for {
      labelId <- label.id
      dir = GraphUtil.toDirection(direction)
    } yield LabelWithDirection(labelId, dir)
  }

  def tryOption[A, R](key: A, f: A => Option[R]) = {
    f(key) match {
      case None => throw new KGraphExceptions.InternalException(s"$key is not found in DB. create $key first.")
      case Some(r) => r
    }
  }

  def toEdge(ts: Long, operation: String, srcId: String, tgtId: String,
             labelStr: String, direction: String = "", props: String): Edge = {

    val label = tryOption(labelStr, getServiceLable)
    val dir =
      if (direction == "") GraphUtil.toDirection(label.direction)
      else GraphUtil.toDirection(direction)

//    Logger.debug(s"$srcId, ${label.srcColumnWithDir(dir)}")
//    Logger.debug(s"$tgtId, ${label.tgtColumnWithDir(dir)}")

    val srcVertexId = toInnerVal(srcId, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexId = toInnerVal(tgtId, label.tgtColumn.columnType, label.schemaVersion)

    val srcColId = label.srcColumn.id.get
    val tgtColId = label.tgtColumn.id.get
    val (srcVertex, tgtVertex) = if (dir == GraphUtil.directions("out")) {
      (Vertex(SourceVertexId(srcColId, srcVertexId), System.currentTimeMillis()),
        Vertex(TargetVertexId(tgtColId, tgtVertexId), System.currentTimeMillis()))
    } else {
      (Vertex(SourceVertexId(tgtColId, tgtVertexId), System.currentTimeMillis()),
        Vertex(TargetVertexId(srcColId, srcVertexId), System.currentTimeMillis()))
    }

    //    val dir = if (direction == "") GraphUtil.toDirection(label.direction) else GraphUtil.toDirection(direction)
    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val op = tryOption(operation, GraphUtil.toOp)

    val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
    val parsedProps = toProps(label, jsObject).toMap
    val propsWithTs = parsedProps.map(kv => (kv._1 -> InnerValLikeWithTs(kv._2, ts))) ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, label.schemaVersion), ts))
    Edge(srcVertex, tgtVertex, labelWithDir, op, ts, version = ts, propsWithTs = propsWithTs)

  }

  def toVertex(ts: Long, operation: String, id: String, serviceName: String, columnName: String, props: String): Vertex = {
    Service.findByName(serviceName) match {
      case None => throw new RuntimeException(s"$serviceName does not exist. create service first.")
      case Some(service) =>
        ServiceColumn.find(service.id.get, columnName) match {
          case None => throw new RuntimeException(s"$columnName is not exist. create service column first.")
          case Some(col) =>
            val idVal = toInnerVal(id, col.columnType, col.schemaVersion)
            val op = tryOption(operation, GraphUtil.toOp)
            val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
            val parsedProps = toProps(col, jsObject).toMap
            Vertex(VertexId(col.id.get, idVal), ts, parsedProps, op = op)
        }
    }
  }

  def toProps(column: ServiceColumn, js: JsObject): Seq[(Int, InnerValLike)] = {

    val props = for {
      (k, v) <- js.fields
      meta <- column.metasInvMap.get(k)
    } yield {
        val innerVal = jsValueToInnerVal(v, meta.dataType, column.schemaVersion).getOrElse(
          throw new RuntimeException(s"$k is not defined. create schema for vertex."))

        (meta.seq.toInt, innerVal)
      }
    props

  }

  def toProps(label: Label, js: JsObject): Seq[(Byte, InnerValLike)] = {

    val props = for {
      (k, v) <- js.fields
      meta <- label.metaPropsInvMap.get(k)
      //        meta <- LabelMeta.findByName(label.id.get, k)
      //      meta = tryOption((label.id.get, k), LabelMeta.findByName)
      innerVal <- jsValueToInnerVal(v, meta.dataType, label.schemaVersion)
    } yield {
        (meta.seq, innerVal)
      }
    //    Logger.error(s"toProps: $js => $props")
    props

  }

  val idTableName = "id"
  val cf = "a"
  val idColName = "id"
  val regionCnt = 10

  def getAdmin(zkAddr: String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkAddr)
    val conn = ConnectionFactory.createConnection(conf)
    conn.getAdmin
  }

  def enableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).enableTable(TableName.valueOf(tableName))
  }

  def disableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
  }

  def dropTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
    getAdmin(zkAddr).deleteTable(TableName.valueOf(tableName))
  }

  //  def deleteEdgesByLabelIds(zkAddr: String,
  //    tableName: String,
  //    labelIds: String = "",
  //    minTs: Long = 0L,
  //    maxTs: Long = Long.MaxValue,
  //    include: Boolean = true) = {
  //    val conf = HBaseConfiguration.create()
  //    val longTimeout = "1200000"
  //    conf.set("hbase.rpc.timeout", longTimeout)
  //    conf.set("hbase.client.operation.timeout", longTimeout)
  //    conf.set("hbase.client.scanner.timeout.period", longTimeout)
  //    conf.set("hbase.zookeeper.quorum", zkAddr)
  //    val conn = HConnectionManager.createConnection(conf)
  //    val table = conn.getTable(tableName.getBytes)
  //    var builder = DeleteLabelsArgument.newBuilder()
  //    val scanner = Scan.newBuilder()
  //
  //    scanner.setTimeRange(TimeRange.newBuilder().setFrom(minTs).setTo(maxTs))
  //    /**
  //     *  when we clean up all data does not match current database ids
  //     *  we will delete row completely
  //     */
  //    if (!include) scanner.setFilter(ProtobufUtil.toFilter(new FirstKeyOnlyFilter))
  //
  //    builder.setScan(scanner)
  //    for (id <- labelIds.split(",")) {
  //      builder.addId(id.toInt)
  //    }
  //
  //    val argument = builder.build()
  //
  //    val regionStats = table.coprocessorService(classOf[GraphStatService], null, null,
  //      new Batch.Call[GraphStatService, Long]() {
  //        override def call(counter: GraphStatService): Long = {
  //          val controller: ServerRpcController = new ServerRpcController()
  //          val rpcCallback: BlockingRpcCallback[CountResponse] = new BlockingRpcCallback[CountResponse]()
  //
  //          if (include) {
  //            counter.cleanUpDeleteLabelsRows(controller, argument, rpcCallback)
  //          } else {
  //            counter.cleanUpDeleteLabelsRowsExclude(controller, argument, rpcCallback)
  //          }
  //
  //          val response: CountResponse = rpcCallback.get()
  //          if (controller.failedOnException()) throw controller.getFailedOn()
  //          if (response != null && response.hasCount()) {
  //            response.getCount()
  //          } else {
  //            0L
  //          }
  //        }
  //      })
  //
  //    //    regionStats.map(kv => Bytes.toString(kv._1) -> kv._2) ++ Map("total" -> regionStats.values().sum)
  //  }
  def createTable(zkAddr: String, tableName: String, cfs: List[String], regionMultiplier: Int, ttl: Option[Int]) = {
    Logger.info(s"create table: $tableName on $zkAddr, $cfs, $regionMultiplier")
    val admin = getAdmin(zkAddr)
    val regionCount = admin.getClusterStatus.getServersSize * regionMultiplier
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      try {
        val desc = new HTableDescriptor(TableName.valueOf(tableName))
        desc.setDurability(Durability.ASYNC_WAL)
        for (cf <- cfs) {
          val columnDesc = new HColumnDescriptor(cf)
            .setCompressionType(Compression.Algorithm.LZ4)
            .setBloomFilterType(BloomType.ROW)
            .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
            .setMaxVersions(1)
            .setTimeToLive(2147483647)
            .setMinVersions(0)
            .setBlocksize(32768)
            .setBlockCacheEnabled(true)
          if (ttl.isDefined) columnDesc.setTimeToLive(ttl.get)
          desc.addFamily(columnDesc)
        }

        if (regionCount <= 1) admin.createTable(desc)
        else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
      } catch {
        case e: Throwable =>
          Logger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      }
    } else {
      Logger.error(s"$zkAddr, $tableName, $cf already exist.")
    }
  }

    /**
   * update label name.
   */
  def updateLabelName(oldLabelName: String, newLabelName: String) = {
    for {
      old <- Label.findByName(oldLabelName)
    } {
      Label.findByName(newLabelName) match {
        case None =>
          Label.updateName(oldLabelName, newLabelName)
        case Some(_) =>
        //          throw new RuntimeException(s"$newLabelName already exist")
      }
    }
  }

  // we only use murmur hash to distribute row key.
  private def getStartKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount))
  }

  private def getEndKey(regionCount: Int) = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)))
  }


}
