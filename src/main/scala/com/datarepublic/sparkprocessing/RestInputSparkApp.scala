package com.datarepublic.sparkprocessing

import java.io.File
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeoutException

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import com.microsoft.ml.spark.io.IOImplicits._
import org.apache.spark.sql.execution.streaming.DistributedHTTPSourceProvider
import com.microsoft.ml.spark.io.http.HTTPSchema.string_to_response
import org.apache.spark.sql.functions._

object RestInputSparkApp extends App {
  val log = Logger.getLogger(getClass.getName)
  val host = "localhost"
  val port = 8889
  val apiName = "foo"

  val tmpDir = {
    tmpDirCreated = true
    Files.createTempDirectory("MML-Test-")
  }
  val sparkConf = new SparkConf()
    .setAppName("Test App")
    .setMaster(s"local[*]")
    .set("spark.logConf", "true")
    .set("spark.sql.shuffle.partitions", "5")
    .set("spark.driver.maxResultSize", "6g")
    .set("spark.sql.warehouse.dir", "/tmp/sparkWarehouse")
    .set("spark.sql.crossJoin.enabled", "true")
  val session: SparkSession = {
    //    log.info(s"Creating a spark session for suite $this")
    sessionInitialized = true
    val sess = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    sess
  }
  var tmpDirCreated = false

  import session.implicits._
  var sessionInitialized = false

  val mySchema = new StructType()
    .add("name",StringType)
      .add("firstname",StringType)
      .add("middlename",StringType)

  def createServer(): DataStreamWriter[Row] = {
    def baseReader: DataStreamReader = {
      session.readStream.server
        .address(host, port, apiName)
        .option("maxPartitions", 3)
    }
    def baseWriter(df: DataFrame): DataStreamWriter[Row] = {
      df.writeStream
        .server
        .option("name", apiName)
        .queryName(apiName)
        .option("checkpointLocation",
          new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
    }
    log.info(classOf[DistributedHTTPSourceProvider].getName)

    baseWriter(baseReader
      .load()
        .parseRequest(apiName,mySchema)
//        .withColumn("content",col("request.entity.content"))
//      .withColumn("contentLength", col("request.entity.contentLength"))
//      .withColumn("reply", string_to_response( concat( col("content").cast(StringType) )))
        .withColumn("reply", string_to_response(col("name").cast(StringType)))
    )
  }

  def createDistbutedServer(): DataStreamWriter[Row] = {
    def baseReaderDist: DataStreamReader = {
      session.readStream.distributedServer
        .address(host, port, "foo")
        .option("maxPartitions", 3)
    }
    def baseWriterDist(df: DataFrame): DataStreamWriter[Row] = {
      df.writeStream
        .distributedServer
        .option("name", "foo")
        .queryName("foo")
        .option("checkpointLocation",
          new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
    }
    baseWriterDist(
      baseReaderDist
        .load()
        .withColumn("contentLength", col("request.entity.contentLength"))
        .withColumn("reply", string_to_response(col("contentLength").cast(StringType)))

    )
  }

  def waitForServer(server: StreamingQuery, maxTimeWaited: Int = 50000, checkEvery: Int = 1000): Unit = {
    var waited = 0
    while (waited < maxTimeWaited) {
      if (!server.isActive) throw server.exception.get
      if (server.recentProgress.length > 1) return
      Thread.sleep(checkEvery.toLong)
      waited += checkEvery
    }
    throw new TimeoutException(s"Server Did not start within $maxTimeWaited ms")
  }

  val server = createServer().start()

  waitForServer(server)

  server.awaitTermination()


}