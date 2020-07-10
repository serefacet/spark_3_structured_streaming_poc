package com.serefacet.spark.streaming

import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.functions.from_confluent_avro
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, StreamingQueryManager, Trigger}
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies


object SparkDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Udemy_Data_Deep_Dive")
      .getOrCreate()
    //  spark.sparkContext.setLogLevel("INFO")

    spark.conf.set("spark.sql.shuffle.partitions",10)

    val config = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:18081",
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "event_tracking.raw_events.CLPServeEvent",
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "CLPServeEvent",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "event_tracking.raw_events",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaStorageNamingStrategies.RECORD_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
    )

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:19092,localhost:29092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "event_tracking.raw_events.CLPServeEvent")
      .load()
      .select(from_confluent_avro(functions.col("value"), config).as("data"))
      .select("data.*")
      .groupBy("courseId")
      .count()


    val query = df
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    //debugStreams(spark.streams)
    query.awaitTermination()
  }

  def debugStreams(streams: StreamingQueryManager): Unit = {
    streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })
  }

}
