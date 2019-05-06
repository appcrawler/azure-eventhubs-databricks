// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.functions._

val eventHubsConf = EventHubsConf("Endpoint=sb://sdhhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={YOUR_KEY};EntityPath=sdhhub").setStartingPosition(EventPosition.fromStartOfStream)

val spark = SparkSession.builder.appName("Tweets").getOrCreate()
val eventHubStream = spark.readStream.format("eventhubs").options(eventHubsConf.toMap).load()
eventHubStream.printSchema()
val messages = eventHubStream.selectExpr("cast (body as string) AS Content")
//messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()


// COMMAND ----------

val df = eventHubStream.select(explode(split($"body".cast("string"), "\\s+")).as("word"))
  .groupBy($"word")
  .count

// COMMAND ----------

display(df.select($"word", $"count"))
