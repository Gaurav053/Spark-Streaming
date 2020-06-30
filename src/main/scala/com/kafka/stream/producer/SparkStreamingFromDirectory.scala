package com.kafka.stream.producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .appName("ReadFromDirectory")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val df = spark.readStream
      .schema(schema)
      .json("hdfs://HDPHAPR/user/cbhdj/test/data")

    df.printSchema()

    val groupDF = df.select("firstname")
    groupDF.printSchema()

    df.selectExpr("CAST(id as STRING) AS key",
    "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "turia.atldc.nscorp.com:6667")
      .option("topic", "DE.ATC.BOS.Utcs.Raw")
      .option("checkpointLocation", "hdfs://HDPHAPR/user/cbhdj/spark_checkpoint_2")
      .start()
      .awaitTermination()
  }
}