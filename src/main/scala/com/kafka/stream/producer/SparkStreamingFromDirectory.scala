package com.kafka.stream.producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

    groupDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
}
