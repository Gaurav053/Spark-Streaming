name := "kafkaStream"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.3.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.4",
  "org.apache.spark" %% "spark-streaming" % "2.3.4",
  "org.apache.spark" %% "spark-sql" % "2.3.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4",
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
)
