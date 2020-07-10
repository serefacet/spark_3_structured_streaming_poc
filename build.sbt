name := "structured_streaming_with_spark_3"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
  "org.apache.spark" %% "spark-avro" % "3.0.0",
  "za.co.absa" %% "abris" % "3.2.1"
)
