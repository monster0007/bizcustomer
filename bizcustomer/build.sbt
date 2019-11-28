name := "bizcustomer"

version := "0.1"

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cdh-releases-rcs/"
)

libraryDependencies += "org.apache.spark"%"spark-core_2.11"%"2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark"%"spark-sql_2.11"%"2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-cdh6.0.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"

libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.3" from "http://repo1.maven.org/maven2/net/sf/json-lib/json-lib/2.3/json-lib-2.3-jdk15.jar"

// https://mvnrepository.com/artifact/org.apache.hive/hive-hbase-handler
libraryDependencies += "org.apache.hive" % "hive-hbase-handler" % "2.1.1-cdh6.0.1"

// https://mvnrepository.com/artifact/io.jvm.uuid/scala-uuid
libraryDependencies += "io.jvm.uuid" %% "scala-uuid" % "0.3.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"

libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.3" from "http://repo1.maven.org/maven2/net/sf/json-lib/json-lib/2.3/json-lib-2.3-jdk15.jar"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP9" % Test

