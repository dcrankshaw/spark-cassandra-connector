package com.datastax.spark.connector.demo

import com.datastax.spark.connector.util.Logging
import org.apache.spark.{SparkContext, SparkConf}

trait DemoApp extends App with Logging {

  val words = "./spark-cassandra-connector-demos/simple-demos/src/main/resources/data/words"

  val SparkMasterHost = "ec2-54-166-69-12.compute-1.amazonaws.com"

  val CassandraHost = "ec2-54-144-144-29.compute-1.amazonaws.com"


  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .set("spark.cleaner.ttl", "3600")
    .setMaster(SparkMasterHost)
    .setAppName(getClass.getSimpleName)

  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}
}
