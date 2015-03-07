package com.datastax.spark.connector.demo

import com.datastax.spark.connector.cql.CassandraConnector
import scala.util.Random

object BasicReadWriteDemo extends DemoApp {

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS velox_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS user_models (userid INT PRIMARY KEY, model LIST)")
    session.execute("TRUNCATE user_models")
    // session.execute("INSERT INTO velox_test.key_value(key, value) VALUES (1, 'first row')")
    // session.execute("INSERT INTO velox_test.key_value(key, value) VALUES (2, 'second row')")
    // session.execute("INSERT INTO velox_test.key_value(key, value) VALUES (3, 'third row')")
  }

  import com.datastax.spark.connector._

  val modelDimensions = 50

  // Read table test.kv and print its contents:
  // Write two new rows to the test.kv table:
  val randomUserModels = sc.parallelize((0 to 50000)).map(u => (u, Seq.fill(modelDimensions)(Random.nextInt(5000)).toList))
  randomUserModels.saveToCassandra("velox_test", "user_models", SomeColumns("userid", "model"))


  // col.collect().foreach(row => log.info(s"New Data: $row"))
  log.info(s"Work completed, stopping the Spark context.")
  sc.stop()
}
