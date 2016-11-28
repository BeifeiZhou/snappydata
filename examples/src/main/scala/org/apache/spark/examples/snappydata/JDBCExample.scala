/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.examples.snappydata

import java.sql.DriverManager

import scala.util.Try

/**
 * An example that shows JDBC operations on SnappyData system
 *
 * Before running this example, ensure that SnappyData cluster is started and
 * running. To start the cluster execute the following command:
 * sbin/snappy-start-all.sh
 */
object JDBCExample {
  def doOperationsUsingJDBC(clientPort: String): Unit = {
    println("****JDBCExample****")

    println("Initializing a JDBC connection")
    // JDBC url string to connect to SnappyData cluster
    val url: String = s"jdbc:snappydata://localhost:$clientPort/"
    val conn1 = DriverManager.getConnection(url)

    val stmt1 = conn1.createStatement()
    println("Creating a table (PARTSUPP) using JDBC connection")
    stmt1.execute("DROP TABLE IF EXISTS APP.PARTSUPP")
    stmt1.execute("CREATE TABLE APP.PARTSUPP ( " +
        "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
        "PS_SUPPKEY     INTEGER NOT NULL," +
        "PS_AVAILQTY    INTEGER NOT NULL," +
        "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
        "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY', BUCKETS '11' )")

    println()
    println("Inserting a record in PARTSUPP table using prepared statement")
    val preparedStmt1 = conn1.prepareStatement("INSERT INTO APP.PARTSUPP VALUES(?, ?, ?, ?)")
    preparedStmt1.setInt(1, 100)
    preparedStmt1.setInt(2, 1)
    preparedStmt1.setInt(3, 5000)
    preparedStmt1.setInt(4, 100)
    preparedStmt1.execute()
    preparedStmt1.close()

    println()
    println("Inserting data in PARTSUPP table using statement")
    stmt1.execute("INSERT INTO APP.PARTSUPP VALUES(200, 2, 50, 10)")
    stmt1.execute("INSERT INTO APP.PARTSUPP VALUES(300, 3, 1000, 20)")
    stmt1.execute("INSERT INTO APP.PARTSUPP VALUES(400, 4, 200, 30)")

    println()
    println("The contents of PARTSUPP are")
    val rs1 = stmt1.executeQuery("SELECT * FROM APP.PARTSUPP")
    while (rs1.next()) {
      println(rs1.getInt(1) + "," + rs1.getInt(2) + "," + rs1.getInt(3) + "," + rs1.getInt(4))
    }
    rs1.close()
    stmt1.close()

    println()
    println("Initializing another JDBC connection")
    val conn2 = DriverManager.getConnection(url)

    println()
    println("Displaying the the list of tables using second JDBC connection")
    val md = conn2.getMetaData()
    val rs2 = md.getTables(null, "APP", "%", null)
    while (rs2.next()) {
      println(rs2.getString(3))
    }

    println()
    println("Selecting records of PARTSUPP from the second JDBC connection")
    println("The contents of PARTSUPP are")
    val rs3 = conn2.createStatement().executeQuery("SELECT * FROM APP.PARTSUPP")
    while (rs3.next()) {
      println(rs3.getInt(1) + "," + rs3.getInt(2) + "," + rs3.getInt(3) + "," + rs3.getInt(4))
    }
    rs3.close()
    stmt1.close()

    conn1.close()
    conn2.close()

    println("****Done****")
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 1) {
      printUsage()
    } else if (args.length == 0) {
      println("Using default client port 1527 for JDBC connection")
      doOperationsUsingJDBC("1527")
    } else {
      if (Try(args(0).toInt).isFailure) {
        printUsage()
      } else {
        doOperationsUsingJDBC(args(0))
      }

    }

  }

  def printUsage(): Unit = {
    val usage: String =
      "Usage: bin/run-example JDBCExample <clientPort>\n" +
          "\n" +
          "clientPort - client port number for SnappyData on which JDBC connections are accepted \n"
    println(usage)
  }
}
