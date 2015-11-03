package io.snappydata.core

import java.sql.{SQLException, DriverManager}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode

/**
 * Created by rishim on 3/11/15.
 */
class JDBCColumnarRelationAPISuite extends FunSuite with Logging with BeforeAndAfter {

  private val testSparkContext = SnappySQLContext.sparkContext

  before {
    DriverManager.getConnection("jdbc:derby:./JdbcRDDSuiteDb;create=true")
  }
  after {
    try {
      DriverManager.getConnection("jdbc:derby:./JdbcRDDSuiteDb;shutdown=true")
    } catch {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      case sqlEx: SQLException => {
        if (sqlEx.getSQLState.compareTo("08006") != 0) {
          throw sqlEx
        }
      }
    }

  }

  test("Create table in an external DataStore in Non-Embedded mode") {
    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val props = Map(
      "url" -> "jdbc:derby:./JdbcRDDSuiteDb",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    snc.sql("DROP TABLE IF EXISTS TEST_JDBC_TABLE_2")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("TEST_JDBC_TABLE_2")
    val count = dataDF.count()
    assert(count === data.length)
  }
}
