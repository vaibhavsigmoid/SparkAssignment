package question_2_test
import question2.ExportAggregatorDriver

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ExportAggregatorDriverTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("ExportAggregatorDriverTest")
    .master("local[*]")
    .getOrCreate()

  test("ExportAggregatorDriver should execute extraction, aggregation, and writing without errors") {
    val filePath = "/path/to/mock/data.csv"  // Add your test file path
    val year = 2018

    // Main execution (assuming it completes without throwing an error)
    ExportAggregatorDriver.main(Array(filePath, year.toString))
    println("Test 'ExportAggregatorDriver should execute extraction, aggregation, and writing without errors' passed.")
  }
}

