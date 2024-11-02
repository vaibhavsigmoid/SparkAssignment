package question_3_test

import question3.ExportAggregator
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class ExportAggregatorSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("ExportAggregatorTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("ExportAggregator.aggregateData should aggregate data by country") {
    val data = Seq(
      ("India", 1000.0),
      ("India", 2000.0),
      ("USA", 5000.0)
    ).toDF("country", "value")

    val result = ExportAggregator.aggregateData(data)
    assert(result.filter(col("country") === "India").head().getAs[Double]("total_export") == 3000.0)
    assert(result.filter(col("country") === "USA").head().getAs[Double]("total_export") == 5000.0)

    println("ExportAggregator.aggregateData test case passed.")
  }

  test("ExportAggregator.categorizeExporters should categorize exporters correctly") {
    val data = Seq(
      ("India", 3000.0),
      ("USA", 5000.0),
      ("China", 1000.0),
      ("Germany", 2000.0)
    ).toDF("country", "total_export")

    val result = ExportAggregator.categorizeExporters(data)

    assert(result.filter(col("category") === "big").count() == 3)
    assert(result.filter(col("category") === "small").count() == 1)
    assert(result.filter(col("category") === "medium").count() == 0)

    println("ExportAggregator.categorizeExporters test case passed.")
  }

//  after {
//    spark.stop()
//  }
}
