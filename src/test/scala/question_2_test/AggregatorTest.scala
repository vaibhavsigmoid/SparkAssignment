package question_2_test

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import question2.ExportAggregator

class ExportAggregatorTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("ExportAggregatorTest")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  test("aggregateData should aggregate exports by commodity and country and rank them") {
    // Mock data
    val data = Seq(
      ("Commodity1", "Country1", 500.0),
      ("Commodity1", "Country2", 1500.0),
      ("Commodity1", "Country3", 700.0),
      ("Commodity2", "Country1", 1000.0)
    ).toDF("Commodity", "country", "value")

    val result: DataFrame = ExportAggregator.aggregateData(data)

    // Verify that the data has the correct ranks
    val commodity1Ranks = result.filter(col("Commodity") === "Commodity1").orderBy("rank").collect()
    assert(commodity1Ranks(0).getAs[Int]("rank") == 1)
    assert(commodity1Ranks(1).getAs[Int]("rank") == 2)
    assert(commodity1Ranks(2).getAs[Int]("rank") == 3)

    val commodity2Ranks = result.filter($"Commodity" === "Commodity2").collect()
    assert(commodity2Ranks(0).getAs[Int]("rank") == 1)
    println("Test 'extractData should filter data by year and convert value column to Double' passed.")
  }
}
