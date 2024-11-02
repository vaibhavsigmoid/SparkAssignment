package question_2_test

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import question2.ExportExtractor

class ExportExtractorTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("ExportExtractorTest")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  test("extractData should filter data by year and convert value column to Double") {
    // Mock data
    val data = Seq(
      ("Commodity1", "Country1", 2018, "1000"),
      ("Commodity2", "Country2", 2019, "2000"),
      ("Commodity3", "Country3", 2018, "1500")
    ).toDF("Commodity", "country", "year", "value")

    // Write mock data to a temporary file for testing
    val filePath = "test_data1.csv"
    data.write.mode("overwrite").option("header", "true").csv(filePath)

    val result: DataFrame = ExportExtractor.extractData(spark, filePath, 2018)

    assert(result.count() == 2)
    assert(result.filter(col("year") === 2018).count() == 2)
    assert(result.schema("value").dataType.simpleString == "double")

    println("Test 'extractData should filter data by year and convert value column to Double' passed.")
  }
}

