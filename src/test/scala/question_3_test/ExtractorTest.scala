package question_3_test
import question3.ExportExtractor

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class ExportExtractorSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("ExportExtractorTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("ExportExtractor.extractData should filter by year and commodity") {
    val data = Seq(
      ("India", 2018, "Oil", 1000),
      ("India", 2018, "Electronics", 5000),
      ("India", 2017, "Oil", 2000)
    ).toDF("country", "year", "Commodity", "value")

    val year = 2018
    val commodity = "Oil"
    val filePath = "test_data2.csv"
    data.write.mode("overwrite").option("header", "true").csv(filePath)
    val result = ExportExtractor.extractData(spark,filePath, year, commodity)

    // Verify the filtering
    assert(result.filter(col("Commodity") === "Oil").count() == 1)
    assert(result.filter(col("year") === 2018).count() == 1)
    assert(result.schema("value").dataType == DoubleType)

    println("ExportExtractor.extractData test case passed.")
  }


}
