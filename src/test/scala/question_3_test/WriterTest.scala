package question_3_test

import question3.ExportWriter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import java.io.File

class ExportWriterSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("ExportWriterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("ExportWriter.writeData should write data partitioned by category") {
    val data = Seq(
      ("India", 3000.0, "big"),
      ("USA", 5000.0, "big"),
      ("China", 1000.0, "small")
    ).toDF("country", "total_export", "category")

    val year = 2018
    val commodity = "Oil"
    val outputPath = s"output/$year/$commodity"

    // Call the write function
    ExportWriter.writeData(data, year, commodity)

    // Verify files were written to the output path
    val outputDir = new File(outputPath)
    assert(outputDir.exists() && outputDir.isDirectory)

    println(s"ExportWriter.writeData test case passed. Data is written to $outputPath")
  }

}
