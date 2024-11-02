package question_2_test
import question2.ExportWriter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.DataFrame
import java.io.File

class ExportWriterTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("ExportWriterTest")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  test("writeData should write the data to the output directory partitioned by Commodity") {
    // Mock data
    val data = Seq(
      ("Commodity1", "Country1", 1),
      ("Commodity1", "Country2", 2),
      ("Commodity2", "Country1", 1)
    ).toDF("Commodity", "country", "rank")

    val year = 2018
    val outputPath = s"output/$year"

    // Clean up output directory before running the test
    new File(outputPath).delete()

    ExportWriter.writeData(data, year)

    // Verify that files are created in the correct partition
    assert(new File(s"$outputPath/Commodity=Commodity1").exists())
    assert(new File(s"$outputPath/Commodity=Commodity2").exists())

    println("Test 'writeData should write the data to the output directory partitioned by Commodity' passed.")
  }
}
