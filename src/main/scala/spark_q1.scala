import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object spark_q1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MaxExportCommodity <file_path> <year> <country>")
      sys.exit(1)
    }

    val filePath = args(0)
    val year = args(1)
    val country = args(2)

    val spark = SparkSession.builder
      .appName("MaxExportCommodity")
      .master("local[*]") // Use local mode for testing
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val filteredData = data
      .filter(col("year") === year && col("country") === country)
      .withColumn("value", col("value").cast("double"))

    val maxCommodity = filteredData
      .orderBy(desc("value"))
      .select("Commodity", "value")
      .first()

    if (maxCommodity != null) {
      println(s"The commodity exported the most by India to $country in $year was: ${maxCommodity.getString(0)} with a value of ${maxCommodity.getDouble(1)}")
    } else {
      println(s"No export data found for $country in $year.")
    }

    spark.stop()
  }

  // Call main here with example arguments for testing
  main(Array("/Users/vaibhavgupta/Downloads/india-trade-data/2018-2010_export.csv", "2018", "AFGHANISTAN TIS"))
}
