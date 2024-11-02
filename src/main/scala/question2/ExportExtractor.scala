package question2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExportExtractor {
  def extractData(spark: SparkSession,filePath: String, year: Int): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .filter(col("year") === year) // Filter data for the specified year
      .withColumn("value", col("value").cast("double")) // Convert value to Double
  }
}

