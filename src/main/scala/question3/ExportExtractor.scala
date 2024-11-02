package question3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExportExtractor {
  def extractData(spark:SparkSession,filePath: String, year: Int, commodity: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath).filter(col("year") === year && col("Commodity") === commodity)
      .withColumn("value", col("value").cast("double"))
  }
}
