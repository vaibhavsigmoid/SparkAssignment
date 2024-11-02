package question3

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExportAggregator {
  def categorizeExporters(data: DataFrame): DataFrame = {
    val sortedData = data.sort(col("total_export").desc)

    // Calculate size-based thresholds
    val totalRows = sortedData.count().toDouble
    val bigThresholdIndex = Math.ceil(totalRows * 0.75).toInt
    val smallThresholdIndex = Math.ceil(totalRows * 0.25).toInt

    val bigThreshold = sortedData.collect()(bigThresholdIndex - 1).getAs[Double]("total_export")
    val smallThreshold = sortedData.collect()(smallThresholdIndex - 1).getAs[Double]("total_export")

    sortedData.withColumn("category", when(col("total_export") >= bigThreshold, "big")
      .when(col("total_export") >= smallThreshold, "medium")
      .otherwise("small"))
  }

  def aggregateData(data: DataFrame): DataFrame = {
    data.groupBy("country")
      .agg(sum("value").as("total_export"))
  }
}
