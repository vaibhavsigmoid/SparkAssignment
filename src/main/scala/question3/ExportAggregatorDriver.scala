package question3

import org.apache.spark.sql.SparkSession

object ExportAggregatorDriver {
  def main(args: Array[String]): Unit = {
    //    val filePath = if (args.length > 0) args(0) else {
    //      println("Enter the path to the data file:")
    //      scala.io.StdIn.readLine()
    //    }
    val filePath = "/Users/vaibhavgupta/Downloads/india-trade-data/2018-2010_export.csv"

    println("Enter the year you want to process (e.g., 2018):")
    val year = scala.io.StdIn.readInt()

    println("Enter the commodity to process:")
    val commodity = scala.io.StdIn.readLine()

    val spark: SparkSession = SparkSession.builder
      .appName("ExportAggregatorDriver")
      .master("local[*]")
      .getOrCreate()

    try {
      val extractedData = ExportExtractor.extractData(spark,filePath, year, commodity)
      val aggregatedData = ExportAggregator.aggregateData(extractedData)
      val categorizedData = ExportAggregator.categorizeExporters(aggregatedData)

      ExportWriter.writeData(categorizedData, year, commodity)
    } finally {
      spark.stop()
    }
  }
}

