package question2
import org.apache.spark.sql.SparkSession

object ExportAggregatorDriver extends  App {

    // Get inputs from the user
//    val filePath = if (args.length > 0) args(0) else {
//      println("Enter the path to the data file:")
//      scala.io.StdIn.readLine()
//    }
    val filePath = "/Users/vaibhavgupta/Downloads/india-trade-data/2018-2010_export.csv"

    println("Enter the year you want to process:")
    val year = scala.io.StdIn.readInt()
//      val year = 2018

    val spark = SparkSession.builder
      .appName("ExportAggregatorDriver")
      .master("local[*]") // Use local mode for testing
      .getOrCreate()
    import spark.implicits._


      // Step 1: Extract data
      val extractedData = ExportExtractor.extractData(spark,filePath, year)
      // Step 2: Aggregate data
      val aggregatedData = ExportAggregator.aggregateData(extractedData)

      // Step 3: Write the output
      ExportWriter.writeData(aggregatedData, year)

    spark.stop()
  }


