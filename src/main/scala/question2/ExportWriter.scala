package question2

import org.apache.spark.sql.DataFrame

object ExportWriter {
  def writeData(data: DataFrame, year: Int): Unit = {
    data.write
      .mode("overwrite")
      .partitionBy("Commodity")
      .csv(s"output/$year") // Write to output/year directory

    println(s"Export ranking data has been written to output/$year directory.")
  }
}

