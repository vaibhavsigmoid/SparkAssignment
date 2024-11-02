package question3

import org.apache.spark.sql.DataFrame

object ExportWriter {
  def writeData(data: DataFrame, year: Int, commodity: String): Unit = {
    data.write
      .mode("overwrite")
      .partitionBy("category")
      .csv(s"output/$year/$commodity")

    println(s"Data has been written to output/$year/$commodity directory.")
  }
}
