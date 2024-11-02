package question2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions => F}

object ExportAggregator {
  def aggregateData(data: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Commodity").orderBy(F.desc("total_export"))

    data.groupBy("Commodity", "country")
      .agg(F.sum("value").as("total_export"))
      .withColumn("rank", F.row_number().over(windowSpec))
      .select("Commodity", "country", "rank") // Select relevant columns
  }
}
