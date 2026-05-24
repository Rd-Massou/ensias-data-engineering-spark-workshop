package com.ensias.batch

import com.ensias.SparkApp
import com.ensias.models._
import com.ensias.parsers.FlightBatchConfigParser
import com.ensias.transformations.FlightLogicProcessing
import com.ensias.utils.SparkIOUtils
import org.apache.spark.sql.SparkSession

object LiveExample extends SparkApp {

  override def appName: String = "example-flight-batch-job"

  override def run(args: Array[String]): Unit = {
    import spark.implicits._

    val myDataframe = spark.read
        .format("csv")
        .option("header", true)
        .load("src/test/resources/inputs/salesExampleData")
    
    val filteredDataframe = myDataframe
        .select(
            "order_id",
            "product_name",
            "quantity",
            "unit_price",
            "currency",
            "discount_pct",
            "tax_pct",
            "returned"
        ).withColumn("total_price", $"quantity" * $"unit_price")
        .filter($"total_price" > 2000)

    logger.info(s"Result of my spark read ${filteredDataframe.show(10)}")

    filteredDataframe
        .write
        .format("json")
        .save("target/test-output/LiveResult")
  }
}