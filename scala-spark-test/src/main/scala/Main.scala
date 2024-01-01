package com.daiyadeguchi

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

// Took me a few minutes running main
// The reason was I needed to add "Include dependencies with provided scope" option to run config
object Main {
  def main(args: Array[String]): Unit = {
    // appName and master is required argument
    val spark = SparkSession.builder().appName("scala-spark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    /*
    // Basic show methods
    // show() shows 20 lines or so of input file
    df.show()
    df.printSchema()

    // A few ways to define columns
    df.select("Date", "Open", "Close").show()
    val column = df("Date")
    col("Date")
    import spark.implicits._
    $"Date"

    df.select(col("Date"), $"Open", df("Close")).show()
    */

    val column = df("Open")
    // as() changes the name of the column for better understandability
    val newColumn = (column + (2.0)).as("OpenIncreasedBy2")
    val columnString = (column.cast(StringType)).as("OpenAsString")

    val newColumnString = functions.concat(columnString, lit("Hello world")).as("OpenAsStringWithConcat")

    df.select(column, newColumn, columnString, newColumnString)
      .filter(newColumn > 2.0)
      .filter(newColumn > column)
      // == for object equality, === for value equality
      //.filter(newColumn === column)
      .show(truncate=false)


  }
}