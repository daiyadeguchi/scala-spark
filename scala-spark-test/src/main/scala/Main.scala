package com.daiyadeguchi

import org.apache.spark.sql.SparkSession

// Took me a few minutes running main
// The reason was I needed to add "Include dependencies with provided scope" option to run config
object Main {
  def main(args: Array[String]): Unit = {
    // appName and master is required argument
    val spark = SparkSession.builder().appName("scala-spark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("data/AAPL.csv")

    // show() shows 20 lines or so of input file
    df.show()
  }
}