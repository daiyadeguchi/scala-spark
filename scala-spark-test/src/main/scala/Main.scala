package com.daiyadeguchi

import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, year}
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

    // Rename each column with camelcase
    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    // _* -> syntax to make list to varargs
    // withColumn creates a new column with the second parameter
    // The assignment: add a column diff bw open and close
    //                 filter to day when the close price was more than 10% higher than the open price
    val stockData = df.select(renameColumns: _*)

    /*
    // Basic show methods
    // show() shows 20 lines or so of input file
    df.show()
    df.printSchema()

    // A few ways to define columns
    df.select("Date", "Open", "Close").show()
    val column = df("Date")
    col("Date")
    // need to import implicits for $ annotation
    import spark.implicits._
    $"Date"

    df.select(col("Date"), $"Open", df("Close")).show()

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

    // Compile time safety
    // Not recommended because you could typo
    val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampExpression")
    // recommended because no typo
    val timestampFromFunction = current_timestamp().cast(StringType).as("timestampFunction")

    df.select(timestampFromExpression, timestampFromFunction).show()

    // selectExpr directly
    df.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp()").show()

    // SQL from df
    // need to create the view to use it in sql
    df.createTempView("df")
    spark.sql("select * from df").show()
      .withColumn("diff", col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1)
    stockData.show()

    // lowercase
    // df.select(df.columns.map(c => col(c).as(c.toLowerCase())): _*).show()
    */

    // either use agg or sorting function such as max directly for sorting
    import spark.implicits._
    stockData
      .groupBy(year($"Date").as("year"))
      .agg(functions.max($"close").as("maxClose"), functions.avg($"close").as("avgClose"))
      .sort($"year".desc)
      .show()

    stockData
      .groupBy(year($"date").as("year"))
      .max("close", "high")
      .show()

  }
}