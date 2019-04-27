package com.akshitbhatia.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_Structured_Streaming extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Logger ")


  def filter_data(filter_dt: DataFrame): StreamingQuery = {
    log.warn("\nWriting data to file initializing...\n")
    filter_dt
      .writeStream
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", "/home/akshit/Desktop/checkpoint_filter_data")
      .option("path", "/home/akshit/Desktop/filter_data_parquet")
      .start()
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark_Streaming")
      .getOrCreate()
    log.warn("\nSpark Session Started..\n")

    import spark.implicits._

    val inputstream = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    log.warn("\nNetcat Running..\n")


    val raw_data = inputstream.as[String].flatMap(x => x.split(" ")) //DS
    log.warn("\nInput Streaming..\n")

    val columns = raw_data.withColumn("raw_data", split($"value", "\\;")) //DF
      .select($"raw_data".getItem(0).as("Name"),
      $"raw_data".getItem(1).as("Age"),
      $"raw_data".getItem(2).as("Gender"),
      $"raw_data".getItem(3).as("Company"),
      $"raw_data".getItem(4).as("Year")).drop("raw_data")

    val df_datatypes = columns.selectExpr("cast(Name as String) Name", "cast(Age as int) Age", "cast(Gender as String) Gender", "cast(Company as String)", "cast(Year as int) Year").toDF("Name", "Age", "Gender", "Company", "Year")

    val final_data = df_datatypes.select((when($"Name" === "", null).otherwise($"Name")).alias("Name"),
      (when($"Age" === "", null).otherwise($"Age")).alias("Age"),
      (when($"Gender" === "", null).otherwise($"Gender")).alias("Gender"),
      (when($"Company" === "", null).otherwise($"Company")).alias("Company"),
      (when($"Year" === "", null).otherwise($"Year")).alias("Year"))


    val filter_df = final_data
      .withColumn("time", current_timestamp())
      .filter("Age is not null")
      .filter($"Age" < 99)
      .filter("Name is not null")


    val window_df= filter_df
      .withWatermark("time", "1 minutes")
      .groupBy(window($"time", "2 minutes", "1 minutes"),$"Name",$"Age",$"Gender",$"Company",$"Year")
      .count()
      .select($"window",$"Name",$"Age",$"Gender",$"Company",$"Year")


    val query = filter_data(window_df)
    log.warn("\nQuery Done: Wait for Termination.\n")

    query.awaitTermination()

  }

}


