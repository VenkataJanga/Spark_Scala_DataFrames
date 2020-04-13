package com.deere.spark.scala.basics.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object StringToData {
 
  def main(args:Array[String]){
    val sparkkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  sparkkSession.sparkContext.setLogLevel("ERROR")

  import sparkkSession.sqlContext.implicits._
  
  Seq(
      ("18000128"),("18000130")).toDF("Date").select(
      col("Date"),
      to_date(col("Date"),"yyyy-mm-dd").as("to_date")
    ).show()
  
  }
}