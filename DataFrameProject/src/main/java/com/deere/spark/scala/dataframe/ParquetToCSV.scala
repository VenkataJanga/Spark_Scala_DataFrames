package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

object ParquetToCSV {
  
  def main(args:Array[String]){
    val sparkSession= SparkSession.builder().appName("ParquetToCSV").master("local[*]").getOrCreate()
    val file_path = "C:/SparkScala/DataFrame/data/zipcodes.parquet"
   // val parDf = sparkSession.read.parquet(file_path)
    val parDf = sparkSession.read.format("parquet").load(file_path)
    parDf.printSchema()
    parDf.show(false)
    
    
    //convert parquet file into CSV file
    parDf.write.option("header", true).format("csv").csv("C:/SparkScala/DataFrame/spark_output/zipcodes1.csv")
    
  }
}