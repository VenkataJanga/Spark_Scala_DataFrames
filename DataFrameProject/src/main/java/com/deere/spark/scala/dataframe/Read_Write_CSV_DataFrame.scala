package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

object Read_Write_CSV_DataFrame {
  
  def main(args:Array[String]){
    
    val sparkSession = SparkSession.builder().appName("Read_Write_CSV_DataFrame").master("local[*]").getOrCreate()
    var file_path = "C:/SparkScala/DataFrame/Data/zipcodes.csv"
    
    val zipcode_df = sparkSession.read.options(Map("head" -> "true","inferSchema"-> "true","delimiter"->",")).csv(file_path)
    
    zipcode_df.printSchema()
    zipcode_df.show(false)
    
    zipcode_df.write.option("header", true).csv("C:/SparkScala/DataFrame/spark_output/zip_codes.csv")
    
    
  }
  
}