package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

object ParquetToJSON {
  def main(args:Array[String]){
    val sparkSession= SparkSession.builder().appName("ParquetToJSON").master("local[*]").getOrCreate()
    val file_path = "C:/SparkScala/DataFrame/data/zipcodes.parquet"
    
    val parDf = sparkSession.read.parquet(file_path)
    parDf.printSchema()
    parDf.show(false)
    
    
    
    //converting into JSON
    
    parDf.write.format("json").json("C:/SparkScala/DataFrame/spark_output/zipcodes11.json")
    
  }
}