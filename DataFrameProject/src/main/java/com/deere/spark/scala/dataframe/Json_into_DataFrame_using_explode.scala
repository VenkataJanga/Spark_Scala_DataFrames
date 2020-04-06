package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.columnar.compression.Encoder
import org.apache.spark.sql.execution.columnar.compression.Encoder
import org.apache.spark.sql.execution.columnar.compression.Encoder
import org.apache.spark.sql.execution.columnar.compression.Encoder

object Json_into_DataFrame_using_explode {
  
  def main(args:Array[String]){
    val sparkSession = SparkSession.builder().appName("Json_into_DataFrame_using_explode").master("local").getOrCreate()
    
    import sparkSession.implicits._
    
    
    val filePath = "C:/SparkScala/DataFram/Projects/PricePredection/ProgramingFrameWorkWithAuthor.json"
  /*  val df = sparkSession.read.format("json")
             option("header", true).
             option("inferSchema", "true").
              json("filePath")
  */
    
      val df = sparkSession.read.format("json").
             option("header", true).
             option("inferSchema", "true").
             json("filePath")
  

   
  }
}