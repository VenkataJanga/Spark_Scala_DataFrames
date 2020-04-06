package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame;

object FromTextFile {
  def main(args:Array[String]){
    
    val sparkSession = SparkSession.builder().appName("FromTextFile").master("local[*]").getOrCreate()
    var file_path = "C:/SparkScala/DataFrame/Data/text1.txt"
    
    val df:DataFrame = sparkSession.read.text(file_path)
    df.printSchema()
    df.show(false)
    
    //converting to columns by Split
     import sparkSession.implicits._
    
    val df2 = df.map(f=>{
          val elements = f.getString(0).split(",")
           (elements(0),elements(1))
          })
          
          
    df2.printSchema()
    df2.show(false)
    }
    
}