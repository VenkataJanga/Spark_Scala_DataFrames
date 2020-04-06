package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

object Create_DataFrame_from_Tuples {
    
  def main(args: Array[String]){
   
    val sparkSession = SparkSession.builder().appName("Create_DataFrame_from_Tuples using toDF").master("local").getOrCreate()
    
    val touple_data = Seq(
                          ("Veg Pizaa","2.50"),
                          ("Veg Pizaa with Conrn","3.0"),
                          ("Veg Pizaa with Kazuandcorn","3.50"),
                          ("Non-Veg Pizaa","4.50"),
                          ("Non-Veg Pizaa with Conrn","5.0"),
                          ("Non-Veg Pizaa with Kazuandcorn","6.50")
                          )
    val toupleDF = sparkSession.createDataFrame(touple_data).toDF("Pizza Name","Price")
    
    
    println(toupleDF.printSchema())
    println(toupleDF.show())
    
    val columns = toupleDF.columns;
    
    columns.foreach(println)
    
    
    //DataFrame column names and types
    
    val (columnNames, columnDataTypes) = toupleDF.dtypes.unzip
    
    
    println(s" Data column Name are :${columnNames.mkString(",")}")
    println(s"Data for Columns Data types are : ${columnDataTypes.mkString(",")} ")
    
    
  }
}