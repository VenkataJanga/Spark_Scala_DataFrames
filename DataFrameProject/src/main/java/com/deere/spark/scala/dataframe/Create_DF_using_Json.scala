package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession

object Create_DF_using_Json {
  
  def main(args:Array[String]){
    
    val file_path =  "C:/SparkScala/DataFrame/"
    
    
    val sparkSession = SparkSession.builder().appName("Create_DF_using_Json").master("local[*]").getOrCreate()
    
    //val df_single = sparkSession.read.option("header", true).option("inferSchema", true).csv(file_path+"employees_singleLine.json")
    
   // println(df_single.printSchema())
   // df_single.show()
    
    val df_multiple = sparkSession.read.option("multiLine", true).option("mode", "PERMISSIVE").csv(file_path+"employees_multiLine.json")
    println(df_multiple.printSchema())
    df_multiple.show()
    
    
   // df_single.except(df_multiple).show()
    //df_single.show()
    //df_single.select("deptno").distinct.show()
    // we can create a temp table and execute the query on the table
    //Register a table
    //df_single.registerTempTable("employeeTbl")
    //df_single.createTempView("employeeTbl")
    
  // val data =  sparkSession.sqlContext.sql("select distinct deptno from employeeTb1")
   //println(data)
  }
  
}