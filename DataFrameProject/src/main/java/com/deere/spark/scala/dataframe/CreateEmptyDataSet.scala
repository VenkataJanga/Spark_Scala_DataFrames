package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CreateEmptyDataSet extends App {
  
  val spark:SparkSession =SparkSession.builder().appName("CreateEmptyDataSet").master("local[*]").getOrCreate()
   
  
    import spark.implicits._
    case class Name(firstName: String, middleName:String,lastName: String)
   
    val schema = StructType(
                      StructField("firstName",StringType,true)::
                      StructField("middleName",StringType,true)::
                      StructField("lastName",StringType,true)::Nil
                      )
    val cols = Seq("firstName","middleName","lastName")
    
    spark.createDataset(Seq.empty[Name])
    spark.createDataset(Seq.empty[(String,String,String)])
    spark.createDataset(spark.sparkContext.emptyRDD[Name])
    
    Seq.empty[Name].toDS()
    Seq.empty[(String,String,String)].toDS()
    spark.emptyDataset[Name]
   
}