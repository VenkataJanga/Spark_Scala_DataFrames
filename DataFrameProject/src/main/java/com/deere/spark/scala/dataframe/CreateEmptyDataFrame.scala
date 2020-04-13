package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

object CreateEmptyDataFrame extends App{
  
   val spark:SparkSession =SparkSession.builder().appName("CreateEmptyDataFrame").master("local[*]").getOrCreate()
   
    case class Name(firstName: String, lastName: String, middleName:String)
    val schema = StructType(
                             StructField("firstName", StringType, true) ::
                             StructField("lastName", IntegerType, false) ::
                             StructField("middleName", IntegerType, false) :: Nil
                          )
    val cols = Seq("firstName","lastName","middleName")
    
    val createDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    import spark.implicits._
    
    Seq.empty[(String,String,String)].toDF(cols:_*)
    
    Seq.empty[Name].toDF().printSchema()
    
    
    

  
}