package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CreateDataFrame  {
  def main(args:Array[String]){
    
    val sparkSession = SparkSession.builder()
    .appName("CreateDataFrame using SparkSesion")
    .master("local")
    .getOrCreate()
    
    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = sparkSession.sparkContext.parallelize(data)
    
    

    //From Data (USING createDataFrame)
    var dfFromData2 = sparkSession.createDataFrame(data).toDF(columns:_*)
    
    println(dfFromData2.show(2))
    
    val schema = StructType(columns
      .map(fieldName => StructField(fieldName, StringType, nullable = true)))

    
    
    //From Data (USING createDataFrame and Adding schema using StructType)
    import scala.collection.JavaConversions._
    val rowData = data
      .map(attributes => Row(attributes._1, attributes._2))
      
      
    var dfFromData3 = sparkSession.createDataFrame(rowData,schema)
    println(dfFromData3.show(2))
  }
}