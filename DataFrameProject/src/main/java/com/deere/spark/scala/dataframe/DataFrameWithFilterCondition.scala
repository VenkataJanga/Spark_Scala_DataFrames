package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.map_from_arrays

object DataFrameWithFilterCondition extends App {
   val spark:SparkSession =SparkSession.builder().appName("CreateEmptyDataSet").master("local[*]").getOrCreate()
   import spark.implicits._
  
   val arrayData = Seq(
                        Row(Row("Venkata ","","Janga"),"1","M",3100,List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
                        Row(Row("Rod ","Rose","Williams"),"2","M",4100,List("Tennis"),Map("hair"->"brown","eye"->"black")),
                        Row(Row("Quentin ","","Amos"),"3","M",8100,List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
                        Row(Row("Vijay ","","Devarajan"),"4","M",7100,null,Map("hair"->"blond","eye"->"red")),
                        Row(Row("James","Anne",""),"5","F",6100,List("Blogging"),Map("white"->"black","eye"->"black"))
                     )
                     
  val schema = new StructType()
                               .add("name",new StructType()
                                                           .add("first_name",StringType)
                                                           .add("middle_name",StringType)
                                                           .add("last_name",StringType)
                                   )
                                   .add("id",StringType)
                                   .add("gender",StringType)
                                   .add("salary",IntegerType)
                                   .add("hobbies",ArrayType(StringType))
                                   .add("prperties",MapType(StringType,StringType))
                                   
                                   
  val createDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), schema)
  
  createDF.printSchema()
  createDF.show()
  
  //filter Condition
  createDF.filter(createDF("gender") === "M").show(false)
  
   //Struct condition
  createDF.filter(createDF("name.lastname") === "Janga").show(false)
  
  //Array condition
  createDF.filter(array_contains(createDF("hobbies"),"Movies")).show(false)
  
  //Map condition
  //createDF.filter(map_from_arrays(createDF("prperties"),"hair")).show(false)
  
  //Multiple condition
  //createDF.filter(createDF("gender" == "M" && "salary" =<= 4300)
  
}