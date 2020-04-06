package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType

object FromArrayToColumn  {
  
   def main(args:Array[String]){
    
    val sparkSession = SparkSession.builder()
    .appName("WithCass using SparkSesion")
    .master("local")
    .getOrCreate()
    
    val arrData = Seq(
                      Row("James",List("Java","C++","Scala")),
                      Row("Michael",List("Spark","Java","C++")),
                      Row("Robert",List("CSharp","VB",""))
                    )
    val arraySchema = new StructType()
                      .add("name",StringType)
                      .add("subjects",ArrayType(StringType))
  

  val arrayDF = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(arrData),arraySchema)
  arrayDF.printSchema()
  arrayDF.show()

  
  val arrayDFColumn = arrayDF.select(
    arrayDF("name") +: (0 until 2).map(i => arrayDF("subjects")(i).alias(s"LanguagesKnown$i")): _*
  )
  arrayDFColumn.show(false)
  
  
  //How to convert Array of Array to column
  val arrayArrayData = Seq(
                          Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
                          Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
                          Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
                        )
                        
  val arrayArraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(ArrayType(StringType)))

  val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  df.printSchema()
  df.show()

  val df2 = df.select(
      df("name") +: (0 until 2).map(i => df("subjects")(i).alias(s"LanguagesKnown$i")): _*
    )

  df2.show(false)
   }
}