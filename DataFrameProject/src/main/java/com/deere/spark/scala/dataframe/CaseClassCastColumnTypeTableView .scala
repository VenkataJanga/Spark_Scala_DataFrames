package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType

object CaseClassCastColumnTypeTableView extends App {
  
   val spark:SparkSession =SparkSession.builder().appName("CaseClassCastColumnTypeTableView").master("local[*]").getOrCreate()
   
   val data = Seq(
                   Row("Venkata",41,"2020-01-01","true","M",3000.60),
                   Row("SreeRam",14,"2016-01-01","true","M",3500.60),
                   Row("Sai",11,"2019-01-01","true","M",4000.60),
                   Row("Karuna",38,"2018-01-01","true","F",5000.60),
                   Row("Ajay",34,"2017-01","true","M",6000.60),
                   Row("Vijay",24,"2009-01-01","true","M",8000.60),
                   Row("Vidya",29,"2008-01-01","true","F",9000.60),
                 )
                 
    val schema = StructType(
                      Array(
                          StructField("Name",StringType,true),
                          StructField("Age",IntegerType,true),
                          StructField("DataOfJoining",StringType,true),
                          StructField("isGraduate",StringType,true),
                          StructField("Gender",StringType,true),
                          StructField("Salary",DoubleType,true)
                          )
                        )
   val createDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
   createDataFrame.printSchema()
   createDataFrame.show()
   
   
   
   val sqlQuery = createDataFrame.selectExpr("cast(Age as int) Age",
                                             "cast(DataOfJoining as String) as jobStartDate",
                                             "cast(isGraduate as String) as isGraduate")
  sqlQuery.printSchema()
  sqlQuery.show()
  
  
  
  sqlQuery.createOrReplaceTempView("EmployeTable")
  val query = spark.sql("select STRING(Age),BOOLEAN(isGraduate),DATE(jobStartDate) from EmployeTable")
  query.printSchema()
  query.show()
                    
  
  val query1 = spark.sql("select STRING(Age),BOOLEAN(isGraduate),DATE(jobStartDate) from EmployeTable where Age > 40")
  query1.printSchema()
  query1.show()
  
}