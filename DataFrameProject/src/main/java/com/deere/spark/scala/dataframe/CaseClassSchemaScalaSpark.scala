package com.deere.spark.scala.dataframe

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType



object CaseClassSchemaScalaSpark extends App {
  
  case class Name(first_name:String,last_name:String,age:Integer)
  
  case class Employee(fullName:String, age:Integer,gender:String)
  
  val encoderSchema = Encoders.product[Employee].schema
  encoderSchema.printTreeString()
  
  import org.apache.spark.sql.catalyst.ScalaReflection
  
  val schema =ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]
  schema.printTreeString()
  
  
  val encoderSchemaNames = Encoders.product[Name].schema
  encoderSchemaNames.printTreeString()
  
  val schemaName = ScalaReflection.schemaFor[Name].dataType.asInstanceOf[StructType]
  
  schemaName.foreach(println)
  
}