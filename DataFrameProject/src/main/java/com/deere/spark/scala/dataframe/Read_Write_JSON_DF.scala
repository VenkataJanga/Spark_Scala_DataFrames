package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SaveMode

object Read_Write_JSON_DF {
  
  def main(args:Array[String]){
    
    val sparkSession = SparkSession.builder().appName("Read_Write_JSON_DF").master("local[*]").getOrCreate()
    var file_path = "C:/SparkScala/DataFrame/Data/"
    
    val zipcode_json_df = sparkSession.read.option("multiline", "true")
                          .format("json").load(file_path+"zipcode1.json")
    zipcode_json_df.printSchema()
    zipcode_json_df.show(false)
    
    //read multiple files
    val df2 = sparkSession.read.option("multiline", "true").
            json(file_path+"zipcode1.json",file_path+"zipcode2.json")
    df2.show(false)
    
    
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",IntegerType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",StringType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)
    val df_with_schema = sparkSession.read.schema(schema)
        .json(file_path+"zipcode1.json",file_path+"zipcode2.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)
    
    //Read JSON file using Spark SQL
  sparkSession.sqlContext.sql("CREATE TEMPORARY VIEW zipcode using json OPTIONS"+
        " (path 'C:/SparkScala/DataFrame/Data/zipcode1.json')")
    sparkSession.sqlContext.sql("SELECT * FROM zipcode").show()

  //Write Spark DataFrame to JSON file
    //df_with_schema.write.json("C:/SparkScala/DataFrame/spark_output/zip_codes.json")
    
    //Saving modes
    df_with_schema.write.mode(SaveMode.Overwrite).json("C:/SparkScala/DataFrame/spark_output/zip_codes.json")
    
  }
}