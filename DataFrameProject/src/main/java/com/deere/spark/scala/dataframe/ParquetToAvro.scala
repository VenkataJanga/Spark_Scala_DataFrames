package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object ParquetToAvro {
  def main(args:Array[String]){
    val sparkSession= SparkSession.builder().appName("ParquetToJSON").master("local[*]").getOrCreate()
    val file_path = "C:/SparkScala/DataFrame/data/zipcodes.parquet"
    
    val parDf = sparkSession.read.parquet(file_path)
    parDf.printSchema()
    parDf.show(false)
    
    //parDf.write.format("avro").save("C:/SparkScala/DataFrame/spark_output/zipcodes11.avro")
    parDf.write.format("avro")
    .mode(SaveMode.Overwrite)
   .save("C:/SparkScala/DataFrame/spark_output/zipcodes11.avro")
 // parDf.select("RecordNumber", "Zipcode","ZipCodeType","City","State","LocationType","Lat","Long","Xaxis","Yaxis","WorldRegion","Country","LocationText","Location","Decommisioned","TaxReturnsFiled","EstimatedPopulation","TotalWages").write.format("avro").save("zipcodes.avro")
   
    /**
      * Write Avro Partition
      */
    parDf.write.partitionBy("RecordNumber","Zipcode","ZipCodeType")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(file_path+"zipcodes_partition.avro")
      
    import sparkSession.sqlContext.implicits._
   /**
      * Reading Avro Partition
      */
    sparkSession.read
      .format("avro")
      .load(file_path+"zipcodes_partition.avro")
      .where(col("RecordNumber") >= 48000)
      .show()
  }
}