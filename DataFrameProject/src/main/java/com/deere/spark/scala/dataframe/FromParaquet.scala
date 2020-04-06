package com.deere.spark.scala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object FromParaquet {
  
  def main(args:Array[String]){
   val sparkSession:SparkSession = SparkSession.builder().appName("FromParaquet").master("local[*]").getOrCreate()
   
    val data = Seq(("James ","","Smith","36636","M",3000),
      ("Michael ","Rose","","40288","M",4000),
      ("Robert ","","Williams","42114","M",4000),
      ("Maria ","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1)
    )

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    
    import sparkSession.implicits._
    val df:DataFrame = data.toDF(columns:_*)
    
    df.printSchema()
    df.show(false)
    
    //Writing the paraquet file and saving the specified folder
    val file_path = "C:/SparkScala/DataFrame/parquet_output/people.paraquet"
    df.write.parquet(file_path)
    
    val paraDf = sparkSession.read.parquet(file_path)
    paraDf.createOrReplaceTempView("ParquetTable")

    sparkSession.sql("select * from ParquetTable where salary >= 4000").explain()
    val parquetSql = sparkSession.sql("select * from ParquetTable where salary >= 4000 ")
    parquetSql.printSchema()
    parquetSql.show()
    
    
    df.write.partitionBy("gender","salary").parquet("C:/SparkScala/DataFrame/parquet_output/people2.paraquet")
    
    val parDf = sparkSession.read.parquet("C:/SparkScala/DataFrame/parquet_output/people2.paraquet")
    parDf.createOrReplaceTempView("paraquetTable1")
    
    val parDf1 = sparkSession.sql("select * from paraquetTable1 where gender ='M' and salary == 3000")
    parDf1.printSchema()
    parDf1.show(false)
    
  }
    
}