package com.deere.spark.scala.basics.rdd

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CricketAnalysis{
   def main(args:Array[String]){
      val sc = new SparkContext("local","CricketAnalysis");
      val data = sc.textFile("C:/SparkScala/DataFrame/Data/Cricket_Batsman_Analysis.csv");
      val df = data.map((x => x.split("\t")))
      val df1 = df.filter(f=>f(9) == "Test")
      val df2 = df1.map{f => (f(1).split("#")(1),1)}
      val df3 = df2.reduceByKey(_ + _)
      val df4 = df3.sortBy(_._2, false)
      df4.foreach(println)
      
      val rdd = data.map(f => f.split("\t")).filter(f => f(9)=="Test").map{f=>(f(1).split("#")(1),1)}.reduceByKey(_+_).sortBy(_._2, false)
  
      rdd.foreach(println)
      rdd.keys.foreach(println)
      rdd.values.foreach(println)
   }
  
  
  
  
}