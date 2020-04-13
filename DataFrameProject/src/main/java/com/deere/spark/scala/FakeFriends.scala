package com.deere.spark.scala.basics.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object FakeFriends {
  
  def parseLine(line:String) ={
      val fields = line.split(",")
      val age = fields(2).toInt
      val numberOfFriends =  fields(3).toInt
      (age,numberOfFriends)
    }
  
  def main(args:Array[String]){
    val sc = new SparkContext("local","FakeFriends");
    var file_path = "C:/SparkScala/DataFrame/Data/"
    val lines = sc.textFile(file_path+"countFriends.csv")
    val rdd = lines.map(parseLine)
    rdd.foreach(println)
    
    val totalByAge = rdd.mapValues(f => (f,1)).reduceByKey((x,y)=> (x._1 + y._1 , x._2 + y._2))
    totalByAge.foreach(println)
    
    val keys = totalByAge.keys
    
     val values = totalByAge.values
    println("KEYS...................")
    keys.foreach(println)
    println("VALUES...................")
    values.foreach(println)
    
  }
}