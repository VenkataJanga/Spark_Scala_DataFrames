package com.deere.spark.scala.basics.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.min

object MinimumTemparature {
  def parseLine(lines:String)={
    val fields = lines.split(",")
    val id = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat*0.1f*(9.0f/5.0f)+32.0f
     
   (id,entryType,temperature)
  }
  def main(args:Array[String]){
    
    //val sparkSession = SparkSession.builder().appName("MinimumTemparature").master("local[*]").getOrCreate()
    val sc = new SparkContext("local[*]","MinimumTemparature")
    var lines = sc.textFile("C:/SparkScala/Data/SparkScala/SparkScala/1800.csv")// get the data from local file system
    val parsedLines = lines.map(parseLine) // call the parsLine function and returns id,entryType,temperature
    
    val minTemperature = parsedLines.filter(f=> f._2=="TMIN")// get the all minimum temperature values
    
  //  minTemperature.foreach(println)
    
    val stationTemp = minTemperature.map(x=> (x._1,x._3.toFloat))
    stationTemp.foreach(println)
    
    val minTemperatureBySationTemp = stationTemp.reduceByKey((x,y) => min(x,y))
    
     minTemperatureBySationTemp.foreach(println)
    
     //collect , format and print the result
     
     val results = minTemperatureBySationTemp.collect()
     
     for(res <- results){
       val id = res._1
       val temp = res._2
       val temparature = f"$temp.0.2f"
       
       println(s"$id mimimum tempeerature $temparature")
       
     }
  }
}