package com.deere.spark.scala.basics.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object PopularMovies extends App {
  val sc = new SparkContext("local","PopularMovies");
  var file_path = "C:/SparkScala/ml-100k//u.data"
  val data = sc.textFile(file_path)
  
  // Map to (movieID, 1) tuples
  val pairData = data.map(f=>(f.split("\t")(1).toInt,1))
  pairData.foreach(println)
  
  val countPair = pairData.reduceByKey((x,y)=>x+y)
  countPair.foreach(println)
  
  val sortData = countPair.sortByKey()
  sortData.foreach(println)
  val resultData = sortData.collect()
  
  resultData.foreach(println)
  
  
  
  
}