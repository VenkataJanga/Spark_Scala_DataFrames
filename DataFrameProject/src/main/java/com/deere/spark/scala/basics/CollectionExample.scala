package com.deere.spark.scala.basics

object CollectionExample extends App{
  
   /**
    * Convert Java collection to Scala collection
    */
  // Step 1: Import converters
  import scala.collection.JavaConverters._

  // Step 2: Assume you have a Java Map
  val donutJavaMap: java.util.Map[String, Double] = new java.util.HashMap[String, Double]()
  donutJavaMap.put("Plain Donut", 2.50)
  donutJavaMap.put("Vanilla Donut", 3.50)
  donutJavaMap.put("Veg Pizaa",2.50)
  donutJavaMap.put("Veg Pizaa with Conrn",3.0)
  donutJavaMap.put("Veg Pizaa with Kazuandcorn",3.50)
  donutJavaMap.put("Non-Veg Pizaa",4.50)
  donutJavaMap.put("Non-Veg Pizaa with Conrn",5.0)
  donutJavaMap.put("Non-Veg Pizaa with Kazuandcorn",6.50)

  // Step 3: Convert the Java Map by calling .asScala
  val donutScalaMap = donutJavaMap.asScala

  
   //println(" KEYs Are ${donutScalaMap.keys})
  val keys = donutScalaMap.keys
  println("Keys ==="+keys)
  // Step 4: You now have a Scala Map
  val pricePlainDonut = donutScalaMap("Plain Donut")
  
  println(pricePlainDonut)
 val setDonuts = donutScalaMap.map(_._1).toSet
 
 println(setDonuts)
 
}