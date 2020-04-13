package com.deere.spark.scala.basics.rdd
import org.apache.spark.sql.SparkSession
object FakeFriends1 {
   def parseLine(line:String) ={
      val fields = line.split(",")
      val age = fields(2).toInt
      val numberOfFriends =  fields(3).toInt
      (age,numberOfFriends)
    }
  def main(args:Array[String]){
    val sparkSession = SparkSession.builder().appName("").master("").getOrCreate()
    var file_path = "C:/SparkScala/DataFrame/Data/"
    val lines = sparkSession.read.textFile(file_path+"countFriends.csv")
    // val dataFrame = lines.map(parseLine)
  }
}