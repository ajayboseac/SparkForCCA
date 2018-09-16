package com.sundogsoftware.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

import breeze.linalg.min
import breeze.linalg.max


object CalculateMinimumTemperature {
  
  def main(args: Array[String]){
       // Create a SparkContext using every core of the local machine, named RatingsCounter
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingsCounter")
    
    val lines = sc.textFile("/Users/ajbose/code_new/LearnSpark/SparkScala/1800.csv");
    val temps = lines.map(x=>(x.split(",")(1),x.split(",")(2),x.split(",")(3)))
    val minTemps = temps.filter(x=>x._2=="TMIN").map(x=>(x._1,x._3.toFloat)).reduceByKey((x,y)=> min(x,y)).collect()
//    println(minTemps.head._2.head)
    minTemps.foreach(println(_))
  }
  
}