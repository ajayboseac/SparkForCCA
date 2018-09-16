package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerOrder {
  
  def main(args : Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "RatingsCounter");
      val lines = sc.textFile("/Users/ajbose/code_new/LearnSpark/SparkScala/customer-orders.csv");
      val customersInOrder=lines.map(x=>(x.split(",")(0),x.split(",")(2).toFloat)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey().collect();
      customersInOrder.foreach(println)
  }
}