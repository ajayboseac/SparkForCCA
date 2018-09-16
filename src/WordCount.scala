package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object  WordCount {
  
  def main( args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingsCounter");
    val lines = sc.textFile("/Users/ajbose/code_new/LearnSpark/SparkScala/book.txt")
    val words = lines.flatMap(x=>x.split("\\W+"));
    val countedWords = words.map(x=>(x.toLowerCase(),1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false);
    countedWords.collect().foreach(println)
  }
  
}