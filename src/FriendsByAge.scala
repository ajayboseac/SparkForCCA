package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object FriendsByAge {
  
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/Users/ajbose/code_new/LearnSpark/SparkScala/fakefriends.csv")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => (x.toString().split(",")(2).toInt,(x.toString().split(",")(3).toInt,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._1/x._2).sortByKey().collect();
    
    //by first Name
//    val ratings = lines.map(x => (x.toString().split(",")(1).toString,(x.toString().split(",")(3).toInt,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>x._1/x._2).sortByKey().collect();
    
//    val averageData = ratings.takeSample(false, 1000);
    
    // Print each result on its own line.
    
    ratings.foreach(println);
  }
}