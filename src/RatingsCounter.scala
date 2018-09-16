package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/Users/ajbose/code_new/LearnSpark/Spark/ml-100k/u.data")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => ((x.toString().split("\t"))(2),1));
    
    val result = ratings.reduceByKey((x,y)=> x+y).collect();
    
    result.foreach(println(_));
    
//    val averageData = ratings.takeSample(false, 1000);
    
    // Print each result on its own line.
    
//    ratings.foreach(println);
  }
}
