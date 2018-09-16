package com.ajbose.spark;

import org.apache.spark.SparkContext

object MovieSimilaritiesMine {
  
  def main(args : Array[String])
  {
    //Load the movie names
    
    // Load the dat file
    
    val sc = new SparkContext("local[*]","MovieSimilaritiesMine")
    val lines = sc.textFile("resources/u.data");
    val userRatings = lines.map(x=>(x.split("\\s")(0),(x.split("\\s")(1).toInt,x.split("\\s")(2).toDouble)));
    val sampleRatings = userRatings.takeSample(false,1);
    sampleRatings.foreach(println)
  }
  
}