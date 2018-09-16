package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object AnalyzeBizLog {
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "AnalyzeBizLog")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/Users/ajbose/Desktop/BixLogData.txt")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
//    val rudsAudit= lines.map(x => decompress(x.toString().split("|")(4)));   
//    val averageData = ratings.takeSample(false, 1000);
    val rudsAudit= lines.map(x => x.toString().split('|')(4));   
    // Print each result on its own line.
    rudsAudit.foreach(println);
  }
  
  def decompress ( data: String) {
		
		val decodedOutput: Array[Byte] = java.util.Base64.getDecoder.decode(data);
		
		val inflater = new java.util.zip.Inflater();
		inflater.setInput(decodedOutput);
		val outputStream = new java.io.ByteArrayOutputStream(decodedOutput.length);
		
		val buffer = Array[Byte](100);
		while (!inflater.finished()) {
			
			val count = inflater.inflate(buffer);
			outputStream.write(buffer, 0, count);
		}

		outputStream.close();
		
		val output = outputStream.toByteArray();
		inflater.end();
		
		return output;
	}
}