package com.ajbose.spark;

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.log4j._

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

/**
 * This is very similiar to movieratings but with a lil twist.
 * Instead of just displaying the ID of the movie , We try to display the name of the movie read from a dictionary file.
 * The dictionary is broadcasted into the spark context and is available at disposal for all executors.
 */
object ReadableMovieRatings {
  
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    implicit val codec = Codec("UTF-8")
    // Handle input data varitions
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val sc = new SparkContext("local[*]","ReadableMovieRatings");
    val lines = Source.fromFile("resources/u.item").getLines();
   
    var nameDict : Map[Int,String] = Map();
    
    for(line <- lines ){
       nameDict+= (line.split("\\|")(0).toInt->line.split("\\|")(1))
    }

   val nameDictRdd = sc.broadcast(nameDict);
   val data= sc.textFile("resources/u.data")
   val sortedData = data.map(x=>(x.split("\t")(1).toInt,1)).reduceByKey((x,y)=>x+y).map(x=>(nameDictRdd.value(x._1),x._2)).map(x=>(x._2,x._1)).sortByKey(false).collect();
   sortedData.foreach(println(_))
   
  }
  
}