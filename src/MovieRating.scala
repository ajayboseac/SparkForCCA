package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object MovieRating {
  
  def main(args : Array[String]){
    
     implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
     Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "RatingsCounter");
     
     val lines = Source.fromFile("/Users/ajbose/code/Spark/ml-100k/u.item").getLines();
     
     

     
     var movieNames: Map[Int,String] = Map();
     
     for(line <- lines){
       var fields = line.split("\\|");
       movieNames += (fields(0).toInt->fields(1));
     }
     
     var nameDict = sc.broadcast(movieNames);
     
     val data= sc.textFile("/Users/ajbose/code/Spark/ml-100k/u.data")
     val sortedData = data.map(x=>(x.split("\t")(1).toInt,1)).reduceByKey((x,y)=>x+y).map(x=>(nameDict.value(x._1),x._2)).map(x=>(x._2,x._1)).sortByKey().collect();
     sortedData.foreach(println);
     
  }
}