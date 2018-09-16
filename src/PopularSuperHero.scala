package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object PopularSuperHero {
  
  def parseName(line: String) : Option[(Int,String)] ={
    val splits = line.split('\"');
    if(splits.length>1){
      return Some(splits(0).trim().toInt,splits(1))
    }else{
      return None;
    }
  }
  
  def main(args : Array[String]){
    implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
     Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "RatingsCounter");
      val rdd= sc.textFile("/Users/ajbose/Desktop/SprakData/Marvel-names.txt").flatMap(parseName)
      val lines = sc.textFile("/Users/ajbose/Desktop/SprakData/Marvel-graph.txt")
      val heroesPopularity = lines.map(x=>(x.split("\\s+")(0).toInt,x.split("\\s+").length-1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1.toInt)).sortByKey(false);
      val mostPopular = heroesPopularity.take(10);
      for(mostPopularCurrent<-mostPopular){
        val mostPopularCurrentName = rdd.lookup(mostPopularCurrent._2)(0);
        println(s"${mostPopularCurrentName} : ${mostPopularCurrent._1}")
      }
  }
}