import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.math.sqrt


object MovieSimilaritiestest {
  
  
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  /**
   * Load movie names from u.item file.
   */
  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("resources/u.item").getLines();
    for (line <- lines) {
      var fields = line.split('|')
      movieNames += (fields(0).toInt -> fields(1))
    }
    return movieNames
  }
  
  
  def makePairs( x: UserRatingPair) = {
    
    var movieRating1 = x._2._1;
    var movieRating2 = x._2._2;
    
    val movieId1= movieRating1._1;
    val movieId2= movieRating2._1;
    
    val rating1= movieRating1._2;
    val rating2= movieRating2._2;
    
    ((movieId1,movieId2),(rating1,rating2));
    
  }
  
  def filterDuplicates ( pair:UserRatingPair ): Boolean= {
    var movieRating1 = pair._2._1;
    var movieRating2 = pair._2._2;
    val movieId1= movieRating1._1;
    val movieId2= movieRating2._1;
    movieId1<movieId2
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[(Double, Double)]
  
   def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }

  /**
   * Here we try to generate the movie similarity 
   * 
   */
  def main(args: Array[String]) {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    val movieNames = loadMovieNames();
//    movieNames.foreach(println(_))
    
    //Load actual movie rating data
    
//    val data = sc.textFile("resources/u.data");
    val data = sc.textFile("resources/u.data");
    val movieRatings = data.map(line => (line.split("\t")(0).toInt,(line.split("\t")(1).toInt,line.split("\t")(2).toDouble)));
    
    val joinedRatings = movieRatings.join(movieRatings)
    
    val filteredJoinedRatings =  joinedRatings.filter(filterDuplicates);
    
    val moviePairs = filteredJoinedRatings.map(x => makePairs(x));
    
    val groupedRatings = moviePairs.groupByKey();
    
    val movieSimilarities = groupedRatings.mapValues(computeCosineSimilarity).cache();
    
    groupedRatings.foreach(println(_))
    
    val movieIdGiven = args(0).toInt
    
    
     val scoreThreshold = 0.97
     val coOccurenceThreshold = 50.0
    
    val similarMovies = movieSimilarities.filter(x=> {
      val pair = x._1;
      val similarity = x._2;
      
      (movieIdGiven==pair._1 || movieIdGiven==pair._2 )&& (similarity._1>scoreThreshold && similarity._2>coOccurenceThreshold)
       
    })
    
    for(similarMovie <- similarMovies){
      
      var moviePair =  similarMovie._1;
      var simStrengrth =  similarMovie._2;
      
      var similarMovieId = moviePair._1;
      
      if(similarMovieId == movieIdGiven){
        similarMovieId = moviePair._2;
      }
      
      println (s"Similar Movie : ${movieNames.get(similarMovieId)}")
      
    }
    
  }

}