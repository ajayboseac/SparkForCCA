import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/*
 * Given a dummy e-commerce data of customer-id:Int,item-id:String,totalSpent:Double 
 * Caculate the customer who has spent the max.
 */
object MaxSpentByCustomer {
  
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","MaxSpentByCustomer");
    val linesRDD = sc.textFile("resources/customer-orders.csv");
    val tempRDD = linesRDD.map(line => (line.toString().split(",")(0).toInt,line.toString().split(",")(2).toFloat));
    val resultRDD = tempRDD.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).first();
//    resultRDD.foreach(println(_))
    println(s"Customer: ${resultRDD._2} has the highest spending of ${resultRDD._1}");
  }
  
}