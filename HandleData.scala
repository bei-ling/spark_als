import org.apache.spark.mllib.linalg.Vector
import scala.math._
import org.apache.commons.math3.linear._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import java.util.Date
import java.io._
object HandleData{
	
	
 	def main(args: Array[String]) 
	{

	   val spark = SparkSession
		      .builder
		      .appName("HandleData Application")
		      .getOrCreate()
		
    	val sc = spark.sparkContext
    	val conf=sc.getConf
	    val PARTITIONS = 12
 
	 	val url = "/home/program/data/ratings_5.dat"
	 	val data = sc.textFile(url,PARTITIONS)
	    val ratings = data.filter(x => x!="userId,movieId,rating,timestamp").map {
	        line =>
	        val parts = line.split(",")
	        (parts(0).toInt, parts(1).toInt, parts(2).toDouble,parts(3).toInt)
	    }
	    def maxFunc(iter: Iterator[(Int,Int,Double,Int)]): Iterator[(Int,Int)] = {
		    var Umax =  Int.MinValue
		    var Pmax = 	Int.MinValue
		    var res = List[(Int,Int)]()
		    while (iter.hasNext)
		    {
		      val cur = iter.next
		      Umax = Math.max(cur._1,Umax)
		      Pmax = Math.max(cur._2,Pmax)
		    }
		    res .::= (Umax,Pmax)
		    res.iterator
		}
 		val tmpRdd = ratings.mapPartitions(maxFunc)
 		val users = tmpRdd.map(x => x._1).reduce((x,y) => Math.max(x,y))
 		val items = tmpRdd.map(x => x._2).reduce((x,y) => Math.max(x,y))
	    for(i <- 1 to 5){
	    	val temp = ratings.filter(line => line._1 <= (users*(i/5.0))).map{
	    		case (i,j,k,l) => (i.toString+","+j.toString+","+k.toString+","+l.toString+"\r\n")
	    	}.collect()
	    	val filename = "/home/program/data/TestData/ratings_" + i + ".dat"
	    	val file = new FileWriter(filename,true)
	    	for(item <- 0 until temp.size)
	    	{
	    		file.write(temp(item))
	    	}
	    	file.close()
	    }

 		spark.stop()
	}

}
