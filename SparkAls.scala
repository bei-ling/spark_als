import org.apache.spark.mllib.linalg.Vector
import scala.math._
import org.apache.commons.math3.linear._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import java.util.Date
import java.io._
object SparkAls{
	
	case class Rating[@specialized(Int, Long) ID](user: ID, item: ID, rating: Double)
	
 	def train(
 			ratings: RDD[(Int,Int,Double,Int)],
 			rank: Int,
 			lambda: Double,
 			ITERATION: Int,
 			PARTITIONS: Int):(Array[RealVector],Array[RealVector]) ={

 		val numPartitions = PARTITIONS
 		val sc = ratings.sparkContext
 		//conf.set("spark.default.parallelism", "24*2")
 		 
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
		    res .::= (Umax+1,Pmax+1)
		    res.iterator
		}
 		val tmpRdd = ratings.mapPartitions(maxFunc)
 		val users = tmpRdd.map(x => x._1).reduce((x,y) => Math.max(x,y))
 		val items = tmpRdd.map(x => x._2).reduce((x,y) => Math.max(x,y))
	 
 		
		val usersRating = ratings.map{
			case (user,product,rating,time) => (user,(product,rating))
		}.groupByKey()
		.partitionBy(new HashPartitioner(numPartitions))
		.persist()
		 
		val itemsRating = ratings.map{
			case (user,product,rating,time) => (product,(user,rating))
		}.groupByKey()
		.partitionBy(new HashPartitioner(numPartitions))
		.persist()
		 
 	    var userFactors = Array.fill(users)(randomVector(rank))
		var itemFactors = Array.fill(items)(randomVector(rank))
		 
		var buserFactors = sc.broadcast(userFactors)
    	var bitemFactors = sc.broadcast(itemFactors)


    	for(it <- 1 to ITERATION )  //set the interations 
		{
			println("ITERATION " + it)

			var tmpUb = usersRating.mapPartitions( iter => 
				{
					var userfactors: Iterator[(Int,RealVector)] = Iterator()
				    while (iter.hasNext)
				    {
				       val cur = iter.next
				       userfactors ++=  update(cur._1,cur._2,bitemFactors.value,users,rank,lambda)
				    }
				    userfactors
			    }
			).collect()
			
			tmpUb.foreach{
        		case (index, vector) => 
        		userFactors(index) = vector
        	}
			buserFactors = sc.broadcast(userFactors)
			var tmpPb = itemsRating.mapPartitions( iter => 
				{
					var itemfactors: Iterator[(Int,RealVector)] = Iterator()
				    while (iter.hasNext)
				    {
				       val cur = iter.next
				       itemfactors ++= update(cur._1,cur._2,buserFactors.value,items,rank,lambda)
				    }
				    itemfactors
				}
			).collect()
			
		 	tmpPb.foreach{
        		case (index, vector) => 
        		itemFactors(index) = vector
        	}
			bitemFactors = sc.broadcast(itemFactors)	
		}

	 	usersRating.unpersist()
		itemsRating.unpersist()
		(userFactors,itemFactors)
 	}
 	def update(
 			index: Int,
 			R: Iterable[(Int,Double)],
 			userOritemFactors: Array[RealVector],
 			loop: Int,
 			rank: Int,
 			lambda: Double): Iterator[(Int,RealVector)]={

 		var left: RealMatrix = new Array2DRowRealMatrix(rank, rank)
        var right: RealVector = new ArrayRealVector(rank)
        R.foreach{
        	case (cols, values) => 
	    	right  = right.add(userOritemFactors(cols).mapMultiply(values))
	    	val x = userOritemFactors(cols)
			left = left.add(x.outerProduct(x)) 
        }
        for (d <- 0 until rank) {
	      left.addToEntry(d, d, lambda)
	    }
	    var ans = List[(Int,RealVector)]()
	    ans .::= (index,new CholeskyDecomposition(left).getSolver.solve(right))
	    ans.iterator
 	}
  	def computeRMSE(R: RDD[(Int,Int,Double,Int)],U: Array[RealVector],
		P: Array[RealVector]): Double =
	{
	     
		val RPredict = R.map{
			case (i,j,rating,time) => ((i,j),U(i.toInt).dotProduct(P(j.toInt))) 
		}
		
		val ratesAndPreds =  R.map{
			case (user,product,rating,time) => ((user,product),rating) 
		}.join(RPredict)
		//ratesAndPreds.saveAsTextFile("hdfs://sist10:9000/Big4/MXJ_result/") 
		//ratesAndPreds.foreach(println)
		//ratesAndPreds.saveAsTextFile("/home/program/alsResult") 
		val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
			  val err = (r1 - r2)
			  err * err
		}.mean()
		return MSE
	}
  
  	def runALS(rank: Int,lambda: Double,iter: Int,fileURL: String,PARTITIONS: Int)
  	{
  		val spark = SparkSession
		      .builder
		      .appName("Recommedation Application")
		      .getOrCreate()
		
    	val sc = spark.sparkContext
    	val conf=sc.getConf
 		conf.getBoolean("spark.broadcast.compress",true)
 		conf.set("spark.broadcast.compress","true")

		val file= sc.textFile(fileURL,PARTITIONS)
		val ratings = file.filter(x => x!="userId,movieId,rating,timestamp").map {
	        line =>
	        val parts = line.split(",")
	        (parts(0).toInt, parts(1).toInt, parts(2).toDouble,parts(3).toInt % 10)
	    }
	    // Build the recommendation model using ALS

	    val (userFactors,itemFactors) =  
	    	train(ratings,rank,lambda,iter,PARTITIONS)
		val mse = computeRMSE(ratings,userFactors,itemFactors)  
	   	println(mse)
		spark.stop()
  	}
	def main(args: Array[String]) 
	{

	    var rank = 10
	    var lambda = 0.01
	    var iter = 10
	    //val url = "hdfs://sist10:9000/Big4/ratings.csv" 
	    val PARTITIONS = 12 * args(1).toInt
	 	val url = "hdfs://sist10:9000/Big4/"+args(0)
	 	val start = System.currentTimeMillis()

	    runALS(rank,lambda,iter,url,PARTITIONS)

 		val end = System.currentTimeMillis()
 		println("file "+ url)
 		println("core "+ args(1).toInt)
 		println("Run time is "+ (end -start))
 		val file = new FileWriter("/root/BigFour/Mxj/result.txt",true)
 		val content = "======================"+
 		"\r\nfile "+ url +
 		"\r\ncore "+ args(1).toInt +
 		"\r\nun time is "+ (end -start)/1000+" s"+
 		"\r\n======================"
 		file.write(content)
 		file.close()
	}
	private def randomVector(n: Int): RealVector =
        new ArrayRealVector(Array.fill(n)(math.random))

    private def randomMatrix(rows: Int, cols: Int): RealMatrix =
        new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}
