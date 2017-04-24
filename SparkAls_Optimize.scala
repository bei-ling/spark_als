import org.apache.spark.mllib.linalg.Vector
import scala.math._
import org.apache.commons.math3.linear._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
object SparkAls{
	case class Rating[@specialized(Int, Long) ID](user: ID, item: ID, rating: Double)

	def computeRMSE(R: RDD[(Int,Int,Double,Int)],U: Array[RealVector],
		P: Array[RealVector],rows: Int,cols: Int): Double =
	{
	     
		val RPredict = R.map{
			case (i,j,rating,time) => ((i,j),U(i.toInt).dotProduct(P(j.toInt))) 
		}
		
		val ratesAndPreds =  R.map{
			case (user,product,rating,time) => ((user,product),rating) 
		}.join(RPredict)
		//ratesAndPreds.saveAsTextFile("hdfs://sist10:9000/Big4/MXJ_result/") 
		ratesAndPreds.take(100).foreach(println)
		val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
			  val err = (r1 - r2)
			  err * err
		}.mean()
		return MSE
	}
  	def predictProduct(user: Array[RealVector],product: Array[RealVector],F: Int)
  	{
  		 val fact = user(0)
  		 var indexRatings: Map[Double,Int] = Map()
  		 for(i <- 0 until product.length)
  		 {
  		 	val x = product(i)
	  		val y1 = x.dotProduct(fact)
	  		//println(i+1+"      "+y1)
	  	 }
  	}
  	 
 	def train(
 		ratings: RDD[(Int,Int,Double,Int)],
 		rank: Int,
 		lambda: Double,
 		ITERATION: Int)={
 		val numPartitions = 4
 		val sc = ratings.sparkContext
 		val conf=sc.getConf
 		conf.getBoolean("spark.broadcast.compress",true)
 		conf.set("spark.broadcast.compress","true")
 		conf.set("spark.default.parallelism", "48")
 		 

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
 		val U = tmpRdd.map(x => x._1).reduce((x,y) => Math.max(x,y))
 		val P = tmpRdd.map(x => x._2).reduce((x,y) => Math.max(x,y))
		//println(U,P)
 		
		val R = ratings.map{
			case (user,product,rating,time) => (user,(product,rating))
		}.groupByKey()
		.partitionBy(new HashPartitioner(numPartitions))
		.persist()
		 
		val Rt = ratings.map{
			case (user,product,rating,time) => (product,(user,rating))
		}.groupByKey()
		.partitionBy(new HashPartitioner(numPartitions))
		.persist()
		 
 	    var Ub = Array.fill(U)(randomVector(rank))
		var Pb = Array.fill(P)(randomVector(rank))
		 
		var bUb = sc.broadcast(Ub)
    	var bPb = sc.broadcast(Pb)


    	for(it <- 1 to ITERATION )  //set the interations 
		{
			println("ITERATION " + it)

			var tmpUb = R.mapPartitions( iter => 
				{
					var ans: Iterator[(Int,RealVector)] = Iterator()
				    while (iter.hasNext)
				    {
				       val cur = iter.next
				       ans ++=  update(cur._1,cur._2,bPb.value,U,rank,lambda)
				    }
				    ans
			    }
			).collect()
			
			tmpUb.foreach{
        		case (index, vector) => 
        		Ub(index) = vector
        	}
			bUb = sc.broadcast(Ub)
			var tmpPb = Rt.mapPartitions( iter => 
				{
					var ans: Iterator[(Int,RealVector)] = Iterator()
				    while (iter.hasNext)
				    {
				       val cur = iter.next
				       ans ++= update(cur._1,cur._2,bUb.value,P,rank,lambda)
				    }
				    ans
				}
			).collect()
			
		 	tmpPb.foreach{
        		case (index, vector) => 
        		Pb(index) = vector
        	}
			bPb = sc.broadcast(Pb)	
		}
		val mse = computeRMSE(ratings,Ub,Pb,U,P)  
	   	println(mse)
	 	R.unpersist()
		Rt.unpersist()
 	}
 	def update(index: Int,R: Iterable[(Int,Double)],P: Array[RealVector],loop: Int,
 		rank: Int,lambda: Double): Iterator[(Int,RealVector)]=
 	{
 		var left: RealMatrix = new Array2DRowRealMatrix(rank, rank)
        var right: RealVector = new ArrayRealVector(rank)
        R.foreach{
        	case (cols, values) => 
	    	right  = right.add(P(cols).mapMultiply(values))
	    	val x = P(cols)
			left = left.add(x.outerProduct(x)) 
        }
        for (d <- 0 until rank) {
	      left.addToEntry(d, d, lambda)
	    }
	    var ans = List[(Int,RealVector)]()
	    ans .::= (index,new CholeskyDecomposition(left).getSolver.solve(right))
	    ans.iterator
 	}
  	def load(rank: Int,lambda: Double,iter: Int,fileURL: String)
  	{
  		val spark = SparkSession
		      .builder
		      .appName("Recommedation Application")
		      .getOrCreate()

    	val sc = spark.sparkContext
		// Should be some file on your system
		val file= sc.textFile(fileURL,6)
		println("=====numbers===",file.count())
		val ratings = file.filter(x => x!="userId,movieId,rating,timestamp").map {
	        line =>
	        val parts = line.split(",")
	        (parts(0).toInt, parts(1).toInt, parts(2).toDouble,parts(3).toInt % 10)
	    }
	    //this is for training set and test set
	    /*
	    val numPartitions = 4
	    val training = ratings.filter(x => x._4 < 6) // 
	      .repartition(numPartitions)
	      .cache()
	    val validation = ratings.filter(x => x._4 >= 6 && x._4 < 8)
	      .repartition(numPartitions)
	      .cache()
	    val test = ratings.filter(x => x._4 >= 8)
	      .repartition(numPartitions)
	      .cache()
	      */
	    //there is to test all data
	    train(ratings,rank,lambda,iter)
		 
		spark.stop()
  	}
	def main(args: Array[String]) 
	{
		/*
		//Used to iptimize all the parameters
		val ranks = List(8, 10)
	    val lambdas = List(0.1, 0.01)
	    val numIterations = List(7,10)

	    var bestRank = 0
	    var bestLambda = -1.0
	    var bestIter = -1
	    var beseMse = Double.MaxValue
	    for(rank <- ranks; lambda <- lambdas; iter <- numIterations)
	    {
	    	val mse = train(rank,lambda,iter)
	    	if(mse < beseMse)
	    	{
	    		beseMse = mse
	    		bestRank = rank
	    		bestLambda = lambda
	    		bestIter = iter
	    	}
	    }
	    println("The bese modle is rank="+bestRank,"lambda="+bestLambda,"iter="+bestIter)
	    */
	    var rank = 10
	    var lambda = 0.01
	    var iter = 10
	    //val url = "hdfs://sist10:9000/Big4/ratings.csv" 
	    
	    val url = "/home/program/data/ratings.csv" 
	    val mse = load(rank,lambda,iter,url)
 		
	}
	private def randomVector(n: Int): RealVector =
        new ArrayRealVector(Array.fill(n)(math.random))

    private def randomMatrix(rows: Int, cols: Int): RealMatrix =
        new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}