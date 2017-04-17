import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.Vector
import scala.math._
import org.apache.commons.math3.linear._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object SparkAls{
	val F = 10  // features 
	val ITERATION = 10  //iteration 
	val LAMBDA = 0.01 // 
	def updateUser(Index: Int, R: Vector,
	P: Array[RealVector],Loop: Int): RealVector =
	{
	    var left: RealMatrix = new Array2DRowRealMatrix(F, F)
            var right: RealVector = new ArrayRealVector(F)
		
	    R.foreachActive{
	    	case (cols, values) => 
	    	right  = right.add(P(cols).mapMultiply(values))
	    	val x = P(cols)
		left = left.add(x.outerProduct(x))  
	    }
		 
	   for (d <- 0 until F) {
	      left.addToEntry(d, d, LAMBDA)
	    }
		new CholeskyDecomposition(left).getSolver.solve(right)
	}
	def updateProduct(Index: Int, R: Array[(Long,Vector)],hashPb: Map[Int,Int],
	U: Array[RealVector],Loop: Int): RealVector =
	{
	    var left: RealMatrix = new Array2DRowRealMatrix(F, F)
            var right: RealVector = new ArrayRealVector(F)
	    if(hashPb.contains(Index))
	    {
		R(hashPb(Index))._2.foreachActive{
		    	case (cols, values) => 
		    	right  = right.add(U(cols).mapMultiply(values)) 
		    	val x = U(cols)
			left = left.add(x.outerProduct(x))
	    	} 
	    } 
		 
	    for (d <- 0 until F) {
	      left.addToEntry(d, d, LAMBDA)
	    }
	    new CholeskyDecomposition(left).getSolver.solve(right)
	    
	}
	def computeRMSE(R: CoordinateMatrix,U: Array[RealVector],
		P: Array[RealVector],rows: Int,cols: Int): Double =
	{
	     
		val RPredict = R.entries.map{
			case MatrixEntry(i,j,rating) => ((i,j),U(i.toInt).dotProduct(P(j.toInt))) 
		}
		
		val ratesAndPreds =  R.entries.map{
			case MatrixEntry(user,product,rating) => ((user,product),rating) 
		}.join(RPredict)
		//ratesAndPreds.foreach(println)
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
	  		println(i+1+"      "+y1)
	  	 }

  	}
	def main(args: Array[String]) {
 
		val spark = SparkSession
		      .builder
		      .appName("Recommedation Application")
		      .getOrCreate()

    	val sc = spark.sparkContext
		val dataFile = "/home/program/data/ratings.dat" // Should be some file on your system
		val matrixData= sc.textFile(dataFile,4).cache()
		val Rmatrix = new CoordinateMatrix(matrixData.map(x => x.split("::")).map{
		    case Array(user,product,rating,time) =>   MatrixEntry(user.toLong-1,product.toLong-1,rating.toDouble)
		})

		val U = Rmatrix.numRows().toInt
		val P = Rmatrix.numCols().toInt
		val R = Rmatrix.toIndexedRowMatrix().rows
		val Rt = Rmatrix.transpose().toIndexedRowMatrix().rows //transpose RDD
		val Ri = R.map{
			case IndexedRow(index,vector) => (index,vector)
		}.sortByKey().collect
		val Rj = Rt.map{
			case IndexedRow(index,vector) =>(index,vector)
		}.sortByKey().collect

		var hashPb: Map[Int,Int] = Map()
		for(i <- 0  until Rj.size){
			hashPb += (Rj(i)._1.toInt -> i)
		}
		var Ub = Array.fill(U)(randomVector(F))
		var Pb = Array.fill(P)(randomVector(F))
		//Initialize p matrix
		 
    	        var bUb = sc.broadcast(Ub)
    	        var bPb = sc.broadcast(Pb)
		 

		for(it <- 1 to ITERATION )  //set the interations 
		{
			 println("ITERATION " + it)
			 Ub = sc.parallelize(0 until U,4).map(i=>updateUser(i,Ri(i)._2,bPb.value,P)).collect()
			 bUb = sc.broadcast(Ub)
			 Pb = sc.parallelize(0 until P,4).map(j=>updateProduct(j,Rj,hashPb,bUb.value,U)).collect()
			 bPb = sc.broadcast(Pb)	
			
		}
		//compute mse
		val mse = computeRMSE(Rmatrix,Ub,Pb,U,P)  
	   	println(mse)
	   	predictProduct(Ub,Pb,F)
		spark.stop()
	}
	private def randomVector(n: Int): RealVector =
           new ArrayRealVector(Array.fill(n)(math.random))

        private def randomMatrix(rows: Int, cols: Int): RealMatrix =
           new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}
