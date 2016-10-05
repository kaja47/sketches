package collab

import breeze._
import breeze.linalg._
import scala.math.sqrt
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, future, Await }
import scala.concurrent.ExecutionContext.Implicits.global


object FunkSVD {

	def apply(input: Seq[SparseVector[Double]], features: Int, iterations: Int): (DenseMatrix[Double], DenseMatrix[Double]) = {

		val m = input.size // rows
		val n = input.head.size // cols
		val k = features
		val λ = 0.003 // learning rate
		val γ = 0.1   // regularization term

		def R = for { 
			row <- (0 until input.size).iterator ;
			(col, r) <- input(row).activeIterator
		} yield (r, row, col)

		val U = DenseMatrix.fill[Double](k, m)(0.1)
		val V = DenseMatrix.fill[Double](k, n)(0.1)

		val size = input.map(_.activeSize).sum
		println("size "+size)

		for (f <- 0 until k) {
			println("feature "+f)
			for (it <- 0 until iterations) { // until convergence
				var err = 0.0d
				println("iteration "+it)
				for ((r, a, i) <- R) {
					val p = U(::, a) dot V(::, i)
					val ε = r - p

					err += ε * ε

					val du = λ * (ε * V(f, i) - γ * U(f, a))
					val dv = λ * (ε * U(f, a) - γ * V(f, i))

					U(f, a) += du
					V(f, i) += dv

				}
				println(sqrt(err/size))
			}
		}

		(U.t, V.t)
	}

	def fast(input: Seq[SparseVector[Double]], features: Int, iterations: Int = Int.MaxValue, learningRate: Double = 0.003): (DenseMatrix[Double], DenseMatrix[Double]) = {

		val m = input.size // rows
		val n = input.head.size // cols
		val k = features
		val λ = learningRate
		val γ = 0.1   // regularization term

		val totalSize = input.map(_.activeSize).sum
		println("totalSize "+totalSize)

		val rows      = new Array[Int](totalSize)
		val cols      = new Array[Int](totalSize)
		val ratings   = new Array[Double](totalSize)
		val residuals = new Array[Double](totalSize)

		var j = 0
		for { 
			row <- (0 until input.size).iterator ;
			(col, r) <- input(row).activeIterator
		} {
			rows(j) = row
			cols(j) = col
			ratings(j)   = r
			j += 1
		}

//		val U = DenseMatrix.fill[Double](k, m)(0.1)
//		val V = DenseMatrix.fill[Double](k, n)(0.1)

		val U = new Array[Double](k*m) // row major
		for (i <- 0 until k*m) U(i) = 0.1

		val V = new Array[Double](k*n) // row major
		for (i <- 0 until k*n) V(i) = 0.1


		def dot(a: Array[Double], b: Array[Double], ai: Int, bi: Int, len: Int): Double = {
			var i = 0
			var res = 0.0
			while (i < len) {
				res += a(ai*len+i) * b(bi*len+i)
				i += 1
			}
			res
		}

		def predict(j: Int, prod: Double): Double =
			residuals(j) + prod


		for (f <- 0 until k) {
			println("feature "+f)

			var j = 0
			while (j < totalSize) {
				val (a, i) = (rows(j), cols(j))
				residuals(j) = dot(U, V, a, i, k) - U(a*k+f) * V(i*k+f)
				j += 1
			}

			var preverr = 99999999.0d
			var err     = 0.0d
			var errdiff = Double.PositiveInfinity

			for (it <- 0 until iterations if errdiff > 0.000001) { // until convergence
				val start = System.currentTimeMillis

				err = 0.0d
				print("feature "+f+", iteration "+it)
				var j = 0
				while (j < totalSize) {
					val r = ratings(j)
					val a = rows(j)
					val i = cols(j)

					val u = U(a*k+f)
					val v = V(i*k+f)

					val p = predict(j, u * v) // dot(U, V, a, i, k) //U(::, a) dot V(::, i)
					val ε = r - p

					err += ε * ε

					val du = λ * (ε * v - γ * u)
					val dv = λ * (ε * u - γ * v)

					U(a*k+f) += du
					V(i*k+f) += dv

					j += 1
				}
				val delta = System.currentTimeMillis - start
				println(" "+delta+"ms")

				val errdiff = sqrt(preverr/totalSize) - sqrt(err/totalSize)
				println("err "+sqrt(err/totalSize)+" errdiff "+errdiff)
				preverr = err
			}

		}

		val UM = new DenseMatrix(m, k, U, offset = 0, majorStride = k, isTranspose = true)
		val VM = new DenseMatrix(n, k, V, offset = 0, majorStride = k, isTranspose = true)

		(UM, VM)
	}

	def ballancedRanges(input: Seq[SparseVector[Double]], n: Int): (Seq[Range], Seq[Range]) = {
		val rowCounts = input.map(_.activeSize.toDouble)

		val colCounts = DenseVector.zeros[Double](input.head.size)
		for (vec <- input) {
			colCounts += vec
		}

		(ballance(rowCounts.toArray, n), ballance(colCounts.toArray, n))
	}

	def ballance(counts: Array[Double], n: Int) = {
		val sum = counts.sum
		val rangeSum = sum / n
		val cumsum = counts.scan(0.0)(_ + _).tail
		val borders = (0 until n map (i => cumsum.indexWhere(_ >= rangeSum * i))) :+ cumsum.length
		borders.sliding(2).toVector.map { case Seq(start, end) => start until end }
	}


	class Chunk(val size: Int) {
		val rows      = new Array[Int](size)
		val cols      = new Array[Int](size)
		val ratings   = new Array[Float](size)
		val residuals = new Array[Float](size)
	}

	def par(threads: Int, input: Seq[SparseVector[Double]], features: Int, iterations: Int = Int.MaxValue, learningRate: Double = 0.003): (DenseMatrix[Float], DenseMatrix[Float]) = {

		val m = input.size // rows
		val n = input.head.size // cols
		val k = features
		val λ = learningRate.toFloat
		val γ = 0.1f   // regularization term

		val totalSize = input.map(_.activeSize).sum
		println("totalSize "+totalSize)

		val (rowRanges, colRanges) = ballancedRanges(input, threads)

		println(rowRanges map (r => (r.start, r.end)))
		println(colRanges map (r => (r.start, r.end)))

		val chunks: Array[Array[Chunk]] = (0 until threads).par.map { chunkRow =>
			Array.tabulate(threads) { chunkCol =>

				def iterate: Iterator[(Int, Int, Float, Int)] = {
					var j = 0
					for { 
						row <- rowRanges(chunkRow).iterator
						(col, r) <- input(row).activeIterator
						if colRanges(chunkCol).contains(col)
					} yield {
						j += 1
						(row, col, r.toFloat, j-1)
					}
				}

				val chunk = new Chunk(iterate.size)

				for ((row, col, r, j) <- iterate) {
					chunk.rows(j)    = row
					chunk.cols(j)    = col
					chunk.ratings(j) = r
				}

				chunk
			}
		}.toArray

		for (ch <- chunks; c <- ch) { println(c.size) }

		// tall and thin matrix, latent factors of rows
		val U = new Array[Float](k*m)
		for (i <- 0 until k*m) U(i) = 0.1f

		// short and wide matrix, latent factors of columns
		val V = new Array[Float](k*n)
		for (i <- 0 until k*n) V(i) = 0.1f


		def dot(U: Array[Float], V: Array[Float], ui: Int, vi: Int, k: Int, n: Int): Float = {
			var i = 0
			var res = 0.0f
			while (i < k) {
				res += U(ui+m*i) * V(vi+n*i)
				i += 1
			}
			res
		}

		for (f <- 0 until k) {
			println("feature "+f)


			for (cs <- chunks.par) {
				for (chunk <- cs) {
					var j = 0
					while (j < chunk.size) {
						val row = chunk.rows(j)
						val col = chunk.cols(j)
						chunk.residuals(j) = dot(U, V, row, col, k, n) - U(row + m*f) * V(col + n*f)
						j += 1
					}
				}
			}

			var preverr = Double.MaxValue
			var err     = 0.0d
			var errdiff = Double.MaxValue

			for (it <- (0 until iterations).iterator if errdiff > 1e-7) { // until convergence
				val start = System.currentTimeMillis
				print("feature "+f+", iteration "+it)

				val startChunkCoords = 0 until threads map (t => (t,t))
				val stages: Seq[Seq[(Int, Int)]] = 0 until threads map { t => startChunkCoords map { case (row, col) => (row, (col + t) % 4) } }

				err = 0.0d
				stages foreach { coords =>
					val xs = coords.par map { case (chunkRow, chunkCol) =>
						var chunkErr = 0.0d
						val chunk = chunks(chunkRow)(chunkCol)
						var j = 0
						while (j < chunk.size) {
							val r   = chunk.ratings(j)
							val row = chunk.rows(j)
							val col = chunk.cols(j)

							val u = U(row + m*f)
							val v = V(col + n*f)

							val p = chunk.residuals(j) + u * v // predicted rating
							val ε = r - p

							chunkErr += ε * ε

							val du = λ * (ε * v - γ * u)
							val dv = λ * (ε * u - γ * v)

							U(row + m*f) += du
							V(col + n*f) += dv

							j += 1
						}
						chunkErr
					}
					err += xs.sum
				}

				val delta = System.currentTimeMillis - start
				println(" "+delta+"ms")

				errdiff = (sqrt(preverr/totalSize) - sqrt(err/totalSize)).toFloat
				println(s"prevErr: $preverr (${sqrt(preverr/totalSize)}), err: $err (${sqrt(err/totalSize)}), errdiff: $errdiff")
				preverr = err
			}

		}

		val UM = new DenseMatrix(m, k, U, offset = 0, majorStride = k, isTranspose = true)
		val VM = new DenseMatrix(n, k, V, offset = 0, majorStride = k, isTranspose = true)

		(UM, VM)
	}




	def thread(f: => Unit): Thread = {
		new Thread(new Runnable {
			def run: Unit = f
		})
	}

	def runThreads[K](xs: Seq[K])(f: K => Unit) = {
		val ts = xs map { x => thread(f(x)) }
		ts.foreach(_.start())
		ts.foreach(_.join())
	}
}
