package atrox.sketch

import scala.language.postfixOps
import breeze.linalg.{ SparseVector, DenseVector, DenseMatrix, BitVector, normalize, Vector => bVector, operators }
import breeze.stats.distributions.Rand



object MinHash {

  def apply[T](hashFunctions: Int)(implicit mk: HashFunc[Int] => MinHasher[T]): IntSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => mk(HashFunc.random(i*1000)), Estimator(hashFunctions))


  trait MinHasher[-T] extends IntSketcher[T] {
    def apply(t: T): Int
  }

  implicit class IntArrayMinHasher(f: HashFunc[Int]) extends MinHasher[Array[Int]] {
    def apply(set: Array[Int]): Int = {
      var min = Int.MaxValue
      var j = 0 ; while (j < set.length) {
        min = math.min(min, f(set(j)))
        j += 1
      }
      min
    }
  }

  implicit class GeneralMinHasher[T](f: HashFunc[Int]) extends MinHasher[Traversable[T]] {
    def apply(set: Traversable[T]): Int = {
      var min = Int.MaxValue
      for (el <- set) {
        min = math.min(min, f(el.hashCode))
      }
      min
    }
  }

  case class Estimator(sketchLength: Int) extends IntEstimator {
    def estimateSimilarity(sameBits: Int): Double =
      sameBits.toDouble / sketchLength

    def minSameBits(sim: Double): Int = {
      require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
      (sim * sketchLength).toInt
    }
  }

}



/** based on https://www.sumologic.com/2015/10/22/rapid-similarity-search-with-weighted-min-hash/ */
object WeightedMinHash {

  def apply[T, W](hashFunctions: Int, weights: W)(implicit mk: ((HashFunc[Int], W)) => WeightedMinHasher[T]): IntSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => mk(HashFunc.random(i*1000), weights), MinHash.Estimator(hashFunctions))


  /** applies one function to every element of one set and reduces it to minumum */
  trait WeightedMinHasher[-T] extends IntSketcher[T] {
    def apply(t: T): Int
  }

  implicit class IntArrayMinHasher(fw: (HashFunc[Int], Array[Int])) extends WeightedMinHasher[Array[Int]] {
    val (f, weights) = fw
    def apply(set: Array[Int]): Int = {
      var min = Int.MaxValue
      var j = 0 ; while (j < set.length) {
        var h = set(j)
        var i = 0 ; while (i < weights(j)) {
          min = math.min(min, f(h))
          i += 1
        }
        j += 1
      }
      min
    }
  }

  implicit class GeneralMinHasher[T](fw: (HashFunc[Int], Map[T, Int])) extends WeightedMinHasher[Traversable[T]] {
    val (f, weights) = fw
    def apply(set: Traversable[T]): Int = {
      var min = Int.MaxValue
      for (el <- set) {
        var h = el.hashCode
        for (_ <- 0 until weights(el)) {
          min = math.min(min, f(h))
        }
      }
      min
    }
  }

}



/** MinHash that uses only one bit. It's much faster than traditional MinHash
  * but it seems it's less precise.
  * As of right now it's not really suitable for LSH, because most elements
  * are hashed into few buckets. Investigation pending.
  * https://www.endgame.com/blog/minhash-vs-bitwise-set-hashing-jaccard-similarity-showdown */
object WeightedSingleBitMinHash {
  import MinHash.MinHasher

  def apply[T, W](hashFunctions: Int, weights: W)(implicit mk: HashFunc[Int] => MinHasher[T]): BitSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => WeightedSingleBitMinHasher(mk(HashFunc.random(i*1000))), mkEstimator(hashFunctions))

  case class WeightedSingleBitMinHasher[T](mh: MinHasher[T]) extends BitSketcher[T] {
    def apply(x: T): Boolean = (mh(x) & 1) != 0
  }

  implicit class IntArrayMinHasher(f: HashFunc[Int]) extends MinHash.IntArrayMinHasher(f)
  implicit class GeneralMinHasher(f: HashFunc[Int])  extends MinHash.GeneralMinHasher(f)

  def mkEstimator(sketchLength: Int) = sketchLength match {
    case 64  => new Estimator(sketchLength) with BitEstimator64
    case 128 => new Estimator(sketchLength) with BitEstimator128
    case 256 => new Estimator(sketchLength) with BitEstimator256
    case _   => new Estimator(sketchLength)
  }

  class Estimator(val sketchLength: Int) extends BitEstimator {
    def estimateSimilarity(sameBits: Int): Double =
      1.0 - 2.0 / sketchLength * (sketchLength - sameBits)

    def minSameBits(sim: Double): Int = {
      sketchLength - ((1 - sim) / (2.0 / sketchLength)).toInt
    }
  }
}



object RandomHyperplanes {
  import scala.language.reflectiveCalls

  def apply[V](n: Int, vectorLength: Int)(implicit ev: CanDot[V]): BitSketchers[V] =
    Sketchers(n, (i: Int) => mkSketcher(vectorLength, i * 1000), Estimator(n))

//  def apply(rowMatrix: DenseMatrix[Double], n: Int): BitSketch[DenseVector[Double]] = ???
//    apply(0 until rowMatrix.rows map { r => rowMatrix(r, ::).t }, n)(CanDotDouble)


  trait CanDot[InVec] {
    type RndVec
    def makeRandomHyperplane(length: Int, seed: Int): RndVec
    def dot(a: InVec, b: RndVec): Double
  }

  implicit def CanDotFloat[V](implicit dotf: operators.OpMulInner.Impl2[V, DenseVector[Float], Float]) = new CanDot[V] {
    type RndVec = DenseVector[Float]
    def makeRandomHyperplane(length: Int, seed: Int): RndVec = mkRandomHyperplane(length, seed) mapValues (_.toFloat)
    def dot(a: V, b: DenseVector[Float]): Double = dotf(a, b).toDouble
  }

  implicit def CanDotDouble[V](implicit dotf: operators.OpMulInner.Impl2[V, DenseVector[Double], Double]) = new CanDot[V] {
    type RndVec = DenseVector[Double]
    def makeRandomHyperplane(length: Int, seed: Int): RndVec = mkRandomHyperplane(length, seed)
    def dot(a: V, b: DenseVector[Double]): Double = dotf(a, b)
  }


  private def mkSketcher[V](length: Int, seed: Int)(implicit ev: CanDot[V]): BitSketcher[V] = new BitSketcher[V] {
    private val rand = ev.makeRandomHyperplane(length, seed)
    def apply(item: V) = ev.dot(item, rand) > 0.0
  }

  private def mkRandomHyperplane(length: Int, seed: Int): DenseVector[Double] = {
    val rand = new scala.util.Random(seed)
    DenseVector.fill[Double](length)(if (rand.nextDouble < 0.5) -1.0 else 1.0)
  }

  case class Estimator(sketchLength: Int) extends BitEstimator {
    def estimateSimilarity(sameBits: Int): Double =
      math.cos(math.Pi * (1 - sameBits / sketchLength.toDouble))

    def minSameBits(sim: Double): Int = {
      require(sim >= -1 && sim <= 1, "similarity must be from (-1, 1)")
      math.floor((1.0 - math.acos(sim) / math.Pi) * sketchLength).toInt
    }
  }
}






object RandomProjections {

  def apply[V](projections: Int, bucketSize: Double, vectorLength: Int): IntSketchers[bVector[Double]] =
    Sketchers(projections, (i: Int) => mkSketcher(vectorLength, i * 1000, bucketSize), Estimator(projections))


  private def mkSketcher(vectorLength: Int, seed: Int, bucketSize: Double) =
    new IntSketcher[bVector[Double]] {
      private val randVec = mkRandomUnitVector(vectorLength, seed)

      def apply(item: bVector[Double]): Int =
        ((randVec dot item) / bucketSize).toInt
    }

  private def mkRandomUnitVector(length: Int, seed: Int) = {
    val rand = new scala.util.Random(seed)
    normalize(DenseVector.fill[Double](length)(rand.nextGaussian), 2)
    //normalize(DenseVector.rand[Double](length, Rand.gaussian), 2)
  }

  case class Estimator(sketchLength: Int) extends IntEstimator {
    def estimateSimilarity(sameBits: Int): Double = ???
    def minSameBits(sim: Double): Int = ???
  }

}



/*
object PStableDistributions {

  def apply(vectors: IndexedSeq[SparseVector[Double]], sketchLength: Int, p: Double): PStableDistributions = {
  }

  // http://www.cs.dartmouth.edu/~ac/Teach/CS49-Fall11/Papers/indyk-stable.pdf
  def pstable(p: Double, a: Double, b: Double): Double = {
    require(a >= 0 && a <= 1.0)
    require(b >= 0 && b <= 1.0)
    import math._

    val Θ = (a - 0.5) * Pi // [-π/2, π/2]
    val r = b              // [0, 1]

    sin(p * Θ) / pow(cos(Θ), 1.0 / p) * pow(cos(Θ * (1 - p)) / -log(r), (1 - p) / p)
  }

}


final class PStableDistributions(val sketchArray: Array[Double], val sketchLength: Int, val p: Double) {

  def estimateSimilarity(idxA: Int, idxB: Int): Double

  def sameBits(idxA: Int, idxB: Int): Int = ???
  def minSameBits(sim: Double): Int = ???
  def empty: Sketch = ???
}
*/



object HammingDistance {
  def apply(arr: Array[Long], bits: Int): BitSketch[Nothing] = {
    require(bits % 64 == 0)

    BitSketch[Nothing](
      arr,
      new BitSketchers[Nothing] { self =>
        val sketchLength = bits
        val estimator = Estimator(bits)
        def getSketchFragment(item: Nothing, from: Int, to: Int) = sys.error("this should not happen")
      }
    )
  }

  case class Estimator(sketchLength: Int) extends BitEstimator {
    private[this] val inv = 1.0 / sketchLength

    def estimateSimilarity(sameBits: Int): Double = sameBits * inv
    def minSameBits(sim: Double): Int = (sketchLength * sim).toInt
  }
}





object SimHash {

  def apply[T](implicit f: HashFuncLong[T]): BitSketchers[Array[T]] =
    new BitSketchers[Array[T]] {
      val sketchLength = 64
      val estimator = HammingDistance.Estimator(64)
      def getSketchFragment(item: Array[T], from: Int, to: Int): Array[Long] = {
        require(from == 0 && to == 64)
        Array[Long](doSimHash64(item, f))
      }
    }


  implicit def md5 = new HashFuncLong[String] {
    def apply(x: String): Long = {
      val m = java.security.MessageDigest.getInstance("MD5")
      val bytes = m.digest(x.getBytes())
      java.nio.ByteBuffer.wrap(bytes).getLong
    }
  }

  private def doSimHash64[T](xs: Array[T], f: HashFuncLong[T]): Long = {

    val counts = new Array[Int](64)

    for (x <- xs) {
      val l = f(x)
      var i = 0 ; while (i < 64) {
        counts(i) += (if ((l & (1 << i)) != 0) 1 else -1)
        i += 1
      }
    }

    var hash = 0L
    var i = 0 ; while (i < 64) {
      if (counts(i) > 0) {
        hash |= 1L << i
      }
      i += 1
    }

    hash
  }
}
