package atrox.sketch

import scala.language.postfixOps
import breeze.linalg.{ SparseVector, DenseVector, DenseMatrix, BitVector, normalize, Vector => bVector, operators }
import breeze.stats.distributions.Rand
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import java.util.Arrays


object MinHash {

  def sketching[Item](sets: Seq[Item], n: Int)(implicit mk: HashFunc[Int] => MinHasher[Item]): IntSketching =
    new IntSketchingOf(sets, n, i => mk(HashFunc.random(i * 1000)), Estimator(n))

  def apply[Item](sets: Seq[Item], n: Int)(implicit mk: HashFunc[Int] => MinHasher[Item]): IntSketch =
    IntSketch.make(sketching(sets, n), Estimator(n), componentsAtOnce = n)


  /** applies one function to every element of one set and reduces it to minumum */
  trait MinHasher[-T] extends IntSketcher[T] {
    def apply(t: T): Int
  }

  implicit class IntArrayMinHasher(f: HashFunc[Int]) extends MinHasher[Array[Int]] {
    def apply(set: Array[Int]): Int = {
      var min = Int.MaxValue
      var j = 0
      while (j < set.length) {
        val h = f(set(j))
        if (h < min) { min = h }
        j += 1
      }
      min
    }
  }

  implicit class GeneralMinHasher[T](f: HashFunc[Int]) extends MinHasher[Traversable[T]] {
    def apply(set: Traversable[T]): Int = {
      var min = Int.MaxValue
      for (el <- set) {
        val h = f(el.hashCode)
        if (h < min) { min = h }
      }
      min
    }
  }

  case class Estimator(sketchLength: Int) extends IntEstimator {
    def estimateSimilarity(sameBits: Int): Double =
      sameBits.toDouble / sketchLength

    def minSameBits(sim: Double): Int = {
      require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
      sim * sketchLength toInt
    }
  }

}



/** based on https://www.sumologic.com/2015/10/22/rapid-similarity-search-with-weighted-min-hash/ */
object WeightedMinHash {

  def sketching[Item, W](sets: Seq[Item], weights: W, n: Int)(implicit mk: ((HashFunc[Int], W)) => WeightedMinHasher[Item]): IntSketching =
    new IntSketchingOf(sets, n, i => mk(HashFunc.random(i * 1000), weights), MinHash.Estimator(n))

  def apply[Item, W](sets: Seq[Item], weights: W, n: Int)(implicit mk: ((HashFunc[Int], W)) => WeightedMinHasher[Item]): IntSketch =
    IntSketch.make(sketching(sets, weights, n), MinHash.Estimator(n), componentsAtOnce = n)


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
          h = f(h)
          if (h < min) { min = h }
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
          h = f(h)
          if (h < min) { min = h }
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
object SingleBitMinHash {
  import MinHash.MinHasher

  def sketching[Item](sets: Seq[Item], n: Int)(implicit mk: HashFunc[Int] => MinHasher[Item]): BitSketching =
    new BitSketchingOf(sets, n, (i: Int) => SingleBitMinHasher(mk(HashFunc.random(i * 1000))), mkEstimator(n))

  def apply[Item](sets: Seq[Item], n: Int)(implicit mk: HashFunc[Int] => MinHasher[Item]): BitSketch =
    BitSketch.make(sketching(sets, n), mkEstimator(n), componentsAtOnce = n)

  case class SingleBitMinHasher[T](mh: MinHasher[T]) extends BitSketcher[T] {
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


  def sketching[V <: { def size: Int }](items: Seq[V], n: Int)(implicit ev: CanDot[V]): BitSketching =
    new BitSketchingOf(items, n, i => mkSketcher(items.head.size, (i+1) * 1000), Estimator(n))

  def apply[V <: { def size: Int }](items: Seq[V], n: Int)(implicit ev: CanDot[V]): BitSketch = {
    require(n % 64 == 0, "sketch length muse be divisible by 64 (for now)")
    BitSketch.make(sketching(items, n), Estimator(n), componentsAtOnce = 1)
  }

  def apply(rowMatrix: DenseMatrix[Double], n: Int): BitSketch =
    apply(0 until rowMatrix.rows map { r => rowMatrix(r, ::).t }, n)(CanDotDouble)


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

  def sketching(items: Seq[SparseVector[Double]], n: Int, bucketSize: Int): IntSketching =
    new IntSketchingOf(items, 0 until n map { i => () => mkSketcher(items.head.size, i * 1000, bucketSize) } toArray, Estimator(n))

  def apply(items: Seq[SparseVector[Double]], n: Int, bucketSize: Int): IntSketch =
    IntSketch.make(sketching(items, n, bucketSize), Estimator(n), componentsAtOnce = 1)


  private def mkSketcher(n: Int, seed: Int, bucketSize: Int) = new IntSketcher[bVector[Double]] {
    private val randVec = mkRandomUnitVector(n, seed)

    def apply(item: bVector[Double]): Int =
      (randVec dot item) / bucketSize toInt
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
  def apply(arr: Array[Long], bits: Int): BitSketch = {
    require(bits % 64 == 0)
    new BitSketch(arr, bits, Estimator(bits))
  }

  case class Estimator(sketchLength: Int) extends BitEstimator {
    private[this] val inv = 1.0 / sketchLength

    def estimateSimilarity(sameBits: Int): Double = sameBits * inv
    def minSameBits(sim: Double): Int = (sketchLength * sim).toInt
  }
}





object SimHash {

  val md5 = new HashFuncLong[String] {
    def apply(x: String): Long = {
      val m = java.security.MessageDigest.getInstance("MD5")
      val bytes = m.digest(x.getBytes())
      java.nio.ByteBuffer.wrap(bytes).getLong
    }
  }


  def apply[T](xs: IndexedSeq[Array[T]], f: HashFuncLong[T]): BitSketch = {
    val sketchArray = new Array[Long](xs.length)

    for (i <- 0 until xs.size) {
      sketchArray(i) = mkSimHash64(xs(i), f)
    }

    HammingDistance(sketchArray, 64)
  }


  def mkSimHash64[T](xs: Array[T], f: HashFuncLong[T]): Long = {

    val counts = new Array[Int](64)

    for (x <- xs) {
      val l = f(x)
      var i = 0
      while (i < 64) {
        counts(i) += (if ((l & (1 << i)) != 0) 1 else -1)
        i += 1
      }
    }

    var hash = 0L
    var i = 0
    while (i < 64) {
      if (counts(i) > 0) {
        hash |= 1L << i
      }
      i += 1
    }

    hash
  }
}
