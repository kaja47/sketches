package atrox.sketch

import scala.language.postfixOps
import breeze.linalg.{ SparseVector, DenseVector, DenseMatrix, BitVector, normalize, Vector => bVector, operators }
import breeze.stats.distributions.Rand



object MinHash {

  def apply[T](hashFunctions: Int)(implicit mk: MinHashImpl[T, Any]): IntSketchers[T] =
    weighted(hashFunctions, (a: Any) => 1)

  /** based on https://www.sumologic.com/2015/10/22/rapid-similarity-search-with-weighted-min-hash/ */
  def weighted[T, El](hashFunctions: Int, weights: El => Int)(implicit mk: MinHashImpl[T, El]): IntSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => mk(HashFunc.random(i*1000), weights), Estimator(hashFunctions), None)

  /** MinHash that uses only one bit. It's much faster than traditional MinHash
    * but it seems it's less precise.
    * As of right now it's not really suitable for LSH, because most elements
    * are hashed into few buckets. Investigation pending.
    * https://www.endgame.com/blog/minhash-vs-bitwise-set-hashing-jaccard-similarity-showdown */
  def singleBit[T](hashFunctions: Int)(implicit mk: MinHashImpl[T, Any]): BitSketchers[T] =
    singleBitWeighted(hashFunctions, (a: Any) => 1)

  def singleBitWeighted[T, El](hashFunctions: Int, weights: El => Int)(implicit mk: MinHashImpl[T, El]): BitSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => SingleBitMinHasher(mk(HashFunc.random(i*1000), weights)), SingleBitEstimator(hashFunctions), None)



  trait WeightedMinHasher[-T] extends IntSketcher[T] {
    def apply(t: T): Int
  }

  case class SingleBitMinHasher[T](mh: WeightedMinHasher[T]) extends BitSketcher[T] {
    def apply(x: T): Boolean = (mh(x) & 1) != 0
  }


  trait MinHashImpl[T, +El] {
    def apply(hf: HashFunc[Int], weights: El => Int): WeightedMinHasher[T]
  }

  implicit val IntArrayMinHashImpl = new MinHashImpl[Array[Int], Int] {
    def apply(hf: HashFunc[Int], weights: Int => Int): WeightedMinHasher[Array[Int]] =
      new WeightedMinHasher[Array[Int]] { def apply(set: Array[Int]) = minhashArr(set, hf, weights) }
  }

  (hf, weights) => { (set) => minhashArr(set, hf, weights) }

  implicit def GeneralMinHashImpl[El] = new MinHashImpl[Traversable[El], El] {
    def apply(hf: HashFunc[Int], weights: El => Int): WeightedMinHasher[Traversable[El]] =
      new WeightedMinHasher[Traversable[El]] { def apply(set: Traversable[El]): Int = minhashTrav(set, hf, weights) }
  }


  private def minhashArr(set: Array[Int], f: HashFunc[Int], weights: Int => Int): Int = {
    var min = Int.MaxValue
    var j = 0 ; while (j < set.length) {
      var h = set(j)
      var i = 0 ; while (i < weights(j)) {
        h = f(h)
        min = math.min(min, h)
        i += 1
      }
      j += 1
    }
    min
  }

  private def minhashTrav[El](set: Traversable[El], f: HashFunc[Int], weights: El => Int): Int = {
    var min = Int.MaxValue
    for (el <- set) {
      var h = el.hashCode
      for (_ <- 0 until weights(el)) {
        h = f(h)
        min = math.min(min, h)
      }
    }
    min
  }


  case class Estimator(sketchLength: Int) extends IntEstimator {
    def estimateSimilarity(sameBits: Int): Double =
      sameBits.toDouble / sketchLength

    def minSameBits(sim: Double): Int = {
      require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
      (sim * sketchLength).toInt
    }
  }

  case class SingleBitEstimator(val sketchLength: Int) extends BitEstimator {
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
    Sketchers(n, (i: Int) => mkSketcher(vectorLength, i * 1000), Estimator(n), None)

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
    Sketchers(projections, (i: Int) => mkSketcher(vectorLength, i * 1000, bucketSize), Estimator(projections), None)


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
// A Brief Index for Proximity Searching https://www.researchgate.net/publication/220843654_A_Brief_Index_for_Proximity_Searching
object RandomPermutations {

  type DistFun[T] = (T, T) => Double

  def sketching[T](items: IndexedSeq[T], referencePoints: IndexedSeq[T], dist: DistFun[T]): BitSketching =
    new BitSketching {
      val sketchLength: Int = referencePoints.length
      val length: Int = items.length
      val estimator: BitEstimator = Estimator(referencePoints.length)

      def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Long], destOffset: Int): Unit = {
        val p = permutation(referencePoints, items(itemIdx), dist)
        val arr = encode(p, m = referencePoints.length / 2)

        var i = from
        var j = destOffset
        while (i < to) {
          val bit = (arr(i / 64) >> (i % 64)) & 1
          dest(j / 64) |= (bit << (j % 64))
          i += 1
          j += 1
        }
      }
    }

  def apply[T](items: IndexedSeq[T], referencePoints: IndexedSeq[T], dist: DistFun[T]): BitSketch[T] = {
    val sk = sketching(items, referencePoints, dist)
    BitSketch.make(sk)
  }

  def sketching[T](items: IndexedSeq[T], referencePoints: Int, dist: DistFun[T]): BitSketching =
    sketching(items, sampleReferencePoints(items, referencePoints), dist)

  def apply[T](items: IndexedSeq[T], referencePoints: Int, dist: DistFun[T]): BitSketch[T] =
    apply(items, sampleReferencePoints(items, referencePoints), dist)


  private def sampleReferencePoints[T](items: IndexedSeq[T], n: Int): IndexedSeq[T] = {
    // TODO sampling without repetition
    val rnd = new util.Random(1234)
    IndexedSeq.fill(n) { items(rnd.nextInt(items.length)) }
  }

  def permutation[T](referencePoints: IndexedSeq[T], q: T, dist: DistFun[T]): Array[Int] =
    referencePoints.zipWithIndex.map { case (p, i) => (dist(q, p), i) }.sorted.map(_._2).toArray

  def inv(p: Array[Int]) = {
    val inv = new Array[Int](p.length)
    for (i <- 0 until p.length) inv(p(i)) = i
    inv
  }

  // m = p.length / 2 is apparebntly a good choice
  def encode(p: Array[Int], m: Int) = {
    require(m > 0)
    val pinv = inv(p)
    val C = new Array[Long]((p.length+63)/64)
    for (i <- 0 until p.length) {
      if (math.abs(i - pinv(i)) > m) {
        C(i / 64) |= (1 << (i % 64))
      }
    }
    C
  }

  // Bit-encoding using permutation of the center. Interchangeable with encode.
  def encodePermCenter(p: Array[Int], m: Int) = {
    require(m > 0)
    val pinv = inv(p)
    val C = new Array[Long]((p.length+63)/64)
    val M = p.length / 4
    for (i <- 0 until p.length) {
      var I = i
      if ((I / M) % 3 == 0) {
        I += M
      }
      if (math.abs(I - pinv(i)) > m) {
        C(i / 64) |= (1 << (i % 64))
      }
    }
    C
  }


  case class Estimator(sketchLength: Int) extends BitEstimator {
    def estimateSimilarity(sameBits: Int): Double = ???
    def minSameBits(sim: Double): Int = ???
  }

}

object RandomBisectors {

  type DistFun[T] = (T, T) => Double

  def sketching[T](items: IndexedSeq[T], bisectors: IndexedSeq[(T, T)], dist: DistFun[T]): BitSketching =
    new BitSketchingOf(items, bisectors.length, i => mkSketcher(bisectors(i), dist), Estimator(bisectors.length))

  def apply[T](items: IndexedSeq[T], bisectors: IndexedSeq[(T, T)], dist: DistFun[T]): BitSketch[T] =
    BitSketch.make(sketching(items, bisectors, dist))


  def sketching[T](items: IndexedSeq[T], bisectors: Int, dist: DistFun[T]): BitSketching =
    sketching(items, samplePairs(items, bisectors), dist)

  def apply[T](items: IndexedSeq[T], bisectors: Int, dist: DistFun[T]): BitSketch[T] =
    apply(items, samplePairs(items, bisectors), dist)


  private def samplePairs[T](items: IndexedSeq[T], n: Int): IndexedSeq[(T, T)] = {
    // TODO sampling without repetition
    val rnd = new util.Random(1234)
    def pick() = rnd.nextInt(items.length)
    IndexedSeq.fill(n) { (items(pick()), items(pick())) }
  }


  private def mkSketcher[T](points: (T, T), dist: (T, T) => Double) = new BitSketcher[T] {
    val (a, b) = points
    def apply(item: T): Boolean = dist(a, item) < dist(b, item)
  }

  case class Estimator(sketchLength: Int) extends BitEstimator {
    def estimateSimilarity(sameBits: Int): Double = sameBits.toDouble / sketchLength
    def minSameBits(sim: Double): Int = {
      require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
      (sim * sketchLength).toInt
    }
  }

}
*/



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



object SpectralHashing {
  https://people.csail.mit.edu/torralba/publications/spectralhashing.pdf
  https://github.com/superhans/SpectralHashing/blob/master/compressSH.m
  https://github.com/wanji/sh/blob/master/sh.py
}

*/



object HammingDistance {
  def apply(arr: Array[Long], bits: Int): BitSketch[Any] = {
    require(bits % 64 == 0)

    BitSketch[Any](
      arr,
      new BitSketchers[Any] { self =>
        val sketchLength = bits
        val estimator = Estimator(bits)
        val rank = None
        def getSketchFragment(item: Any, from: Int, to: Int) = sys.error("this should not happen")
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
      val rank = None
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
