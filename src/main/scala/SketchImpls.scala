package atrox.sketch

import scala.language.postfixOps
import breeze.linalg.{ SparseVector, DenseVector, DenseMatrix, BitVector, normalize, Vector => bVector, operators, norm }
import breeze.stats.distributions.Rand
import atrox.fastSparse



object MinHash {

  def apply[T](hashFunctions: Int)(implicit mk: MinHashImpl[T, Any]): IntSketchers[T] =
    weighted(hashFunctions, (a: Any) => 1)

  /** based on https://www.sumologic.com/2015/10/22/rapid-similarity-search-with-weighted-min-hash/ */
  def weighted[T, El](hashFunctions: Int, weights: El => Int)(implicit mk: MinHashImpl[T, El]): IntSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => mk(HashFunc.random(i*1000), weights), Estimator(hashFunctions), Some(mk.mkRank), true)

  /** MinHash that uses only one bit. It's much faster than traditional MinHash
    * but it seems it's less precise.
    * As of right now it's not really suitable for LSH, because most elements
    * are hashed into few buckets. Investigation pending.
    * https://www.endgame.com/blog/minhash-vs-bitwise-set-hashing-jaccard-similarity-showdown */
  def singleBit[T](hashFunctions: Int)(implicit mk: MinHashImpl[T, Any]): BitSketchers[T] =
    singleBitWeighted(hashFunctions, (a: Any) => 1)

  def singleBitWeighted[T, El](hashFunctions: Int, weights: El => Int)(implicit mk: MinHashImpl[T, El]): BitSketchers[T] =
    Sketchers(hashFunctions, (i: Int) => onebit(mk(HashFunc.random(i*1000), weights)), SingleBitEstimator(hashFunctions), Some(mk.mkRank), true)

  private def onebit[T](f: IntSketcher[T]) = new BitSketcher[T] {
    override def apply(t: T) = (f(t) & 1) != 0
    def multi(t: T) = {
      val m = f.multi(t)
      BitMulti((m.hash & 1) != 0, m.cost)
    }
  }


  trait MinHashImpl[T, +El] {
    def apply(hf: HashFunc[Int], weights: El => Int): IntSketcher[T]
    def mkRank: IndexedSeq[T] => Rank[T, T]
  }

  implicit val IntArrayMinHashImpl = new MinHashImpl[Array[Int], Int] {
    type Item = Array[Int]
    def apply(hf: HashFunc[Int], weights: Int => Int) = new IntSketcher[Item] {
      override def apply(set: Item) = minhashArrInt(set, hf, weights)
      def multi(set: Item) = minhashArr(set, hf, weights)
    }
    def mkRank = (items: IndexedSeq[Item]) => SimFun[Item](fastSparse.jaccardSimilarity, items)
  }

  implicit def GeneralMinHashImpl[El] = new MinHashImpl[Set[El], El] {
    type Item = Set[El]
    def apply(hf: HashFunc[Int], weights: El => Int) = new IntSketcher[Item] {
      //def apply (set: Item) = minhashTrav(set, hf, weights)
      def multi(set: Item) = minhashTrav(set, hf, weights)
    }
    def mkRank = (items: IndexedSeq[Item]) => SimFun[Item](jacc, items)
  }


  private def minhashArrInt(set: Array[Int], f: HashFunc[Int], weights: Int => Int): Int = {
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

  private def minhashArr(set: Array[Int], f: HashFunc[Int], weights: Int => Int): IntMulti = {
    var min, min2 = Int.MaxValue
    var j = 0 ; while (j < set.length) {
      var h = set(j)
      var i = 0 ; while (i < weights(j)) {
        h = f(h)
        //min = math.min(min, h)
        if (h < min) {
          min2 = math.min(min2, min)
          min = h
        }
        if (h != min) {
          min2 = math.min(min2, h)
        }
        i += 1
      }
      j += 1
    }
    IntMulti(min, 1, min2)
  }

  private def minhashTrav[El](set: Set[El], f: HashFunc[Int], weights: El => Int): IntMulti = {
    var min, min2 = Int.MaxValue
    for (el <- set) {
      var h = el.hashCode
      for (_ <- 0 until weights(el)) {
        h = f(h)
        //min = math.min(min, h)
        if (h < min) {
          min2 = math.min(min2, min)
          min = h
        }
        if (h != min) {
          min2 = math.min(min2, h)
        }
      }
    }
    IntMulti(min, 1, min2)
  }

  protected def jacc[El](a: Set[El], b: Set[El]) = {
    val small = if (a.size < b.size) a else b
    val large = if (a.size < b.size) b else a

    var in = 0
    for (el <- small) {
      if (large.contains(el)) in += 1
    }

    val un = small.size + large.size - in
    in.toDouble / un
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



/** Estimates cosine of the angle between two vectors. */
object RandomHyperplanes {
  def apply[T](n: Int, vectorLength: Int, normalized: Boolean = false)(implicit ev: CanDot[T]): BitSketchers[T] = {
    Sketchers(n, (i: Int) => mkSketcher(vectorLength, i * 1000), Estimator(n), Some(mkRank(ev, normalized)), false)
  }

//  def apply(rowMatrix: DenseMatrix[Double], n: Int): BitSketch[DenseVector[Double]] = ???
//    apply(0 until rowMatrix.rows map { r => rowMatrix(r, ::).t }, n)(CanDotDouble)


  private def mkRank[InVec](ev: CanDot[InVec], norm: Boolean) =
    if (norm) (items: IndexedSeq[InVec]) => SimFun[InVec]((a, b) => ev.dotInVec(a, b), items)
    else      (items: IndexedSeq[InVec]) => SimFun[InVec]((a, b) => ev.dotInVec(a, b) / (ev.normInVec(a) * ev.normInVec(b)), items)

  trait CanDot[InVec] {
    type RndVec
    def makeRandomHyperplane(length: Int, seed: Int): RndVec
    def dotRndVec(a: InVec, b: RndVec): Double
    def dotInVec(a: InVec, b: InVec): Double
    def normRndVec(a: RndVec): Double
    def normInVec(a: InVec): Double
  }

  type Mul[A, B, C] = operators.OpMulInner.Impl2[A, B, C]
  type Norm[A, B, C] = norm.Impl2[A, B, C]

  implicit def CanDotFloat[T](implicit
      dotf1: Mul[T, DenseVector[Float], Float],
      dotf2: Mul[T, T, Float],
      normf1: Norm[DenseVector[Float], Double, Double],
      normf2: Norm[T, Double, Double]
    ) = new CanDot[T] {

    type RndVec = DenseVector[Float]

    def makeRandomHyperplane(length: Int, seed: Int): RndVec = mkRandomHyperplane(length, seed) mapValues (_.toFloat)
    def dotRndVec(a: T, b: RndVec): Double = dotf1(a, b).toDouble
    def dotInVec(a: T, b: T): Double = dotf2(a, b).toDouble
    def normRndVec(a: RndVec): Double = normf1(a, 2)
    def normInVec(a: T): Double = normf2(a, 2)
  }

  implicit def CanDotDouble[T](implicit
      dotf1: Mul[T, DenseVector[Double], Double],
      dotf2: Mul[T, T, Double],
      normf1: Norm[DenseVector[Double], Double, Double],
      normf2: Norm[T, Double, Double]
    ) = new CanDot[T] {

    type RndVec = DenseVector[Double]

    def makeRandomHyperplane(length: Int, seed: Int): RndVec = mkRandomHyperplane(length, seed)
    def dotRndVec(a: T, b: RndVec): Double = dotf1(a, b)
    def dotInVec(a: T, b: T): Double = dotf2(a, b)
    def normRndVec(a: RndVec): Double = normf1(a, 2)
    def normInVec(a: T): Double = normf2(a, 2)
  }

  implicit def CanDotSparseDouble = new CanDot[SparseVector[Double]] {
    trait RndVec {
      def apply(i: Int): Double
      def norm: Double
    }

    def makeRandomHyperplane(length: Int, seed: Int) = {
      val f = HashFunc.random(seed, 16)

      new RndVec {
        def apply(i: Int): Double = {
          val j = i+1337
          val bit = (f(j/16) >> (j%16)) & 1
          if (bit == 1) 1.0 else -1.0
        }
        val norm: Double = math.sqrt(length)
      }
    }

    def dotRndVec(a: SparseVector[Double], b: RndVec): Double = {
      var d = 0.0
      var offset = 0
      while (offset < a.activeSize) {
        val i = a.indexAt(offset)
        val v = a.valueAt(offset)
        d += v * b(i)
        offset += 1
      }
      d
    }

    def dotInVec(a: SparseVector[Double], b: SparseVector[Double]) = a dot b

    def normInVec(a: SparseVector[Double]): Double = norm(a, 2)
    def normRndVec(a: RndVec): Double = a.norm
  }


  private def mkSketcher[T](length: Int, seed: Int)(implicit ev: CanDot[T]): BitSketcher[T] = new BitSketcher[T] {
    private val rand = ev.makeRandomHyperplane(length, seed)
    //def apply(item: T) = ev.dotRndVec(item, rand) > 0.0
    def multi(item: T) = {
      val dot = ev.dotRndVec(item, rand)
      BitMulti(dot > 0.0, math.abs(dot))
    }
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




/** Estimates distance */
object RandomProjections {

  def apply[V](projections: Int, bucketSize: Double, vectorLength: Int): IntSketchers[bVector[Double]] =
    Sketchers(projections, (i: Int) => mkSketcher(vectorLength, i * 1000, bucketSize), Estimator(projections), None, false)


  private def mkSketcher(vectorLength: Int, seed: Int, bucketSize: Double) =
    new IntSketcher[bVector[Double]] {
      private val randVec = mkRandomUnitVector(vectorLength, seed)

//      def apply(item: bVector[Double]): Int =
//        ((randVec dot item) / bucketSize).toInt

      def multi(item: bVector[Double]) = {
        val d = ((randVec dot item) / bucketSize)
        val bucket = d.toInt
        val neighbour = bucket + (if (d < bucket+0.5) -1 else +1)
        val cost = 0.5 - math.abs(bucket+0.5-d)
        IntMulti(bucket, cost, neighbour)
      }
    }

  private def mkRandomUnitVector(length: Int, seed: Int) = {
    val rand = new scala.util.Random(seed)
    normalize(DenseVector.fill[Double](length)(rand.nextGaussian), 2)
    //normalize(DenseVector.rand[Double](length, Rand.gaussian), 2)
  }

  // def mkRank = (items: IndexedSeq[T]) => SimFun((a, b) => sum(pow(a - b, 2)), items)

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

  def apply(arr: Array[Long], bits: Int): BitSketch[Array[Long]] = {
    require(bits % 64 == 0)

    val sketchers: BitSketchers[Array[Long]] = new BitSketchers[Array[Long]] { self =>
      val sketchLength = bits
      val estimator = Estimator(bits)
      val rank = None
      val uniformCost = true

      def getSketchFragment(item: Array[Long]) = {
        require(item.length*64 == bits)
        item
      }
      def getSketchMultiFragment(item: Array[Long]) =
        MultiFragment(getSketchFragment(item), null, null)
    }

    BitSketch[Array[Long]](arr, sketchers)
  }

  def apply(arr: Array[Array[Long]], bits: Int): BitSketch[Array[Long]] = {
    val longs = (bits+63) / 64
    val len = longs * arr.length
    val res = new Array[Long](len)
    var i = 0 ; while (i < len) {
      var j = 0 ; while (j < longs) {
        res(i*longs + j) = arr(i)(j)
        j += 1
      }
      i += 1
    }
    apply(res, bits)
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
      val uniformCost = true

      def getSketchFragment(item: Array[T]): Array[Long] =
        Array[Long](doSimHash64(item, f))
      def getSketchMultiFragment(item: Array[T]) = ???
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
