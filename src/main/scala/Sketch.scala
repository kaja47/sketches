package atrox.sketch

import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import java.util.Arrays
import atrox.Bits


// Sketcher: one locality sensitive hash function
// Sketchers: collection of locality sensitive hash functions
// Sketching: bundle of Sketchers and items to be sensitively hashed
// Sketch: materialized table of skketch arrays
//
// Data in a BitSketch must be 8 byte aligned. SketchLength may not be multiply
// of 64, but every sketch must start in new long field.


case class SketchCfg(
  maxResults: Int = Int.MaxValue,
  orderByEstimate: Boolean = false,
  compact: Boolean = true,
  parallel: Boolean = false
)



trait IntSketcher[-T] extends (T => Int) {
  /** reduces one item to one component of sketch */
  def apply(item: T): Int = multi(item).hash
  def multi(item: T): IntMulti
}

case class IntMulti(
  hash: Int,
  /** cost of flipping, smaller value means this component is more borderline and flipping can yield more suitable candidates */
  cost: Double,
  neighbour: Int
)

trait BitSketcher[-T] extends (T => Boolean) {
  /** reduces one item to one component of sketch */
  def apply(item: T): Boolean = multi(item).hash
  def multi(item: T): BitMulti
}

case class BitMulti(
  hash: Boolean,
  cost: Double
)


trait Sketchers[T, SketchArray] { self =>
  def sketchLength: Int
  def estimator: Estimator[SketchArray]
  def rank: Option[IndexedSeq[T] => Rank[T, T]]
  def uniformCost: Boolean

  def getSketchFragment(item: T): SketchArray
  def getSketchMultiFragment(item: T): MultiFragment[SketchArray]
}

case class MultiFragment[SketchArray](
  hashes: SketchArray,
  costs: Array[Double],
  neighbours: SketchArray
)

object Sketchers {
  def apply[T](sketchers: Array[IntSketcher[T]], estimator: IntEstimator, rank: Option[IndexedSeq[T] => Rank[T, T]], uniformCost: Boolean) =
    IntSketchersOf(sketchers, estimator, rank, uniformCost)
  def apply[T](n: Int, mk: Int => IntSketcher[T], estimator: IntEstimator, rank: Option[IndexedSeq[T] => Rank[T, T]], uniformCost: Boolean) =
    IntSketchersOf(Array.tabulate(n)(mk), estimator, rank, uniformCost)

  def apply[T](sketchers: Array[BitSketcher[T]], estimator: BitEstimator, rank: Option[IndexedSeq[T] => Rank[T, T]], uniformCost: Boolean) =
    BitSketchersOf(sketchers, estimator, rank, uniformCost)
  def apply[T](n: Int, mk: Int => BitSketcher[T], estimator: BitEstimator, rank: Option[IndexedSeq[T] => Rank[T, T]], uniformCost: Boolean) =
    BitSketchersOf(Array.tabulate(n)(mk), estimator, rank, uniformCost)
}

trait IntSketchers[T] extends Sketchers[T, Array[Int]]
trait BitSketchers[T] extends Sketchers[T, Array[Long]]

case class IntSketchersOf[T](
  sketchers: Array[IntSketcher[T]],
  estimator: IntEstimator,
  rank: Option[IndexedSeq[T] => Rank[T, T]],
  uniformCost: Boolean
) extends IntSketchers[T] {

  val sketchLength = sketchers.length

  def getSketchFragment(item: T): Array[Int] = {
    val res = new Array[Int](sketchLength)
    var i = 0 ; while (i < sketchLength) {
      res(i) = sketchers(i)(item)
      i += 1
    }
    res
  }

  def getSketchMultiFragment(item: T): MultiFragment[Array[Int]] = {
    val hashes     = new Array[Int](sketchLength)
    val costs      = if (uniformCost) null else new Array[Double](sketchLength)
    val neighbours = new Array[Int](sketchLength)

    var i = 0 ; while (i < sketchLength) {
      val m = sketchers(i).multi(item)
      hashes(i)     = m.hash
      if (!uniformCost) {
        costs(i)      = m.cost
      }
      neighbours(i) = m.neighbour
      i += 1
    }

    MultiFragment(hashes, costs, neighbours)
  }
}

case class BitSketchersOf[T](
  sketchers: Array[BitSketcher[T]],
  estimator: BitEstimator,
  rank: Option[IndexedSeq[T] => Rank[T, T]],
  uniformCost: Boolean
) extends BitSketchers[T] {

  val sketchLength = sketchers.length

  def getSketchFragment(item: T): Array[Long] = {
    val res = new Array[Long]((sketchLength+63)/64)
    var i = 0 ; while (i < sketchLength) {
      val s = sketchers(i)(item)
      val bit = if (s) 1L else 0L
      res(i / 64) |= (bit << (i % 64))
      i += 1
    }
    res
  }

  def getSketchMultiFragment(item: T): MultiFragment[Array[Long]] = {
    val hashes = new Array[Long]((sketchLength+63)/64)
    val costs  = new Array[Double](sketchLength)

    var i = 0 ; while (i < sketchLength) {
      val m = sketchers(i).multi(item)
      val bit = (if (m.hash) 1L else 0L)
      hashes(i / 64) |= (bit << (i % 64))
      costs(i) = m.cost
      i += 1
    }
    MultiFragment(hashes, costs, null)
  }

}


object Sketching {
  type IntSketching[T] = Sketching[T, Array[Int]]
  type BitSketching[T] = Sketching[T, Array[Long]]
}

sealed abstract class Sketching[T, SketchArray] { self =>
  def itemsCount: Int
  def sketchLength: Int
  def estimator: Estimator[SketchArray]
  def sketchers: Sketchers[T, SketchArray]

  def getSketchFragment(itemIdx: Int): SketchArray
}

case class SketchingOf[T, SketchArray](
  items: IndexedSeq[T],
  sketchers: Sketchers[T, SketchArray]
) extends Sketching[T, SketchArray] {

  val sketchLength = sketchers.sketchLength
  val itemsCount = items.length
  val estimator = sketchers.estimator

  def getSketchFragment(itemIdx: Int): SketchArray =
    sketchers.getSketchFragment(items(itemIdx))
}



object Sketch {
  def apply[T](items: Seq[T], sk: IntSketchers[T]) = IntSketch(items, sk)
  def apply[T](items: Seq[T], sk: BitSketchers[T]) = BitSketch(items, sk)
  def apply(items: Array[Long], sk: BitSketchers[Nothing]) = BitSketch[Nothing](items, sk)

  def apply[T, SketchArray](items: Seq[T], sk: Sketchers[T, SketchArray]): Sketch[T, SketchArray] = sk match {
    case sk: IntSketchers[T @unchecked] => IntSketch(items, sk)
    case sk: BitSketchers[T @unchecked] => BitSketch(items, sk)
  }
}

sealed abstract class Sketch[T, SketchArray] extends Sketching[T, SketchArray] with Serializable {

  type Idxs = Array[Int]
  type SimFun = (Int, Int) => Double

  def sketchArray: SketchArray
  def itemsCount: Int

  def withConfig(cfg: SketchCfg): Sketch[T, SketchArray]

  def estimator: Estimator[SketchArray]
  def cfg: SketchCfg
  def sameBits(idxA: Int, idxB: Int): Int = estimator.sameBits(sketchArray, idxA, sketchArray, idxB)
  def estimateSimilarity(idxA: Int, idxB: Int): Double = estimator.estimateSimilarity(sameBits(idxA, idxB))
  def minSameBits(sim: Double): Int = estimator.minSameBits(sim)

  def similarIndexes(idx: Int, minEst: Double): Idxs = similarIndexes(idx, minEst, 0.0, null)
  def similarIndexes(idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs = {
    val minBits = estimator.minSameBits(minEst)
    val res = new collection.mutable.ArrayBuilder.ofInt
    var i = 0 ; while (i < itemsCount) {
      val bits = sameBits(idx, i)
      if (bits >= minBits && idx != i && (f == null || f(i, idx) >= minSim)) {
        res += i
      }
      i += 1
    }
    res.result
  }

  def similarItems(idx: Int, minEst: Double): Iterator[Sim] = similarItems(idx, minEst, 0.0, null)
  def similarItems(idx: Int, minEst: Double, minSim: Double, f: SimFun): Iterator[Sim] = {
    val minBits = estimator.minSameBits(minEst)
    val res = new collection.mutable.ArrayBuffer[Sim]
    var i = 0 ; while (i < itemsCount) {
      val bits = sameBits(idx, i)
      var sim: Double = 0.0
      if (bits >= minBits && idx != i && (f == null || { sim = f(i, idx) ; sim >= minSim })) {
        res += Sim(i, if (f != null) sim else estimator.estimateSimilarity(bits))
      }
      i += 1
    }
    res.iterator
  }

  /*
  def allSimilarIndexes(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, Idxs)] = {
    val minBits = estimator.minSameBits(minEst)

    (cfg.compact, cfg.parallel) match {
      // this needs in the worst case O(n * s / 4) additional space, where n is the
      // number of items in this sketch and s is average number of similar
      // indexes
      case (false, false) =>
        val stripeSize = 64
        val res = Array.fill(itemsCount)(IndexResultBuilder.make(false, cfg.maxResults)

        Iterator.range(0, itemsCount, step = stripeSize) flatMap { start =>

          stripeRun(stripeSize, start, itemsCount, minBits, minSim, f, true, new Op {
            def apply(i: Int, j: Int, est: Double, sim: Double): Unit = {
              res(i) += (j, sim)
              res(j) += (i, sim)
            }
          })

          val endi = math.min(start + stripeSize, itemsCount)
          Iterator.range(start, endi) map { i =>
            val arr = res(i).result
            res(i) = null
            (i, arr)
          } filter { _._2.nonEmpty }
        }

      // this needs no additional memory but it have to do full n**2 iterations
      case (_, _) =>
        //Iterator.tabulate(itemsCount) { idx => (idx, similarIndexes(idx, minEst, minSim, f)) }

        val stripeSize = 64
        val stripesInParallel = if (cfg.parallel) 16 else 1
        println(s"copmact, stripesInParallel $stripesInParallel")

        Iterator.range(0, itemsCount, step = stripeSize * stripesInParallel) flatMap { pti =>
          val res = Array.fill(stripeSize * stripesInParallel)(IndexResultBuilder.make(false, cfg.maxResults)

          parStripes(stripesInParallel, stripeSize, pti, itemsCount) { start =>
            stripeRun(stripeSize, start, itemsCount, minBits, minSim, f, false, new Op {
              def apply(i: Int, j: Int, est: Double, sim: Double): Unit = {
                res(i-pti) += (j, sim)
              }
            })
          }

          Iterator.tabulate(res.length) { i => (i + pti, res(i).result) } filter { _._2.nonEmpty }
        }
    }
  }
  def allSimilarIndexes(minEst: Double): Iterator[(Int, Idxs)] =
    allSimilarIndexes(minEst, 0.0, null)
  def allSimilarIndexes: Iterator[(Int, Idxs)] =
    allSimilarIndexes(0.0, 0.0, null)

  def allSimilarItems(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, Iterator[Sim])] = {
    val ff = if (cfg.orderByEstimate) null else f
    allSimilarIndexes(minEst, minSim, ff) map { case (idx, simIdxs) => (idx, indexesToSims(idx, simIdxs, f, sketchArray)) }
  }
  def allSimilarItems(minEst: Double): Iterator[(Int, Iterator[Sim])] =
    allSimilarItems(minEst, 0.0, null)
  def allSimilarItems: Iterator[(Int, Iterator[Sim])] =
    allSimilarItems(0.0, 0.0, null)

  // === internal cruft ===

  protected def parStripes(stripesInParallel: Int, stripeSize: Int, base: Int, length: Int)(f: (Int) => Unit): Unit = {
    if (stripesInParallel > 1) {
      val end = math.min(base + stripeSize * stripesInParallel, length)
      (base until end by stripeSize).par foreach { b =>
        f(b)
      }
    } else {
      f(base)
    }
  }

  // striping/tiling leads to better cache usage patterns and that subsequently leads to better performance
  protected def stripeRun(stripeSize: Int, stripeBase: Int, length: Int, minBits: Int, minSim: Double, f: SimFun, half: Boolean, op: Op): Unit = {
    val endi = math.min(stripeBase + stripeSize, length)
    val startj = if (!half) 0 else stripeBase + 1

    var j = startj ; while (j < length) {
      val realendi = if (!half) endi else math.min(j, endi)
      var i = stripeBase ; while (i < realendi) {
        val bits = sameBits(i, j)
        var sim = 0.0
        if (bits >= minBits && i != j && (f == null || { sim = f(i, j) ; sim >= minSim })) {
          val est = estimator.estimateSimilarity(bits)
          op.apply(i, j, est, if (f == null) est else sim)
        }
        i += 1
      }
      j += 1
    }
  }
  */

  protected abstract class Op { def apply(thisIdx: Int, thatIdx: Int, est: Double, sim: Double): Unit }

  protected def indexesToSims(idx: Int, simIdxs: Idxs, f: SimFun, sketch: SketchArray) =
    simIdxs.iterator.map { simIdx =>
      val est = estimator.estimateSimilarity(sketch, idx, sketch, simIdx)
      Sim(simIdx, if (f != null) f(idx, simIdx) else est)
    }
}

trait Estimator[SketchArray] {
  def sketchLength: Int

  def minSameBits(sim: Double): Int
  def estimateSimilarity(sameBits: Int): Double

  def sameBits(arrA: SketchArray, idxA: Int, arrB: SketchArray, idxB: Int): Int
  def estimateSimilarity(arrA: SketchArray, idxA: Int, arrB: SketchArray, idxB: Int): Double =
    estimateSimilarity(sameBits(arrA, idxA, arrB, idxB))
}


trait IntEstimator extends Estimator[Array[Int]] {
  def sameBits(arrA: Array[Int], idxA: Int, arrB: Array[Int], idxB: Int) = {
    var a = idxA * sketchLength
    var b = idxB * sketchLength
    var same = 0
    val end = a + sketchLength
    while (a < end) {
      same += (if (arrA(a) == arrB(b)) 1 else 0)
      a += 1
      b += 1
    }
    same
  }
}

trait BitEstimator extends Estimator[Array[Long]] {
  def sameBits(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int) = {
    val longsLen = (sketchLength+63) / 64 // assumes sketch is Long aligned
    val a = idxA * longsLen
    val b = idxB * longsLen
    var i = 0
    var same = sketchLength
    while (i < longsLen) {
      same -= bitCount(arrA(a+i) ^ arrB(b+i))
      i += 1
    }
    same
  }
}


trait BitEstimator64 extends BitEstimator {
  override def sameBits(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int): Int = {
    64 - bitCount(arrA(idxA) ^ arrB(idxB))
  }
}

trait BitEstimator128 extends BitEstimator {
  override def sameBits(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int): Int = {
    var same = 128
    same -= bitCount(arrA(idxA*2)   ^ arrB(idxB*2))
    same -= bitCount(arrA(idxA*2+1) ^ arrB(idxB*2+1))
    same
  }
}

trait BitEstimator256 extends BitEstimator {
  override def sameBits(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int): Int = {
    var same = 256
    same -= bitCount(arrA(idxA*4)   ^ arrB(idxB*4))
    same -= bitCount(arrA(idxA*4+1) ^ arrB(idxB*4+1))
    same -= bitCount(arrA(idxA*4+2) ^ arrB(idxB*4+2))
    same -= bitCount(arrA(idxA*4+3) ^ arrB(idxB*4+3))
    same
  }
}






object IntSketch {
  def makeSketchArray[T](sk: IntSketchers[T], items: Seq[T]): Array[Int] = {
    val sketchArray = new Array[Int](items.length * sk.sketchLength)
    for ((item, itemIdx) <- items.iterator.zipWithIndex) {
      val arr = sk.getSketchFragment(item)
      System.arraycopy(arr, 0, sketchArray, itemIdx * sk.sketchLength, sk.sketchLength)
    }
    sketchArray
  }

  def apply[T](items: Seq[T], sk: IntSketchers[T]): IntSketch[T] =
    IntSketch(makeSketchArray(sk, items), sk)
}


case class IntSketch[T](
  sketchArray: Array[Int],
  sketchers: Sketchers[T, Array[Int]],
  cfg: SketchCfg = SketchCfg()
) extends Sketch[T, Array[Int]] {

  val sketchLength = sketchers.sketchLength
  val itemsCount = sketchArray.length / sketchLength
  val estimator = sketchers.estimator

  def withConfig(_cfg: SketchCfg): IntSketch[T] = copy(cfg = _cfg)

  def getSketchFragment(itemIdx: Int): Array[Int] =
    Arrays.copyOfRange(sketchArray, itemIdx * sketchLength, (itemIdx+1) * sketchLength)

  def sketeches: Iterator[Array[Int]] =
    Iterator.tabulate(itemsCount) { i => getSketchFragment(i) }
}



object BitSketch {
  def makeSketchArray[T](sk: BitSketchers[T], items: Seq[T]): Array[Long] = {
    val longsLen = (sk.sketchLength+63) / 64 // assumes sketch is Long aligned
    val sketchArray = new Array[Long](items.length * longsLen)
    for ((item, itemIdx) <- items.iterator.zipWithIndex) {
      val arr = sk.getSketchFragment(item)
      System.arraycopy(arr, 0, sketchArray, itemIdx * longsLen, arr.length)
    }
    sketchArray
  }

  def apply[T](items: Seq[T], sk: BitSketchers[T]): BitSketch[T] =
    BitSketch(makeSketchArray(sk, items), sk)
}


case class BitSketch[T](
  sketchArray: Array[Long],
  sketchers: Sketchers[T, Array[Long]],
  cfg: SketchCfg = SketchCfg()
) extends Sketch[T, Array[Long]] {

  val sketchLength = sketchers.sketchLength
  val itemsCount = sketchArray.length * 64 / sketchLength
  val estimator = sketchers.estimator
  val bitsPerSketch = sketchLength

  def withConfig(_cfg: SketchCfg): BitSketch[T] = copy(cfg = _cfg)

  def getSketchFragment(itemIdx: Int): Array[Long] =
    Bits.getBits(sketchArray, itemIdx * bitsPerSketch, (itemIdx+1) * bitsPerSketch)
}
