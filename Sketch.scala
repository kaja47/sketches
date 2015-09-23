package atrox.sketch

import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import java.util.Arrays


// {Bit/Int}Sketching - a class that can produce sketches, contains Estimator
// {Bit/Int}Sketch - a container of sketches, extends Sketching, contains Estimator
// {Bit/Int}Estimator - a class that can compute similarity estimates from provided sketch arrays


trait HashFunc[@scala.specialized(Int, Long) T] extends Serializable {
  def apply(x: T): Int
}

trait HashFuncLong[T] extends Serializable {
  def apply(x: T): Long
}

trait Sketching[SketchArray] {

  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: SketchArray, destBitOffset: Int): Unit
  def writeSketchFragment(itemIdx: Int, dest: SketchArray, destBitOffset: Int): Unit
  def getSketchFragment(item: Int, from: Int, to: Int): SketchArray
  def getSketchFragment(item: Int): SketchArray
}


trait Sketch[SketchArray] extends Serializable {

  type Idxs = Array[Int]
  type SimFun = (Int, Int) => Double

  def sketchArray: SketchArray
  def length: Int

  def estimator: Estimator[SketchArray]
  def sameBits(idxA: Int, idxB: Int): Int
  def estimateSimilarity(idxA: Int, idxB: Int): Double = estimator.estimateSimilarity(sameBits(idxA, idxB))
  def minSameBits(sim: Double): Int = estimator.minSameBits(sim)
  def empty: Sketch[SketchArray]

  def similarIndexes(idx: Int, minEst: Double): Idxs = similarIndexes(idx, minEst, 0.0, null)
  def similarIndexes(idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs = {
    val minBits = estimator.minSameBits(minEst)
    val res = new collection.mutable.ArrayBuilder.ofInt
    var i = 0 ; while (i < length) {
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
    var i = 0 ; while (i < length) {
      val bits = sameBits(idx, i)
      var sim: Double = 0.0
      if (bits >= minBits && idx != i && (f == null || { sim = f(i, idx) ; sim >= minSim })) {
        res += Sim(idx, i, estimator.estimateSimilarity(bits), sim)
      }
      i += 1
    }
    res.iterator
  }

  def allSimilarIndexes(minEst: Double, minSim: Double, f: SimFun, compact: Boolean = false): Iterator[(Int, Idxs)] = {
    val minBits = estimator.minSameBits(minEst)

    // this needs in the worst case O(n * s / 4) extra memory, where n is the
    // number of items in this sketch and s is average number of similar
    // indexes
    if (!compact) {
      val res = Array.fill(length)(new collection.mutable.ArrayBuilder.ofInt)
      Iterator.tabulate(length) { i =>
        var j = i+1 ; while (j < length) {
          val bits = sameBits(i, j)
          if (bits >= minBits && i != j && (f == null || f(i, j) >= minSim)) {
            res(i) += j
            res(j) += i
          }
          j += 1
        }
        val arr = res(i).result
        res(i) = null
        (i, arr)
      }

    // this needs no additional memory but it have to do full n**2 iterations
    } else {
      Iterator.tabulate(length) { idx => (idx, similarIndexes(idx, minEst, minSim, f)) }
    }
  }
  def allSimilarIndexes(minEst: Double): Iterator[(Int, Idxs)] =
    allSimilarIndexes(minEst, 0.0, null)


  def allSimilarItems(minEst: Double, minSim: Double, f: SimFun, compact: Boolean = false): Iterator[(Int, Iterator[Sim])] = {
    val minBits = estimator.minSameBits(minEst)

    if (!compact) {
      val res = Array.fill(length)(new collection.mutable.ArrayBuilder.ofRef[Sim])
      Iterator.tabulate(length) { i =>
        var j = i+1 ; while (j < length) {
          val bits = sameBits(i, j)
          var sim: Double = 0.0
          if (bits >= minBits && i != j && (f == null || { sim = f(i, j) ; sim >= minSim })) {
            val est = estimator.estimateSimilarity(bits)
            res(i) += Sim(i, j, est, sim)
            res(j) += Sim(j, i, est, sim)
          }
          j += 1
        }
        val arr = res(i).result
        res(i) = null
        (i, arr.iterator)
      }

    } else {
      Iterator.tabulate(length) { idx => (idx, similarItems(idx, minEst, minSim, f)) }
    }
  }
  def allSimilarItems(minEst: Double): Iterator[(Int, Iterator[Sim])] =
    allSimilarItems(minEst, 0.0, null)
}

trait Estimator[SketchArray] {
  def minSameBits(sim: Double): Int
  def estimateSimilarity(sameBits: Int): Double

  def sameBits(arrA: SketchArray, idxA: Int, arrB: SketchArray, idxB: Int): Int
  def estimateSimilarity(arrA: SketchArray, idxA: Int, arrB: SketchArray, idxB: Int): Double
}


// === IntSketching ============================================================


trait IntEstimator extends Estimator[Array[Int]] {
  def sketchLength: Int

  def minSameBits(sim: Double): Int
  def estimateSimilarity(sameBits: Int): Double

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

  def estimateSimilarity(arrA: Array[Int], idxA: Int, arrB: Array[Int], idxB: Int): Double =
    estimateSimilarity(sameBits(arrA, idxA, arrB, idxB))
}


trait IntSketching extends Sketching[Array[Int]] { self =>

  def sketchLength: Int
  def length: Int
  def estimator: IntEstimator

  /** @param itemIdx index of source item
    * @param from index of first sketch component (inclusive)
    * @param to index of last sketch component (exclusive)
    * @param dest sketch array where sketch data must be written
    * @param destOffset index in sketch array that is ready to be filled with sketch data
    */
  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Int], destOffset: Int): Unit

  def writeSketchFragment(itemIdx: Int, dest: Array[Int], destOffset: Int): Unit =
    writeSketchFragment(itemIdx, 0, sketchLength, dest, destOffset)

  def getSketchFragment(item: Int, from: Int, to: Int): Array[Int] = {
    val res = new Array[Int](to-from)
    writeSketchFragment(item, from, to, res, 0)
    res
  }

  def getSketchFragment(item: Int): Array[Int] =
    getSketchFragment(item, 0, sketchLength)

  def slice(_from: Int, _to: Int): IntSketching = new IntSketching {
    val sketchLength = _to - _from
    val length = self.length
    val estimator = self.estimator
    def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Int], destOffset: Int): Unit =
      self.writeSketchFragment(itemIdx, _from + from, _from + to, dest, destOffset)
  }

}


case class IntSketchingOf[T](
  items: Seq[T],
  _sketchers: Array[() => IntSketcher[T]],
  estimator: IntEstimator
) extends IntSketching {

  def this(items: Seq[T], n: Int, mkSketcher: Int => IntSketcher[T], estimator: IntEstimator) =
    this(items, 0 until n map { i => () => mkSketcher(i) } toArray, estimator)

  lazy val sketchers: Array[IntSketcher[T]] = _sketchers map { _.apply }

  val sketchLength = _sketchers.length
  val length = items.length

  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Int], destOffset: Int): Unit = {
    var i = destOffset
    var j = from

    while (j < to) {
      dest(i) = sketchers(j)(items(itemIdx))
      i += 1
      j += 1
    }
  }

  override def slice(_from: Int, _to: Int) = copy(_sketchers = _sketchers.slice(_from, _to))

}



trait IntSketcher[-T] {
  /** reduces one item to one component of sketch */
  def apply(item: T): Int
}



object IntSketch {
  def makeSketchArray(sk: IntSketching, n: Int): Array[Int] = {
    val sketchArray = new Array[Int](sk.length * sk.sketchLength)
    for (component <- 0 until sk.sketchLength by n) {
      val slice = sk.slice(component, component+n)
      for (itemIdx <- 0 until sk.length) {
        slice.writeSketchFragment(itemIdx, 0, n, sketchArray, itemIdx * sk.sketchLength + component)
      }
    }
    sketchArray
  }

  def make[T](sk: IntSketching, estimator: IntEstimator, componentsAtOnce: Int = 0): IntSketch = {
    val n = if (componentsAtOnce <= 0) sk.sketchLength else componentsAtOnce
    IntSketch(makeSketchArray(sk, n), sk.sketchLength, estimator)
  }

}


case class IntSketch(
  sketchArray: Array[Int],
  sketchLength: Int,
  estimator: IntEstimator
) extends Sketch[Array[Int]] with IntSketching {

  val length = sketchArray.length / sketchLength

  def sameBits(idxA: Int, idxB: Int): Int =
    estimator.sameBits(sketchArray, idxA, sketchArray, idxB)

  def empty = copy(sketchArray = null)

  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Int], destOffset: Int): Unit = {
    var i = destOffset
    var j = from

    while (j < to) {
      dest(i) = sketchArray(itemIdx * sketchLength + j)
      i += 1
      j += 1
    }
  }

  def sketeches: Iterator[Array[Int]] =
    (0 until (sketchArray.length/sketchLength)).iterator map { i =>
      Arrays.copyOfRange(sketchArray, i*sketchLength, (i+1)*sketchLength)
    }

}



// === BitSketching ============================================================


trait BitEstimator extends Estimator[Array[Long]] {
  def sketchLength: Int

  def minSameBits(sim: Double): Int
  def estimateSimilarity(sameBits: Int): Double

  def sameBits(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int) = {
    val longsLen = sketchLength / 64
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

  def estimateSimilarity(arrA: Array[Long], idxA: Int, arrB: Array[Long], idxB: Int): Double =
    estimateSimilarity(sameBits(arrA, idxA, arrB, idxB))
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


trait BitSketching extends Sketching[Array[Long]] { self =>

  def sketchLength: Int
  def length: Int
  def estimator: BitEstimator

  /** @param itemIdx index of source item
    * @param from index of first sketch component (inclusive)
    * @param to index of last sketch component (exclusive)
    * @param dest sketch array where sketch data must be written
    * @param destBitOffset bit index in sketch array that is ready to be filled with sketch data
    */
  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Long], destBitOffset: Int): Unit

  def writeSketchFragment(itemIdx: Int, dest: Array[Long], destBitOffset: Int): Unit =
    writeSketchFragment(itemIdx, 0, sketchLength, dest, destBitOffset)

  def getSketchFragment(item: Int, from: Int, to: Int): Array[Long] = {
    val res = new Array[Long]((to-from)/64)
    writeSketchFragment(item, from, to, res, 0)
    res
  }

  def getSketchFragment(item: Int): Array[Long] =
    getSketchFragment(item, 0, sketchLength)

  def getSketchFragmentAsLong(item: Int, from: Int, to: Int): Long = {
    val res = new Array[Long](1) // this allocation should end up stack allocated
    writeSketchFragment(item, from, to, res, 0)
    res(0)
  }


  def slice(_from: Int, _to: Int): BitSketching = new BitSketching {
    val sketchLength = _to - _from
    val length = self.length
    val estimator = self.estimator
    def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Long], destBitOffset: Int): Unit =
      self.writeSketchFragment(itemIdx, _from + from, _from + to, dest, destBitOffset)
  }

}


case class BitSketchingOf[T](
  items: Seq[T],
  _sketchers: Array[() => BitSketcher[T]],
  estimator: BitEstimator
) extends BitSketching {

  def this(items: Seq[T], n: Int, mkSketcher: Int => BitSketcher[T], estimator: BitEstimator) =
    this(items, 0 until n map { i => () => mkSketcher(i) } toArray, estimator)

  lazy val sketchers: Array[BitSketcher[T]] = _sketchers map { _.apply }

  val sketchLength = _sketchers.length
  val length = items.length

  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Long], destBitOffset: Int): Unit = {
    var i = destBitOffset
    var j = from

    while (j < to) {
      val s = sketchers(j)(items(itemIdx))
      if (s) {
        dest(i / 64) |= (1L << (i % 64))
      }
      i += 1
      j += 1
    }
  }

  override def slice(_from: Int, _to: Int) = copy(_sketchers = _sketchers.slice(_from, _to))

}


trait BitSketcher[-T] {
  /** reduces one item to one component of sketch */
  def apply(item: T): Boolean
}


object BitSketch {
  def makeSketchArray(sk: BitSketching, n: Int): Array[Long] = {
    val sketchArray = new Array[Long](sk.length * sk.sketchLength / 64)
    for (component <- 0 until sk.sketchLength by n) {
      val slice = sk.slice(component, component+n)
      for (itemIdx <- 0 until sk.length) {
        slice.writeSketchFragment(itemIdx, 0, sk.sketchLength, sketchArray, itemIdx * sk.sketchLength + component)
      }
    }
    sketchArray
  }

  def make[T](sk: BitSketching, estimator: BitEstimator, componentsAtOnce: Int): BitSketch = {
    val n = if (componentsAtOnce <= 0) sk.sketchLength else componentsAtOnce
    BitSketch(makeSketchArray(sk, n), sk.sketchLength, sk.estimator)
  }

}


case class BitSketch(
  sketchArray: Array[Long],
  sketchLength: Int,
  estimator: BitEstimator
) extends Sketch[Array[Long]] with BitSketching {

  def length = sketchArray.length * 64 / sketchLength
  def bitsPerSketch = sketchLength

  def sameBits(idxA: Int, idxB: Int): Int =
    estimator.sameBits(sketchArray, idxA, sketchArray, idxB)

  def empty = copy(sketchArray = null)

  def writeSketchFragment(itemIdx: Int, from: Int, to: Int, dest: Array[Long], destBitOffset: Int): Unit = {
    var i = destBitOffset
    var j = from

    while (j < to) {
      val ii = itemIdx * bitsPerSketch + j
      val bit = (sketchArray(ii / 64) >>> (ii % 64)) & 1L
      dest(i / 64) = bit << (i % 64)
      i += 1
      j += 1
    }
  }
}
