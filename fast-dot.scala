package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector }


/** Sparse-sparse vector dot product that is much simpler than standard one from breeze.
 *  In many cases can be much faster. Someimes even 2.5 times faster. It's
 *  caused by the fact that it contains lot less instruction and only one
 *  semi-unpredictable conditional jump in hot path.
 */
object fastDotProduct extends breeze.generic.UFunc.UImpl2[breeze.linalg.operators.OpMulInner.type, SparseVector[Double], SparseVector[Double], Double] {
  def apply(a: SparseVector[Double], b: SparseVector[Double]): Double = {
    require(a.size == b.size, "Vectors must be the same length!")

    val ak: Array[Int]    = a.array.index
    val av: Array[Double] = a.array.data

    val bk: Array[Int]    = b.array.index
    val bv: Array[Double] = b.array.data

    var prod = 0.0
    var ai, bi = 0
    while (ai != ak.length && bi != bk.length) {
      val a = ak(ai)
      val b = bk(bi)
      if (a == b) {
        prod += av(ai) * bv(bi)
      }

      // progress counter with smaller key
      ai += (if (a <= b) 1 else 0)
      bi += (if (a >= b) 1 else 0)
    }
    prod
  }
}


object fastSparse {

  /** Prepare integer array to be used by set functions in the fastSparse
    * module - ie. values are distinct and increasing. This method might return
    * the new array or modify the old array. */
  def makeSet(arr: Array[Int]): Array[Int] = {
    java.util.Arrays.sort(arr)
    if (isDistinctIncreasingArray(arr)) arr
    else arr.distinct

  }

  def isDistinctIncreasingArray(arr: Array[Int]): Boolean = {
    if (arr.length <= 1) return true

    var last = arr(0)
    var i = 1
    while (i < arr.length) {
      if (last >= arr(i)) return false
      last = arr(i)
      i += 1
    }

    true
  }

  def isIncreasingArray(arr: Array[Int]): Boolean = {
    if (arr.length <= 1) return true

    var last = arr(0)
    var i = 1
    while (i < arr.length) {
      if (last > arr(i)) return false
      last = arr(i)
      i += 1
    }

    true
  }


  /** arguments must be sorted arrays */
  def intersectionSize(a: Array[Int], b: Array[Int]): Int = {
    var size, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }
    size
  }

  /** This method tried to look ahead and skip some unnecessary iterations. In
    * some cases it can be faster than straightforward code, but it's rarely
    * slower. */
  def intersectionSizeWithSkips(a: Array[Int], b: Array[Int], skip: Int): Int = {
    var size, ai, bi = 0

    val alen = a.length - skip
    val blen = b.length - skip

    while (ai < alen && bi < blen) {
      val av = a(ai)
      val bv = b(bi)
      val _ai = ai
      val _bi = bi
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) (if (a(_ai+skip) < bv) skip else 1) else 0)
      bi   += (if (av >= bv) (if (b(_bi+skip) < av) skip else 1) else 0)
    }

    while (ai < a.length && bi < b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }

    size
  }

  def unionSize(a: Array[Int], b: Array[Int]): Int =
    a.length + b.length - intersectionSize(a, b)

  /** result = |a -- b| */
  def diffSize(a: Array[Int], b: Array[Int]): Int =
    a.length - intersectionSize(a, b)


  def intersectionAndUnionSize(a: Array[Int], b: Array[Int]): (Int, Int) = {
    val is = intersectionSize(a, b)
    (is, a.length + b.length - is)
  }

  def jaccardSimilarity(a: Array[Int], b: Array[Int]): Double = {
    val is = intersectionSize(a, b)
    val un = a.length + b.length - is
    is.toDouble / un
  }


  def union(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](unionSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      res(i) = (if (av > bv) bv else av)
      ai += (if (av <= bv) 1 else 0)
      bi += (if (av >= bv) 1 else 0)
      i += 1
    }

    while (ai != a.length) {
      res(i) = a(ai)
      i += 1
      ai += 1
    }

    while (bi != b.length) {
      res(i) = b(bi)
      i += 1
      bi += 1
    }

    res
  }

  def intersection(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](intersectionSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)

      if (av == bv) {
        res(i) = av
        i += 1
      }

      ai += (if (av <= bv) 1 else 0)
      bi += (if (av >= bv) 1 else 0)
    }

    res
  }


  /** result = a -- b */
  def diff(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](diffSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      if (av == bv) {
        ai += 1
        bi += 1

      } else if (av < bv) {
        res(i) = av
        i += 1
        ai += 1

      } else {
        bi += 1
      }
    }

    while (ai != a.length) {
      res(i) = a(ai)
      i += 1
      ai += 1
    }

    res
  }


  def weightedIntersectionSize(a: Array[Int], b: Array[Int], ws: Array[Double]): Double = {
    var ai, bi = 0
    var size = 0.0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) ws(av) else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }
    size
  }

  private def _sum(a: Array[Int], ws: Array[Double]): Double = {
    var s = 0.0
    var i = 0
    while (i < a.length) {
      s += ws(a(i))
      i += 1
    }
    s
  }

  def weightedJaccardSimilarity(a: Array[Int], b: Array[Int], ws: Array[Double]): Double = {
    val is = weightedIntersectionSize(a, b, ws)
    val un = _sum(a, ws) + _sum(b, ws) - is
    is.toDouble / un
  }


  private def _initUnion(sets: Array[Array[Int]]): (MinIntIntHeap, Array[Int]) = {
    val heap = new MinIntIntHeap(sets.length)
    val positions = new Array[Int](sets.length)

    for (i <- 0 until sets.length) {
      if (positions(i) < sets(i).length)  {
        heap.insert(sets(i)(0), i)
        positions(i) += 1
      }
    }

    (heap, positions)
  }


  private def _stepUnion(sets: Array[Array[Int]], i: Int, heap: MinIntIntHeap, positions: Array[Int]) = {
    if (positions(i) < sets(i).length) {
      heap.deleteMinAndInsert(sets(i)(positions(i)), i)
      positions(i) += 1
    } else {
      heap.deleteMin()
    }
  }


  /** Computes size of union of array of sets via multiway merge */
  def unionSize(sets: Array[Array[Int]]): Int = {
    val (heap, positions) = _initUnion(sets)
    var min = Long.MinValue
    var size = 0

    while (heap.nonEmpty) {
      val key = heap.minKey
      val i = heap.minValue

      if (key.toLong != min) {
        size += 1
        min = key
      }

      _stepUnion(sets, i, heap, positions)
    }

    size
  }


  def union(sets: Array[Array[Int]]): Array[Int] = {
    val (heap, positions) = _initUnion(sets)
    var min = Long.MinValue
    val buff = new collection.mutable.ArrayBuilder.ofInt

    while (heap.nonEmpty) {
      val key = heap.minKey
      val i = heap.minValue

      if (key.toLong != min) {
        buff += key
        min = key
      }

      _stepUnion(sets, i, heap, positions)
    }

    buff.result
  }


  def mergeSortedArrays(a: Array[Int], b: Array[Int]): Array[Int] =
    mergeSortedArrays(a, b, new Array[Int](a.length + b.length))

  def mergeSortedArrays(a: Array[Int], b: Array[Int], res: Array[Int]): res.type = {
    var ai, bi, ri = 0
    var al = a.length
    var bl = b.length
    while (ai < al && bi < bl) {
      if (a(ai) <= b(bi)) { res(ri) = a(ai) ; ai += 1 }
      else                { res(ri) = b(bi) ; bi += 1 }
      ri += 1
    }
    while (ai < al) { res(ri) = a(ai) ; ai += 1 ; ri += 1 }
    while (bi < bl) { res(ri) = b(bi) ; bi += 1 ; ri += 1 }
    res
  }


}


object Bits {

  import java.lang.Float. { floatToRawIntBits, intBitsToFloat }
  import java.lang.Integer.highestOneBit

  /** Extract up to 64 bits from a long array. The array is split into number of
    * blocks of length `blockLen`. Bits may span two neighbouring array
    * elements. Requested bits that overrun block length are extracted from the
    * begining of that block (hence *WrappingBlocks). */
  def getBitsWrappingBlocks(arr: Array[Long], blockLen: Int, block: Int, bit: Int, bitLen: Int): Long = {

    // position of long where current sequence starts
    val blockstart = block * blockLen

    // position of first bit to be extracted
    val startbit = blockstart * 64 + bit
    val mask = (1 << bitLen) - 1

    val _endpos = (startbit+bitLen) / 64
    // if position of second long is outside of current
    val endpos = if (_endpos < blockstart + blockLen) _endpos else blockstart

    ((arr(startbit / 64) >>> (startbit % 64)) & mask) |
    ((arr(endpos) << (64 - startbit % 64)) & mask)
  }


  /** Extract up to 64 bits from a long array. Bits may span two neighbouring
    * array elements. Requested bits that overrun length of the provied array
    * are extracted from the begining (hence *Wrapping). */
  def getBitsWrapping(arr: Array[Long], bit: Int, bitLen: Int): Long = {

    val startbit = bit
    val mask = (1 << bitLen) - 1

    val _endpos = (startbit+bitLen) / 64
    // if position of second long is outside of current
    val endpos = if (_endpos < arr.length) _endpos else 0

    ((arr(startbit / 64) >>> (startbit % 64)) & mask) |
    ((arr(endpos) << (64 - startbit % 64)) & mask)
  }


  /** Extract up to 64 bits from a long array. Bits may span two neighbouring
    * array elements. If requested bits overrun length of the array, exception
    * is thrown. Which means `bit` arument must be at less than
    * `arr.length * 64 - bitLen` */
  def getBitsOverlapping(arr: Array[Long], bit: Int, bitLen: Int): Long = {
    val startbit = bit
    val mask = (1 << bitLen) - 1

    ((arr(startbit / 64) >>> (startbit % 64)) & mask) |
    ((arr((startbit+bitLen) / 64) << (64 - startbit % 64)) & mask)
  }


  /** Extract up to 64 bits from a long array. All requested bits must be
    * contained inside one long, otherwise result is incorrect (no exception is
    * thrown). */
  def getBitsInsideLong(arr: Array[Long], bit: Int, bitLen: Int): Long =
    ((arr(bit / 64) >>> (bit % 64)) & (1 << bitLen) - 1)


  def pack(hi: Int, lo: Int): Long = hi.toLong << 32 | lo
  def pack(hi: Float, lo: Int): Long = pack(floatToRawIntBits(hi), lo)
  def pack(hi: Int, lo: Float): Long = pack(hi, floatToRawIntBits(lo))
  def pack(hi: Float, lo: Float): Long = pack(floatToRawIntBits(hi), floatToRawIntBits(lo))

  /** These methods encode floats in such way that they can be sorted by radix sort. */
  def packSortable(hi: Float, lo: Int): Long = pack(floatFlip(floatToRawIntBits(hi)), lo)
  def packSortable(hi: Int, lo: Float): Long = pack(hi, floatFlip(floatToRawIntBits(lo)))
  def packSortable(hi: Float, lo: Float): Long = pack(floatFlip(floatToRawIntBits(hi)), floatFlip(floatToRawIntBits(lo)))

  def unpackIntHi(l: Long): Int = (l >>> 32).toInt
  def unpackIntLo(l: Long): Int = l.toInt
  def unpackFloatHi(l: Long): Float = intBitsToFloat(unpackIntHi(l))
  def unpackFloatLo(l: Long): Float = intBitsToFloat(unpackIntLo(l))
  def unpackSortableFloatHi(l: Long): Float = intBitsToFloat(floatUnflip(unpackIntHi(l)))
  def unpackSortableFloatLo(l: Long): Float = intBitsToFloat(floatUnflip(unpackIntLo(l)))

  /** Converts float to signed int that preserve ordering,
    *  ie. if a < b, then ftsi(a) < ftsi(b)
    *  and if a < 0, then ftsi(a) < 0 */
  def floatToSortableInt(f: Float) = floatFlip(floatToRawIntBits(f))
  def sortableIntToFloat(i: Int)   = intBitsToFloat(floatUnflip(i))

  /** based on http://stereopsis.com/radix.html, except this converts to signed
    * ints. Only difference is that sign bit is never flipped. */
  protected def floatFlip(f: Int) = f ^ (-(f >>> 31) & 0x7FFFFFFF) // float to signed int
  protected def floatUnflip(f: Int) = f ^ (-(f >>> 31) & 0x7FFFFFFF) // signed int to float

  def higherPowerOfTwo(x: Int) =
    highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)

}
