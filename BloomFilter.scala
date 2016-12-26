package atrox

import atrox.sketch.HashFunc
import atrox.sketch.MinHash
import java.lang.Integer.highestOneBit
import scala.math._


object BloomFilter {
  def apply[@scala.specialized(Int, Long) T](expectedItems: Int, falsePositiveRate: Double): BloomFilter[T] = {
    val (bitLength, hashFunctions) = optimalSize(expectedItems, falsePositiveRate)
    apply[T](hashFunctions, bitLength)
  }

  def apply[@scala.specialized(Int, Long) T](hashFunctions: Int, bitLength: Int): BloomFilter[T] =
    new BloomFilter[T](hashFunctions, higherPowerOfTwo(bitLength))

  def optimalSize(expectedItems: Int, falsePositiveRate: Double): (Int, Int) = {
    val n = expectedItems
    val p = falsePositiveRate
    val m = ceil(-(n * log(p)) / log(pow(2.0, log(2.0))))
    val k = round(log(2.0) * m / n)
    (m.toInt, k.toInt)
  }

  private def higherPowerOfTwo(x: Int) =
    highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)
}


trait Bloomy[@scala.specialized(Int, Long) T] {

  def add(x: T): this.type

  def contains(x: T): Boolean

  /** Return true if x is in the Bloom filter. If it's not present, the method
    * adds x into the set. This precise behaviour makes no difference for
    * ordinary Bloom filters but it have effects for counting bloom filters.
    **/
  def getAndSet(x: T): Boolean = {
    val res = contains(x)
    if (!res) {
      add(x)
    }
    res
  }

  def += (x: T): this.type = add(x)
  def apply(x: T) = contains(x)

}


/** A Bloom filter is a space-efficient probabilistic data structure, that is
  * used for set membership queries.  It might give a false positive, but never
  * false negative. That is, if bf.contains(x) returns false, element x is
  * deffinitely not in the set. If it returns true, element x might have been
  * added to the set.  If the set is properly sized, probabilisty of false
  * positive is very small. For example a bloom filter needs around 8 to 10 bits
  * per every inserted element to provide ~1% false positive rate.
  *
  * This implementation tends to overshoot and provides better guarantees by
  * rounding up size of a underlying bit array to the nearest power of two.
  */
class BloomFilter[@scala.specialized(Int, Long) T](
    val hashFunctions: Int, val bitLength: Int
  ) extends (T => Boolean) with Bloomy[T] {

  require(hashFunctions > 0, "number of hash functions must be greater than zero")
  require(bitLength >= 64, "length of a bloom filter must be at least 64 bits")
  require((bitLength & (bitLength - 1)) == 0, "length of a bloom filter must be power of 2")

  private val arr = new Array[Long](bitLength / 64)
  private val mask = bitLength - 1

  protected def elemHashCode(x: T) = x.hashCode

  protected def hash(i: Int, x: T): Int =
    fs(i)(elemHashCode(x))

  protected val fs =
    Array.tabulate[HashFunc[Int]](hashFunctions)(i => HashFunc.random(i * 4747))

  def add(x: T): this.type = {
    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      arr(pos / 64) |= (1L << (pos % 64))
      i += 1
    }

    this
  }


  def contains(x: T): Boolean = {
    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      if (((arr(pos / 64) >>> (pos % 64)) & 1L) == 0) {
        return false
      }
      i += 1
    }

    true
  }


  override def getAndSet(x: T): Boolean = {
    var isSet = true

    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      val longPos = pos / 64
      val bitPos = pos % 64

      isSet &= (((arr(longPos) >>> bitPos) & 1L) != 0)
      arr(longPos) |= (1L << bitPos)

      i += 1
    }

    isSet
  }


  def falsePositiveRate(n: Int) =
    pow(1.0 - pow(1.0 - 1.0 / bitLength, hashFunctions * n), hashFunctions)

  def sizeInBytes = arr.length * 8

  override def toString =
    s"BloomFilter(hashFunctions = $hashFunctions, bitLength = $bitLength)"

  def approximateSize = {
    var bitsSet = 0
    var i = 0 ; while (i < arr.length) {
      bitsSet += java.lang.Long.bitCount(arr(i))
      i += 1
    }

    - bitLength.toDouble / hashFunctions * math.log(1 - bitsSet.toDouble / bitLength)
  }


  def union(bf: BloomFilter[T]): BloomFilter[T] = {
    require(hashFunctions == bf.hashFunctions && bitLength == bf.bitLength,
      "Cannot unite bloom filters with different number of hash functions and lengths")

    val res = new BloomFilter[T](hashFunctions, bitLength)
    var i = 0 ; while (i < res.arr.length) {
      res.arr(i) = arr(i) | bf.arr(i);
      i += 1
    }

    res
  }


  def intersection(bf: BloomFilter[T]): BloomFilter[T] = {
    require(hashFunctions == bf.hashFunctions && bitLength == bf.bitLength,
      "Cannot intersect bloom filters with different number of hash functions and lengths")

    val res = new BloomFilter[T](hashFunctions, bitLength)
    var i = 0 ; while (i < res.arr.length) {
      res.arr(i) = arr(i) & bf.arr(i);
      i += 1
    }

    res
  }

}


class CountingBloomFilter[@scala.specialized(Int, Long) T](
    val hashFunctions: Int, val counters: Int, val counterBits: Int
  ) extends (T => Boolean) with Bloomy[T] {

  require(hashFunctions > 0, "number of hash functions must be greater than zero")
  require(counters >= 64, "length of a bloom filter must be at least 64 bits")
  require((counters & (counters - 1)) == 0, "length of a bloom filter must be power of 2")
  require(counterBits > 0 && counterBits <= 64, "number of counter bits must be greater than 0 and smaller than 64")

  private[this] val arr = new Array[Long](counters * counterBits / 64 + 1) // +1 for easier extraction of bits
  private[this] val mask = counters - 1

  protected def elemHashCode(x: T) = x.hashCode

  protected def hash(i: Int, x: T): Int =
    fs(i)(elemHashCode(x))

  protected val fs =
    Array.tabulate[HashFunc[Int]](hashFunctions)(i => HashFunc.random(i * 4747))


  protected def ripBits(arr: Array[Long], bitpos: Int, bitlen: Int): Long = {
    val mask = (1 << bitlen) - 1
    val endpos = (bitpos+bitlen) / 64

    ((arr(bitpos / 64) >>> (bitpos % 64)) & mask) |
    ((arr(endpos) << (64 - bitpos % 64)) & mask)
  }

  protected def ramBits(arr: Array[Long], bitpos: Int, bitlen: Int, value: Long): Unit = {
    val mask = (1 << bitlen) - 1
    val endpos = (bitpos+bitlen) / 64

    /*
    arr(bitpos / 64) &= ~(mask << (bitpos % 64))
    arr(bitpos / 64) |= (value << (bitpos % 64))

    arr(endpos) &= ~(mask >>> (64 - bitpos % 64))
    arr(endpos) |= (value >>> (64 - bitpos % 64))
    */

    // TODO code without loops
    var i = 0 ; while (i < bitlen) {
      val pos = bitpos + i
      arr(pos/64) ^= (-((value>>i)&1L) ^ arr(pos/64)) & (1L << (pos%64))
      i += 1
    }
  }


  def add(x: T): this.type = {
    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      val bitpos = pos * counterBits
      val maxCnt = (1 << counterBits) - 1

      val cnt = ripBits(arr, bitpos, counterBits)
      if (cnt < maxCnt) {
        ramBits(arr, bitpos, counterBits, cnt+1)
      }

      i += 1
    }

    this
  }


  def contains(x: T): Boolean = {
    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      val bitpos = pos * counterBits

      val cnt = ripBits(arr, bitpos, counterBits)
      if (cnt == 0) return false
      i += 1
    }
    true
  }


  def count(x: T): Long = {
    var minCnt = Long.MaxValue

    var i = 0
    while (i < hashFunctions) {
      val pos = hash(i, x) & mask
      val bitpos = pos * counterBits

      val cnt = ripBits(arr, bitpos, counterBits)
      minCnt = math.min(minCnt, cnt)

      i += 1
    }
    minCnt
  }


  def falsePositiveRate(n: Int) =
    pow(1.0 - pow(1.0 - 1.0 / counters, hashFunctions * n), hashFunctions)

  def sizeInBytes = arr.length * 8

  override def toString =
    s"CountingBloomFilter(hashFunctions = $hashFunctions, counters = $counters, counterBits = $counterBits)"


  def bits = arr.map(_.toBinaryString.padTo(64, '_')).mkString(" ")
}
