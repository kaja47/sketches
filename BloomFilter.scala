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
class BloomFilter[@scala.specialized(Int, Long) T](val hashFunctions: Int, val bitLength: Int) extends (T => Boolean) {

  require(hashFunctions > 0, "number of hash functions must be greater than zero")
  require(bitLength >= 64, "length of a bloom filter must be at least 64 bits")
  require((bitLength & (bitLength - 1)) == 0, "length of a bloom filter must be power of 2")

  private[this] val arr = new Array[Long](bitLength / 64)
  private[this] val mask = bitLength - 1

  protected def hash(i: Int, x: T): Int =
    fs(i)(x.hashCode)

  protected val fs =
    Array.tabulate[HashFunc[Int]](hashFunctions)(i => MinHash.randomHashFunction(i * 4747))

  def += (x: T): this.type = add(x)
  def apply(x: T) = contains(x)

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


  def getAndSet(x: T): Boolean = {
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

  def sizeBytes = arr.length * 8

}
