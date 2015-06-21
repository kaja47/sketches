package atrox

import atrox.sketch.HashFunc
import atrox.sketch.MinHash
import java.lang.Integer.highestOneBit
import scala.math._


// http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
object BloomFilter {
	def apply[@scala.specialized(Int, Long) T](slices: Int, sliceBitLength: Int): BloomFilter[T] =
		new BloomFilter[T](slices, higherPowerOfTwo(sliceBitLength))

	def apply[@scala.specialized(Int, Long) T](expectedNumItems: Int, falsePositiveRate: Double): BloomFilter[T] = {
		val (sliceBitLength, slices) = optimalSize(expectedNumItems, falsePositiveRate)
		apply[T](slices, sliceBitLength)
	}

	def optimalSize(expectedNumItems: Int, falsePositiveRate: Double): (Int, Int) = {
		val n = expectedNumItems
		val p = falsePositiveRate
		val m = ceil(-(n * log(p)) / log(pow(2.0, log(2.0))))
		val k = round(log(2.0) * m / n)
		(m.toInt, k.toInt)
	}

	private def higherPowerOfTwo(x: Int) =
		highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)
}


class BloomFilter[@scala.specialized(Int, Long) T](val slices: Int, val sliceBitLength: Int) extends (T => Boolean) {

	require(slices > 0)
	require(sliceBitLength > 0)
	require(sliceBitLength % 64 == 0)
	require((sliceBitLength & (sliceBitLength - 1)) == 0) // is power of 2

	private[this] val arr = new Array[Long](slices * sliceBitLength / 64)
	private[this] val mask = sliceBitLength - 1
	private[this] val sliceLongLength = sliceBitLength / 64

	def hash(i: Int, x: T): Int =
		fs(i)(x.hashCode)

	val fs = Array.tabulate[HashFunc[Int]](slices)(i => MinHash.randomHashFunction(i * 4747))

	def add(x: T): this.type = {

		var i = 0
		while (i < slices) {
			val pos = hash(i, x) & mask
			arr(sliceLongLength * i + pos / 64) |= (1L << (pos % 64))
			i += 1
		}

		this

	}

	def += (x: T): this.type = add(x)


	def contains(x: T): Boolean = {

		var i = 0
		while (i < slices) {
			val pos = hash(i, x) & mask
			if (((arr(sliceLongLength * i + pos / 64) >>> (pos % 64)) & 1L) == 0) {
				return false
			}
			i += 1
		}

		true

	}

	def apply(x: T) = contains(x)



	def falsePositiveRatio(n: Int) = {
		val p = 1.0 - pow(1.0 - 1.0 / sliceBitLength, n)
		pow(p, slices)
	}


}
