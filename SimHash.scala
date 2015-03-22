package atrox.sketch

import java.lang.Long.{ bitCount, rotateLeft }


object SimHash {

	val md5 = new HashFuncLong[String] {
		def apply(x: String): Long = {
			val m = java.security.MessageDigest.getInstance("MD5")
			m.reset()
			m.update(x.getBytes())
			val bytes = m.digest()

			val bb = java.nio.ByteBuffer.wrap(bytes)
			bb.getLong
		}
	}


	def apply[T](xs: IndexedSeq[Array[T]], f: HashFuncLong[T]): SimHash = {
		val sketchArray = new Array[Long](xs.length)

		for (i <- 0 until xs.size) {
			sketchArray(i) = mkSimHash64(xs(i), f)
		}

		new SimHash(sketchArray)
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



final class SimHash(val sketchArray: Array[Long]) extends BitSketch {

	val bitsPerSketch = 64

	def estimateSimilarity(idxA: Int, idxB: Int): Double = sameBits(idxA, idxB) / 64.0
	def sameBits(idxA: Int, idxB: Int): Int = 64 - bitCount(sketchArray(idxA) ^ sketchArray(idxB))

	def empty = new SimHash(null)

}
