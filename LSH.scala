package atrox.sketch

import breeze.linalg.{ SparseVector, DenseVector, BitVector }
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import scala.util.hashing.MurmurHash3
import scala.collection.mutable


case class Sim(a: Int, b: Int, estimatedSimilatity: Double, similarity: Double)


object LSH {

	def apply(sk: BitSketch, bands: Int, onlyIdxs: Boolean): BitLSH =
		apply(sk, bands, onlyIdxs, -1)

	def apply(sk: BitSketch, bands: Int, onlyIdxs: Boolean, _bandBits: Int): BitLSH = {
		require(sk.bitsPerSketch % 64 == 0)
		require(sk.bitsPerSketch % bands == 0)

		val bandBits = if (_bandBits <= 0) sk.bitsPerSketch / bands else _bandBits
		val bandStep = sk.bitsPerSketch / bands

		require(bandBits < 64)
		val bandMask = (1L << bandBits) - 1
		val bandSize = (1 << bandBits)

		val longsLen = sk.bitsPerSketch / 64

		val sizes = new Array[Int](bands * bandSize)
		for (i <- 0 until sk.sketchArray.length / longsLen) {
			for (b <- 0 until bands) {
				val h = ripBits(sk.sketchArray, sk.bitsPerSketch, i, b, bandStep, bandMask)
				val bucket = b * bandSize + h
				sizes(bucket) += 1
			}
		}

		println(sizes.size)

		val idxs     = Array.tabulate(bands * bandSize) { i => if (sizes(i) == 0) null else new Array[Int](sizes(i)) }
		val sketches = Array.tabulate(bands * bandSize) { i => if (onlyIdxs)      null else new Array[Long](sizes(i)) }

		val is = new Array[Int](bands * bandSize)

		for (i <- 0 until sk.sketchArray.length / longsLen) {
			for (b <- 0 until bands) {
				val h = ripBits(sk.sketchArray, sk.bitsPerSketch, i, b, bandStep, bandMask)
				val bucket = b * bandSize + h
				idxs    (bucket)(is(bucket)) = i
				if (!onlyIdxs) {
					arraycopy(sk.sketchArray, i * longsLen, sketches(bucket), is(bucket) * longsLen, longsLen)
				}
				is(bucket) += 1
			}
		}

		new BitLSH(sk, idxs, sketches, sk.bitsPerSketch, bands, bandBits, bandStep, onlyIdxs)
	}


	def ripBits(sketchArray: Array[Long], bitsPerSketch: Int, i: Int, band: Int, bandStep: Int, bandMask: Long): Int = {
		// (rotateLeft(sketchArray(i), b*bandStep) & bandMask).toInt

		val startbit = i * bitsPerSketch + band * bandStep
		//val mask = ((1 << bandStep) - 1)
		val mask = bandMask

		if ((startbit+bandStep) / 64 < sketchArray.length) {
			((sketchArray(startbit / 64) >>> (startbit % 64)) & mask) |
			((sketchArray((startbit+bandStep) / 64) << (64 - startbit % 64)) & mask) toInt
		} else {
			((sketchArray(startbit / 64) >>> (startbit % 64)) & mask) toInt
		}
	}


	def apply(sk: IntSketch, bands: Int, hashBits: Int = 16, onlyIdxs: Boolean = false): IntLSH = {
		require(sk.sketchLength % bands == 0)
		require(sk.sketchArray.length % sk.sketchLength == 0)

		val hashMask = (1 << hashBits) - 1
		val bandSize = (1 << hashBits) // how many possible hashes is in one band
		val bandLength = sk.sketchLength / bands // how many elements from sketch is used in one band

		// array backed LSH

		val sizes = new Array[Int](bands * bandSize)
		for (i <- 0 until sk.sketchArray.length / sk.sketchLength) {
			for (b <- 0 until bands) {
				val h = hashSlice(sk.sketchArray, sk.sketchLength, i, b, bandLength, hashMask)
				val bucket = b * bandSize + h
				sizes(bucket) += 1
			}
		}

		val idxs     = Array.tabulate(bands * bandSize) { i => if (sizes(i) == 0) null else new Array[Int](sizes(i)) }
		val sketches = Array.tabulate(bands * bandSize) { i => if (onlyIdxs)      null else new Array[Int](sizes(i)) }

		val is = new Array[Int](bands * bandSize)

		for (i <- 0 until sk.sketchArray.length / sk.sketchLength) {
			for (b <- 0 until bands) {
				val h = hashSlice(sk.sketchArray, sk.sketchLength, i, b, bandLength, hashMask)
				val bucket = b * bandSize + h
				idxs(bucket)(is(bucket)) = i
				if (!onlyIdxs) {
//					arraycopy(sk.sketchArray, i * longsLen, sketches(bucket), is(bucket) * longsLen, longsLen)
						???
				}
				is(bucket) += 1
			}
		}

		new IntLSH(sk, idxs, sketches, sk.sketchLength, bands, sk.sketchLength / bands, hashMask)

		/*
		// hashmap backed LSH

		val buffers = Array.fill(bands)(mutable.Map[Int, (mutable.ArrayBuilder.ofInt, mutable.ArrayBuilder.ofInt)]())
		def mkBuilderPair = (new mutable.ArrayBuilder.ofInt, if (onlyIdxs) null else new mutable.ArrayBuilder.ofInt)

		for (i <- 0 until sk.sketchArray.length / sk.sketchLength) {
			for (b <- 0 until bands) {
				val h = hashSlice(sk.sketchArray, sk.sketchLength, i, b, bandLength, hashMask)

				val (idxs, sketches) = buffers(b).getOrElseUpdate(h, mkBuilderPair)
				idxs += i
				if (!onlyIdxs) {
					sketches ++= sk.sketchArray.slice(i * sk.sketchLength, i * sk.sketchLength + sk.sketchLength)
				}
			}
		}

		val data = buffers.map(_.map {
			case (h, (idxs, sketches)) =>
				(h, (idxs.result, if (sketches == null) null else sketches.result))
		}.toMap)

		new IntLSH(sk, data, sk.sketchLength, bands, sk.sketchLength / bands, hashMask)
		*/
	}

	def hashSlice(sketchArray: Array[Int], sketchLength: Int, i: Int, band: Int, bandLength: Int, hashMask: Int) = {
		val start = i * sketchLength + band * bandLength
		val end   = start + bandLength
		//val slice = sketchArray.slice(start, end)
		_hashSlice(sketchArray, start, end) & hashMask
	}


	import scala.util.hashing.MurmurHash3

	// based on scala.util.hashing.MurmurHash3.arrayHash
	private def _hashSlice(arr: Array[Int], start: Int, end: Int): Int = {
		var h = MurmurHash3.arraySeed
		var i = start
		while (i < end) {
			h = MurmurHash3.mix(h, arr(i))
			i += 1
		}
		MurmurHash3.finalizeHash(h, end-start)
	}

}



abstract class LSH {
	def allSimilarities(estimateThreshold: Double): Iterator[Sim]
	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim]

	//def bucketSizes: (Double, Int, Int)
}



final class IntLSH(
		val sketch: IntSketch,
		//val data: Array[Map[Int, (Array[Int], Array[Int])]],
		val idxs: Array[Array[Int]], val sketches: Array[Array[Int]],
		val sketchLength: Int, bands: Int, bandLength: Int, hashMask: Int
	) extends LSH with Serializable {

	private val bandSize = hashMask + 1


	def bandHashes(sketchArray: Array[Int], idx: Int): Iterator[Int] =
		for (b <- 0 until bands iterator) yield
			LSH.hashSlice(sketchArray, sketch.sketchLength, idx, b, bandLength, hashMask)

	def bandHash(sketchArray: Array[Int], idx: Int, band: Int): Int =
		LSH.hashSlice(sketchArray, sketch.sketchLength, idx, band, bandLength, hashMask)



	def candidates(idx: Int): Seq[(Array[Int], Array[Int])] =
		candidates(sketch.sketchArray, idx)

	/** @return Seq[(idxs, sketches)] */
//	def candidates(sketchArray: Array[Int], idx: Int): Seq[(Array[Int], Array[Int])] =
//		for (b <- 0 until bands) yield {
//			val h = LSH.hashSlice(sketchArray, sketch.sketchLength, idx, b, bandLength, hashMask)
//			data(b).getOrElse(h, (Array[Int](), Array[Int]()))
//		}

	def candidates(sketchArray: Array[Int], idx: Int): Seq[(Array[Int], Array[Int])] =
		(for (b <- 0 until bands) yield {
			val h = LSH.hashSlice(sketchArray, sketch.sketchLength, idx, b, bandLength, hashMask)
			val bucket = b * bandSize + h
			(idxs(bucket), sketches(bucket))
		}).filter(_._1 != null)


	def candidateIndexes(sketchArray: Array[Int], idx: Int): Seq[Array[Int]] =
		(for (b <- 0 until bands) yield {
			val h = LSH.hashSlice(sketchArray, sketch.sketchLength, idx, b, bandLength, hashMask)
			val bucket = b * bandSize + h
			idxs(bucket)
		}).filter(_ != null)

	def candidateIndexes(idx: Int): Seq[Array[Int]] =
		candidateIndexes(sketch.sketchArray, idx)


	def idxsStream: Iterator[Array[Int]] = idxs.iterator.filter(_ != null)

	def stream: Iterator[(Array[Int], Array[Int])] =
		(idxs.iterator zip sketches.iterator).filter(_._1 != null)
		// data.iterator.flatMap(_.valuesIterator)

	def allSimilarities(estimateThreshold: Double): Iterator[Sim] = ???
	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim] = ???

	def bucketSizes = ???
}



final class BitLSH(
		val sketch: BitSketch,
		val idxs: Array[Array[Int]], val sketches: Array[Array[Long]],
		val bitsPerSketch: Int, val bands: Int, val bandBits: Int, val bandStep: Int,
		val onlyIdxs: Boolean
	) extends LSH with Serializable {

	require(idxs.length == sketches.length)

	private[this] val bandMask = (1 << bandBits) - 1
	private[this] val bandSize = (1 << bandBits)


	def sameBits(sketchArray: Array[Long], idxA: Int, idxB: Int) = 
		sketch.sameBits(sketchArray, sketchArray, idxA, idxB, sketch.bitsPerSketch)


	def bandHashes(sketchArray: Array[Long], idx: Int): Iterator[Int] =
		for (b <- 0 until bands iterator) yield
			LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandStep, bandMask)


	def candidates(idx: Int): Seq[(Array[Int], Array[Long])] =
		candidates(sketch.sketchArray, idx)

	def candidates(sketchArray: Array[Long], idx: Int): Seq[(Array[Int], Array[Long])] =
		(for (b <- 0 until bands) yield {
			val h = LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandStep, bandMask)
			val bucket = b * bandSize + h
			(idxs(bucket), sketches(bucket))
		}).filter(_._1 != null)


	def candidateIndexes(sketchArray: Array[Long], idx: Int): Seq[Array[Int]] =
		(for (b <- 0 until bands) yield {
			val h = LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandStep, bandMask)
			val bucket = b * bandSize + h
			idxs(bucket)
		}).filter(_ != null)

	def candidateIndexes(idx: Int): Seq[Array[Int]] =
		candidateIndexes(sketch.sketchArray, idx)


	// will contain duplicates
	def similarIndexes(idx: Int, estimateThreshold: Double): Array[Int] =
		similarIndexes(sketch.sketchArray, idx, estimateThreshold, 0.0, null)

	// will contain duplicates
	def similarIndexes(idx: Int, estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Array[Int] =
		similarIndexes(sketch.sketchArray, idx, estimateThreshold, threshold, similarityFunction)

	// will contain duplicates
	def similarIndexes(sketchArray: Array[Long], idx: Int, estimateThreshold: Double): Array[Int] =
		similarIndexes(sketch.sketchArray, idx, estimateThreshold, 0.0, null)

	// will contain duplicates
	def similarIndexes(sketchArray: Array[Long], idx: Int, estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Array[Int] = {
		val minBits = sketch.minSameBits(estimateThreshold)
		val res = new collection.mutable.ArrayBuilder.ofInt

		if (!onlyIdxs) {

			for ((idxs, skArr) <- candidates(sketchArray, idx)) {
				var i = 0
				while (i < idxs.length) {
					val bits = sketch.sameBits(skArr, sketchArray, i, idx, sketch.bitsPerSketch)
					if (bits >= minBits) {
						if (similarityFunction == null || similarityFunction(idxs(i), idx) >= threshold) {
							res += idxs(i)
						}
					}
					i += 1
				}
			}

		} else {

			for ((idxs, _) <- candidates(sketchArray, idx)) {
				var i = 0
				while (i < idxs.length) {
					val bits = sketch.sameBits(sketchArray, sketchArray, idxs(i), idx, sketch.bitsPerSketch)
					if (bits >= minBits) {
						if (similarityFunction == null || similarityFunction(idxs(i), idx) >= threshold) {
							res += idxs(i)
						}
					}
					i += 1
				}
			}

		}

		res.result
	}


	def allSimilarities(estimateThreshold: Double): Iterator[Sim] =
		allSimilarities(estimateThreshold, 0.0, null)


	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim] = {
		val minBits = sketch.minSameBits(estimateThreshold)

		(for ((idxs, skArr) <- stream) yield {
			val res = collection.mutable.ArrayBuffer[Sim]()
			var i = 0
			while (i < idxs.length) {
				var j = i+1
				while (j < idxs.length) {
					val bits = sketch.sameBits(skArr, skArr, i, j, sketch.bitsPerSketch)
					if (bits >= minBits) {
						var sim: Double = 0.0
						if (similarityFunction == null || { sim = similarityFunction(idxs(i), idxs(j)) ; sim >= threshold }) {
							res += Sim(idxs(i), idxs(j), sketch.estimateSimilarity(bits), sim)
						}
					}
					j += 1
				}
				i += 1
			}
			res
		}).flatten
	}


	def stream: Iterator[(Array[Int], Array[Long])] =
		(idxs.iterator zip sketches.iterator).filter(_._1 != null)


	def bucketSizes = {
		def len(xs: Array[Int]) = if (xs == null) 0 else xs.length

		val sum = idxs.map(len).sum
		val cnt = idxs.length
		val avg = sum.toDouble / cnt
		val zeros = idxs count (xs => xs == null || xs.length == 0)

		val min = idxs.map(len).min
		val max = idxs.map(len).max

		val lengths = idxs.map(len).sortBy(-_).take(100)

		(avg, min, max, zeros, lengths.toVector)
	}

}
