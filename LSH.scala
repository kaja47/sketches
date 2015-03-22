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
		val bandMask = (1 << bandBits) - 1
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

		val idxs     = Array.tabulate(bands * bandSize){ i => new Array[Int](sizes(i)) }
		val sketches = Array.tabulate(bands * bandSize){ i => new Array[Long](sizes(i)) }

		val is = new Array[Int](bands * bandSize)

		for (i <- 0 until sk.sketchArray.length / longsLen) {
			for (b <- 0 until bands) {
				val h = ripBits(sk.sketchArray, sk.bitsPerSketch, i, b, bandStep, bandMask)
				val bucket = b * bandSize + h
				idxs    (bucket)(is(bucket)) = i
				arraycopy(sk.sketchArray, i * longsLen, sketches(bucket), is(bucket) * longsLen, longsLen)
				is(bucket) += 1
			}
		}

		new BitLSH(sk, idxs, sketches, sk.bitsPerSketch, bands, bandBits, bandStep)
	}


	def ripBits(sketchArray: Array[Long], bitsPerSketch: Int, i: Int, band: Int, bandStep: Int, bandMask: Int): Int = {
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


	def apply(sk: IntSketch, bands: Int, hashBits: Int = 31, onlyIdxs: Boolean = false): IntLSH = {
		require(sk.sketchLength % bands == 0)

		val hashMask = (1 << hashBits) - 1

		val bandLength = sk.sketchLength / bands

		val buffers = Array.fill(bands)(mutable.Map[Int, (mutable.ArrayBuilder.ofInt, mutable.ArrayBuilder.ofInt)]())
		def mkBuilderPair = (new mutable.ArrayBuilder.ofInt, if (onlyIdxs) null else new mutable.ArrayBuilder.ofInt)

		for (i <- 0 until sk.sketchArray.length / sk.sketchLength) {
			for (b <- 0 until bands) {
				val slice = sk.sketchArray.slice(i * sk.sketchLength + b * bandLength, bandLength)
				val h = MurmurHash3.arrayHash(slice) & hashMask

				val (idxs, sketches) = buffers(b).getOrElseUpdate(h, mkBuilderPair)
				idxs += i
				if (!onlyIdxs) {
					sketches ++= sk.sketchArray.slice(i * sk.sketchLength, sk.sketchLength)
				}
			}
		}

		val data = buffers.map(_.map { case (k, (idxs, sketches)) => (k, (idxs.result, if (sketches == null) null else sketches.result)) }.toMap)

		new IntLSH(sk.empty, data, sk.sketchLength, bands, sk.sketchLength / bands)
	}
}



abstract class LSH {
	def allSimilarities(estimateThreshold: Double): Iterator[Sim]
	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim]

	def bucketSizes: (Double, Int, Int)
}



final class IntLSH(
		val sketch: IntSketch,
		val data: Array[Map[Int, (Array[Int], Array[Int])]],
		val sketchLength: Int, bands: Int, bandLength: Int
	) extends LSH {

	def idxsStream: Iterator[Array[Int]] = stream.map(_._1)
	def stream: Iterator[(Array[Int], Array[Int])] = data.iterator.flatMap(_.valuesIterator)

	def allSimilarities(estimateThreshold: Double): Iterator[Sim] = ???
	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim] = ???

	def bucketSizes = ???
}



final class BitLSH(
		val sketch: BitSketch,
		val idxs: Array[Array[Int]], val sketches: Array[Array[Long]],
		val bitsPerSketch: Int, bands: Int, bandBits: Int, bandStep: Int
	) extends LSH {

	require(idxs.length == sketches.length)

	private[this] val bandMask = (1 << bandBits) - 1
	private[this] val bandSize = (1 << bandBits)


	def sameBits(sketchArray: Array[Long], idxA: Int, idxB: Int) = 
		sketch.sameBits(sketchArray, sketchArray, idxA, idxB, sketch.bitsPerSketch)


	def candidates(idx: Int): Seq[(Array[Int], Array[Long])] =
		candidates(sketch.sketchArray, idx)

	def candidates(sketchArray: Array[Long], idx: Int): Seq[(Array[Int], Array[Long])] =
		for (b <- 0 until bands) yield {
			val h = LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandStep, bandMask)
			val bucket = b * bandSize + h
			(idxs(bucket), sketches(bucket))
		}


	def candidateIndexes(sketch: Array[Long], idx: Int): Seq[Array[Int]] =
		candidates(sketch, idx).map(_._1)


	// will contain duplicates
	def similarIndexes(idx: Int, estimateThreshold: Double): Array[Int] =
		similarIndexes(sketch.sketchArray, idx, estimateThreshold)

	// will contain duplicates
	def similarIndexes(idx: Int, estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Array[Int] =
		similarIndexes(sketch.sketchArray, idx, estimateThreshold, threshold, similarityFunction)

	// will contain duplicates
	def similarIndexes(sketchArray: Array[Long], idx: Int, estimateThreshold: Double): Array[Int] = {
		val minBits = sketch.minSameBits(estimateThreshold)
		val res = new collection.mutable.ArrayBuilder.ofInt

		for ((idxs, skArr) <- candidates(sketchArray, idx)) {
			var i = 0
			while (i < idxs.length) {
				val bits = sketch.sameBits(skArr, sketchArray, i, idx, sketch.bitsPerSketch)
				if (bits >= minBits) {
					res += idxs(i)
				}
				i += 1
			}
		}
		res.result
	}

	// will contain duplicates
	def similarIndexes(sketchArray: Array[Long], idx: Int, estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Array[Int] = {
		val minBits = sketch.minSameBits(estimateThreshold)
		val res = new collection.mutable.ArrayBuilder.ofInt

		for ((idxs, skArr) <- candidates(sketchArray, idx)) {
			var i = 0
			while (i < idxs.length) {
				val bits = sketch.sameBits(skArr, sketchArray, i, idx, sketch.bitsPerSketch)
				if (bits >= minBits) {
					val sim = similarityFunction(idxs(i), idx)
					if (sim >= threshold) {
						res += idxs(i)
					}
				}
				i += 1
			}
		}
		res.result
	}


	def allSimilarities(estimateThreshold: Double): Iterator[Sim] =
		allSimilarities(estimateThreshold, 0.0, (a, b) => 1.0)


	def allSimilarities(estimateThreshold: Double, threshold: Double, similarityFunction: (Int, Int) => Double): Iterator[Sim] = {
		val minBits = sketch.minSameBits(estimateThreshold)
		val res = collection.mutable.ArrayBuffer[Sim]()

		for ((idxs, skArr) <- stream) yield {
			var i = 0
			while (i < idxs.length) {
				var j = i+1
				while (i < idxs.length) {
					val bits = sketch.sameBits(skArr, skArr, i, j, sketch.bitsPerSketch)
					if (bits >= minBits) {
						val sim = similarityFunction(idxs(i), idxs(j))
						if (sim >= threshold) {
							res += Sim(idxs(i), idxs(j), sketch.estimateSimilarity(bits), sim)
						}
					}
					i += 1
				}
			}
		}

		res.iterator
	}


	def stream: Iterator[(Array[Int], Array[Long])] =
		idxs.iterator zip sketches.iterator


	def bucketSizes = {
		val sum = idxs.map(_.length).sum
		val cnt = idxs.length
		val avg = sum.toDouble / cnt

		val min = idxs.map(_.length).min
		val max = idxs.map(_.length).max

		(avg, min, max)
	}

}
