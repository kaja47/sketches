package collab

import breeze.linalg.{ SparseVector, DenseVector, BitVector }
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }


trait Sketch extends Serializable {
	def estimateSimilarity(idxA: Int, idxB: Int): Double
	def sameBits(idxA: Int, idxB: Int): Int
	def minSameBits(sim: Double): Int
}


object RandomHyperplanes {

	def apply(vectors: IndexedSeq[SparseVector[Double]], sketchLength: Int): RandomHyperplanes = {
		require(sketchLength % 64 == 0)

		val randomHyperplanes = (0 until sketchLength).par map { _ => mkRandomHyperplane(vectors.head.size) } seq
		val sketches = vectors.par map { vec => BitVector(randomHyperplanes map { rhp => (rhp dot vec) > 0.0 }: _*).data.toLongArray } toArray

		val arr = new Array[Long](vectors.size * sketchLength / 64)
		for (i <- 0 until vectors.size) {
			arraycopy(sketches(i), 0, arr, i*sketchLength/64, sketchLength/64)
		}

		new RandomHyperplanes(arr, sketchLength)
	}

	private def mkRandomHyperplane(length: Int) =
		DenseVector.rand[Double](length) map (x => if (x < 0.5) -1.0 else 1.0)
}


final class RandomHyperplanes(sketchArray: Array[Long], bitsPerSketch: Int) extends Sketch with Serializable {
	def estimateSimilarity(idxA: Int, idxB: Int): Double =
		-1.0 + 2.0 * sameBits(idxA, idxB) / bitsPerSketch.toDouble

	def sameBits(idxA: Int, idxB: Int): Int = {
		var i, diffBits = 0
		val longsLen = bitsPerSketch / 64
		var res = bitsPerSketch
		while (i < longsLen) {
			res -= bitCount(sketchArray(idxA*longsLen+i) ^ sketchArray(idxB*longsLen+i))
			i += 1
		}
		res
	}

	def minSameBits(sim: Double): Int =
		math.floor((sim + 1) / 2 * bitsPerSketch).toInt
}
