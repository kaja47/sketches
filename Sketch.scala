package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector, normalize }
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }


trait HashFunc[T] {
	def apply(x: T): Int
}

trait HashFuncLong[T] {
	def apply(x: T): Long
}


trait Sketch extends Serializable {
	def estimateSimilarity(idxA: Int, idxB: Int): Double
	def sameBits(idxA: Int, idxB: Int): Int
	def minSameBits(sim: Double): Int
}



object MinHash {
	def apply[T](sets: IndexedSeq[Set[T]], hashFunctions: Array[HashFunc[T]]): MinHash[T] = {

		val sketchArray = new Array[Int](sets.length * hashFunctions.length)
		var i = 0

		for (set <- sets) {
			for (f <- hashFunctions) {
				var min = Int.MaxValue
				for (el <- set) {
					val h = f(el)
					if (h < min) {
						min = h
					}
				}
				sketchArray(i) = min
				i += 1
			}
		}

		new MinHash(sketchArray, hashFunctions.length, hashFunctions)
	}


	def randomHashFunctions(n: Int): Array[HashFunc[Int]] =
		Array.fill(n) {
			val a = BigInt.probablePrime(31, scala.util.Random).toInt
			val b = BigInt.probablePrime(31, scala.util.Random).toInt
			new HashFunc[Int] {
				def apply(x: Int): Int = (a*x+b) // >>> (32-M)
			}
		}

}


final class MinHash[T](sketchArray: Array[Int], sketchLength: Int, hashFunctions: Array[HashFunc[T]]) extends Sketch {

	def estimateSimilarity(idxA: Int, idxB: Int): Double =
		sameBits(idxA, idxB).toDouble / sketchLength

	def sameBits(idxA: Int, idxB: Int): Int = {
		var a = idxA * sketchLength
		var b = idxB * sketchLength
		var i, same = 0
		while (i < sketchLength) {
			same += (if (sketchArray(a+i) == sketchArray(b+i)) 1 else 0)
			i += 1
		}
		same
	}

	def minSameBits(sim: Double): Int = {
		require(sim >= 0.0 && sim <= 1, "similarity must be from (0, 1)")
		sim * sketchLength toInt
	}

}



object RandomHyperplanes {

	def apply(vectors: IndexedSeq[SparseVector[Double]], sketchLength: Int): RandomHyperplanes = {
		require(sketchLength % 64 == 0)

		val dims = vectors.head.size

		val randomHyperplanes = (0 until sketchLength).par map { _ => mkRandomHyperplane(dims) } seq
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


final class RandomHyperplanes(sketchArray: Array[Long], bitsPerSketch: Int) extends Sketch {
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

	def minSameBits(sim: Double): Int = {
		require(sim >= -1 && sim <= 1, "similarity must be from (-1, 1)")
		math.floor((sim + 1) / 2 * bitsPerSketch).toInt
	}
}


object RandomProjections {

	def apply(vectors: IndexedSeq[SparseVector[Double]], sketchLength: Int, bucketSize: Double): RandomProjections = {
		val dims = vectors.head.size

		val randomVectors = (0 until sketchLength).par map { _ => mkRandomUnitVector(dims) } seq
		val sketches: Array[Array[Int]] = vectors.par map { vec => randomVectors map { rv => (rv dot vec) / bucketSize toInt } toArray } toArray

		new RandomProjections(sketches.flatten, sketchLength, bucketSize)
	}

	private def mkRandomUnitVector(length: Int) =
		normalize(DenseVector.rand[Double](length) map (x => if (x < 0.5) -1.0 else 1.0), 2)

}


final class RandomProjections(sketchArray: Array[Int], sketchLength: Int, bucketSize: Double) extends Sketch {

	def estimateSimilarity(idxA: Int, idxB: Int): Double = ???
	def sameBits(idxA: Int, idxB: Int): Int = {
		var a = idxA * sketchLength
		var b = idxB * sketchLength
		var i, same = 0
		while (i < sketchLength) {
			same += (if (sketchArray(a+i) == sketchArray(b+i)) 1 else 0)
			i += 1
		}
		same
	}
	def minSameBits(sim: Double): Int = ???

}
