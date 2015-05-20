package atrox.sketch

import breeze.linalg.{ SparseVector, DenseVector, BitVector, normalize }
import breeze.stats.distributions.Rand
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import java.util.Arrays


trait HashFunc[T] extends Serializable {
	def apply(x: T): Int
}

trait HashFuncLong[T] extends Serializable {
	def apply(x: T): Long
}



trait Sketch extends Serializable {
	def estimateSimilarity(idxA: Int, idxB: Int): Double
	//def estimateSimilarity(sameBits: Int): Double
	def sameBits(idxA: Int, idxB: Int): Int
	def minSameBits(sim: Double): Int
	
	def empty: Sketch
	def get(idx: Int): Sketch = ???
}



abstract class BitSketch extends Sketch {

	def sketchArray: Array[Long]
	def bitsPerSketch: Int

	def minSameBits(sim: Double): Int = {
		require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
		(sim * bitsPerSketch).toInt
	}

	def estimateSimilarity(sameBits: Int): Double =
		sameBits.toDouble / bitsPerSketch

	def sameBits(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int, bitsPerSketch: Int): Int = {
		val longsLen = bitsPerSketch / 64
		val a = idxA * longsLen
		val b = idxB * longsLen
		var i = 0
		var same = bitsPerSketch
		while (i < longsLen) {
			same -= bitCount(arrA(a+i) ^ arrB(b+i))
			i += 1
		}
		same
	}

	def sameBits64(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int): Int = {
		64 - bitCount(arrA(idxA) ^ arrB(idxB))
	}

	def sameBits128(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int): Int = {
		var same = 128
		same -= bitCount(arrA(idxA)   ^ arrB(idxB))
		same -= bitCount(arrA(idxA+1) ^ arrB(idxB+1))
		same
	}

	def sameBits256(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int): Int = {
		var same = 256
		same -= bitCount(arrA(idxA)   ^ arrB(idxB))
		same -= bitCount(arrA(idxA+1) ^ arrB(idxB+1))
		same -= bitCount(arrA(idxA+2) ^ arrB(idxB+2))
		same -= bitCount(arrA(idxA+3) ^ arrB(idxB+3))
		same
	}

	def sameBits512(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int): Int = {
		var same = 512
		same -= bitCount(arrA(idxA)   ^ arrB(idxB))
		same -= bitCount(arrA(idxA+1) ^ arrB(idxB+1))
		same -= bitCount(arrA(idxA+2) ^ arrB(idxB+2))
		same -= bitCount(arrA(idxA+3) ^ arrB(idxB+3))
		same -= bitCount(arrA(idxA+4) ^ arrB(idxB+4))
		same -= bitCount(arrA(idxA+5) ^ arrB(idxB+5))
		same -= bitCount(arrA(idxA+6) ^ arrB(idxB+6))
		same -= bitCount(arrA(idxA+7) ^ arrB(idxB+7))
		same
	}

	def sameBits1024(arrA: Array[Long], arrB: Array[Long], idxA: Int, idxB: Int): Int =
		sameBits512(arrA, arrB, idxA*2, idxB*2) + sameBits512(arrA, arrB, idxA*2+1, idxB*2+1)

	def empty: BitSketch
}



abstract class IntSketch extends Sketch {

	def sketchArray: Array[Int]
	def sketchLength: Int

	def sameBits(arrA: Array[Int], arrB: Array[Int], idxA: Int, idxB: Int, sketchLength: Int): Int = {
		var a = idxA * sketchLength
		var b = idxB * sketchLength
		var i, same = 0
		while (i < sketchLength) {
			same += (if (arrA(a+i) == arrB(b+i)) 1 else 0)
			i += 1
		}
		same
	}

	def empty: IntSketch

	def skteches: Iterator[Array[Int]] = (0 until (sketchArray.length/sketchLength)).iterator map { i => Arrays.copyOfRange(sketchArray, i*sketchLength, (i+1)*sketchLength) }
}



// sketch implementations



object MinHash {

	def apply(sets: Array[Array[Int]], n: Int): MinHash[Int] =
		apply(sets, randomHashFunctions(n))

	def apply(sets: Array[Array[Int]], hashFunctions: Array[HashFunc[Int]]): MinHash[Int] = {
		val sketchArray = new Array[Int](sets.length * hashFunctions.length)
		var i = 0

		for (set <- sets) {
			i = writeMinHash(sketchArray, i, hashFunctions, set)
		}

		new MinHash(sketchArray, hashFunctions.length, hashFunctions)
	}

	/** @return end pos */
	final def writeMinHash(sketchArray: Array[Int], startPos: Int, hashFunctions: Array[HashFunc[Int]], set: Array[Int]): Int = {
		var i = startPos
		var hf = 0
		while (hf < hashFunctions.length) {
			val f = hashFunctions(hf)
			var min = Int.MaxValue
			var j = 0
			while (j < set.length) {
				val h = f(set(j))
				if (h < min) {
					min = h
				}
				j += 1
			}
			sketchArray(i) = min
			i += 1
			hf += 1
		}
		i
	}

	def mkMinHash(set: Array[Int], hashFunctions: Array[HashFunc[Int]]): Array[Int] = {
		val res = new Array[Int](hashFunctions.length)
		writeMinHash(res, 0, hashFunctions, set)
		res
	}


	def apply(sets: IndexedSeq[Set[Int]], n: Int): MinHash[Int] =
		apply(sets, randomHashFunctions(n))

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


	def randomHashFunctions(n: Int, seed: Int = 123456, _M: Int = 32): Array[HashFunc[Int]] = {
		val rand = new scala.util.Random(seed)
		Array.fill(n) {
			new HashFunc[Int] {
				val M = _M
				val a = (rand.nextLong() & ((1L << 62)-1)) * 2 + 1       // random odd positive integer (a < 2^w)
				val b = math.abs(rand.nextLong() & ((1L << (64 - M))-1)) // random non-negative integer (b < 2^(w-M)
				def apply(x: Int): Int = ((a*x+b) >>> (64-M)).toInt

				override def toString = s"HashFunc: $a * x + $b >> ${64-M}"
			}
		}
	}

}



final class MinHash[T](val sketchArray: Array[Int], val sketchLength: Int, hashFunctions: Array[HashFunc[T]]) extends IntSketch {

	def estimateSimilarity(idxA: Int, idxB: Int): Double =
		sameBits(idxA, idxB).toDouble / sketchLength

	def sameBits(idxA: Int, idxB: Int): Int =
		sameBits(sketchArray, sketchArray, idxA, idxB, sketchLength)

	def minSameBits(sim: Double): Int = {
		require(sim >= 0.0 && sim <= 1.0, "similarity must be from (0, 1)")
		sim * sketchLength toInt
	}

	def empty = new MinHash(null, sketchLength, hashFunctions)

}



object RandomHyperplanes {

	def apply(vectors: IndexedSeq[SparseVector[Double]], sketchLength: Int): RandomHyperplanes = {
		require(sketchLength % 64 == 0)

		val dims = vectors.head.size

		/*
		val randomHyperplanes = (0 until sketchLength).par map { _ => mkRandomHyperplane(dims, null) } seq
		val sketches = vectors.par map { vec => BitVector(randomHyperplanes map { rhp => (rhp dot vec) > 0.0 }: _*).data.toLongArray } toArray

		val arr = new Array[Long](vectors.size * sketchLength / 64)
		for (i <- 0 until vectors.size) {
			arraycopy(sketches(i), 0, arr, i*sketchLength/64, sketchLength/64)
		}
		*/

		// memory efficient version, computing one hyperplane at a time
		val arr = new Array[Long](vectors.size * sketchLength / 64)
		0 until sketchLength foreach { h =>
			val rh = mkRandomHyperplane(dims)

			for (i <- 0 until vectors.length) {
				if ((rh dot vectors(i)) > 0.0) {
					arr(i*sketchLength/64 + h / 64) |= (1L << (h % 64))
				}
			}
		}

		new RandomHyperplanes(arr, sketchLength)
	}

	private def mkRandomHyperplane(length: Int) =
		DenseVector.rand[Double](length) map (x => if (x < 0.5) -1.0 else 1.0)
}



final class RandomHyperplanes(val sketchArray: Array[Long], val bitsPerSketch: Int) extends BitSketch {
	def estimateSimilarity(idxA: Int, idxB: Int): Double =
		//-1.0 + 2.0 * sameBits(idxA, idxB) / bitsPerSketch.toDouble
		math.cos(math.Pi * (1 - sameBits(idxA, idxB) / bitsPerSketch.toDouble))

	def sameBits(idxA: Int, idxB: Int): Int =
		sameBits(sketchArray, sketchArray, idxA, idxB, bitsPerSketch)

	override def minSameBits(sim: Double): Int = {
		require(sim >= -1 && sim <= 1, "similarity must be from (-1, 1)")
		//math.floor((sim + 1) / 2 * bitsPerSketch).toInt
		math.floor((1.0 - math.acos(sim) / math.Pi) * bitsPerSketch).toInt
	}

	def empty = new RandomHyperplanes(null, bitsPerSketch)
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



final class RandomProjections(val sketchArray: Array[Int], val sketchLength: Int, bucketSize: Double) extends IntSketch {

	def estimateSimilarity(idxA: Int, idxB: Int): Double = ???
	def sameBits(idxA: Int, idxB: Int): Int =
		sameBits(sketchArray, sketchArray, idxA, idxB, sketchLength)
	def minSameBits(sim: Double): Int = ???

	def empty = new RandomProjections(null, sketchLength, bucketSize)

}



object HammingDistance {
	def apply(arr: Array[Long], bits: Int): BitSketch =
		if (bits == 64) {
			new HammingDistance64(arr)
		} else {
			new HammingDistance(arr, bits)
		}
}



final class HammingDistance64(arr: Array[Long]) extends BitSketch {

	def sketchArray = arr

	val bitsPerSketch = 64

	def estimateSimilarity(idxA: Int, idxB: Int): Double = sameBits(idxA, idxB) / 64.0
	def sameBits(idxA: Int, idxB: Int): Int = 64 - bitCount(arr(idxA) ^ arr(idxB))

	def empty = new HammingDistance64(null)
}


final class HammingDistance(arr: Array[Long], val bitsPerSketch: Int) extends BitSketch {
	require(bitsPerSketch % 64 == 0)

	def sketchArray = arr

	def estimateSimilarity(idxA: Int, idxB: Int): Double = sameBits(idxA, idxB) / bitsPerSketch.toDouble

	def sameBits(idxA: Int, idxB: Int): Int =
		sameBits(arr, arr, idxA, idxB, bitsPerSketch)

	def empty = new HammingDistance64(null)
}
