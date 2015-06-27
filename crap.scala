package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector }

object crap {

	def sum(xs: Seq[SparseVector[Double]]): DenseVector[Double] = {
		val s = DenseVector.zeros[Double](xs.head.size)
		for (x <- xs) {
			s += x
		}
		s
	}

	def df(xs: Seq[SparseVector[Double]]): DenseVector[Double] = {
		val s = DenseVector.zeros[Double](xs.head.size)
		for (vec <- xs) {
			var offset = 0
			while (offset < vec.activeSize) {
				val i = vec.indexAt(offset)
				s(i) += 1
				offset += 1
			}
		}
		s
	}

	def tfBoolean(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
		for (vec <- fs) yield vec mapActiveValues { _ => 1.0 }

	def tfLog(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
		for (vec <- fs) yield vec mapActiveValues { f => 1.0 + math.log(f) }

	def tfAugmented(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
		for (vec <- fs) yield {
			val m = breeze.linalg.max(vec)
			vec mapActiveValues { f => 0.5 + (0.5 * f) / m }
		}


	def tfidf(tfs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
		tfidf(tfs, df(tfs))


	def tfidf(tfs: Seq[SparseVector[Double]], df: DenseVector[Double]): Seq[SparseVector[Double]] = {
		val N = tfs.size
		for (vec <- tfs) yield {
			vec mapActivePairs { case (idx, tf) => tf * math.log(N / df(idx))  }
		}
	}
}



/** Frequency map intended for getting top-K elements from heavily skewed datasets.
  * It's precise if at most K elements have higher frequency than $freqThreshold.
  **/
final class IntFreqMap(initialSize: Int = 32, loadFactor: Double = 0.3, freqThreshold: Int = 16) {
	import java.lang.Integer.highestOneBit

	require(loadFactor > 0.0 && loadFactor < 1.0)
	require(freqThreshold > 1)

	private[this] var capacity = math.max(higherPowerOfTwo(initialSize), 16)
	private[this] var _size = 0
	private[this] var maxSize = (capacity * loadFactor).toInt
	private[this] val realFreqThreshold = higherPowerOfTwo(freqThreshold)

	private[this] var keys: Array[Int] = new Array[Int](capacity)
	private[this] var freq: Array[Int] = new Array[Int](capacity) // frequency of corresponding key
	private[this] val lowFreqs: Array[Int] = new Array[Int](realFreqThreshold+1) // frequency of low frequencies


	def size = _size

	def += (k: Int, count: Int = 1): this.type = {
		assert(count > 0)

		val i = findIdx(k)

		val oldFreq = freq(i)

		if (oldFreq == 0) {
			_size += 1
		}

		keys(i) = k
		freq(i) += count

		updateLowFrequency(oldFreq, freq(i))

		if (_size > maxSize) {
			grow()
		}

		this
	}


	def ++= (ks: Array[Int], count: Int): this.type = {
		var i = 0
		while (i < ks.length) {
			this += (ks(i), count)
			i += 1
		}
		this
	}

	def get(k: Int): Int = {
		val i = findIdx(k)
		if (freq(i) > 0) {
			freq(i)
		} else {
			throw new java.util.NoSuchElementException("key not found: "+k)
		}
	}

	def clear(): this.type = {
		this._size = 0

		var i = 0
		while (i < freq.length) {
			keys(i) = 0
			freq(i) = 0
			i += 1
		}

		var j = 0
		while (j < lowFreqs.length) {
			lowFreqs(j) = 0
			j += 1
		}

		this
	}

	def iterator = keys.iterator zip freq.iterator filter { case (k, f) => f > 0 }

	def toArray: Array[(Int, Int)] = {
		val res = new Array[(Int, Int)](_size)
		var i, j = 0
		while (i < capacity) {
			if (freq(i) > 0) {
				res(j) = (keys(i), freq(i))
				j += 1
			}
			i += 1
		}
		res
	}

	override def toString = iterator.mkString("IntFreqMap(", ",", ")")



	private def updateLowFrequency(oldFreq: Int, newFreq: Int): Unit = {
		val mask = realFreqThreshold - 1
		lowFreqs(if (oldFreq < realFreqThreshold) oldFreq & mask else realFreqThreshold) -= 1
		lowFreqs(if (newFreq < realFreqThreshold) newFreq & mask else realFreqThreshold) += 1
	}

	private def higherPowerOfTwo(x: Int) =
		highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)

	private def findIdx(k: Int) = {
		val mask = capacity - 1
		val pos = k & mask
		var i = pos
		while (freq(i) != 0 && keys(i) != k) {
			i = (i + 1) & mask
		}
		i
	}

	/** used to add elements into resized arrays in grow() method */
	private def growset(k: Int, count: Int): Unit = {
		val i = findIdx(k)
		keys(i) = k
		freq(i) = count
	}

	private def grow(): Unit = {
		val oldKeys = keys
		val oldFreq = freq

		this.capacity *= 2
		this.maxSize = (this.capacity * loadFactor).toInt
		this.keys = new Array[Int](this.capacity)
		this.freq = new Array[Int](this.capacity)

		var i = 0
		while (i < oldKeys.length) {
			if (oldFreq(i) > 0) {
				this.growset(oldKeys(i), oldFreq(i))
			}
			i += 1
		}
	}

	/** @return k most frequent elements, wihout corresponding frequency, not sorted */
	def topK(k: Int): Array[Int] = {
		require(k > 0)

		val realK = math.min(k, size)
		val res = new Array[Int](realK)

		var i = realFreqThreshold+1
		var freqSum = 0
		while (i > 1 && freqSum < realK) {
			i -= 1
			freqSum += lowFreqs(i)
		}

		val smallestFreq = i
		val wholeFreq    = i+1 // TODO: what if size of wholeFreq is bigger than realK, then method produces imprecise results

		var resIdx = 0

		var mapIdx = 0
		while (mapIdx < freq.length && resIdx < res.length) {
			if (freq(mapIdx) >= wholeFreq) {
				res(resIdx) = keys(mapIdx)
				resIdx += 1
			}
			mapIdx += 1
		}

		mapIdx = 0
		while (mapIdx < freq.length && resIdx < res.length) {
			if (freq(mapIdx) < wholeFreq && freq(mapIdx) >= smallestFreq) {
				res(resIdx) = keys(mapIdx)
				resIdx += 1
			}
			mapIdx += 1
		}

		res
	}

}
