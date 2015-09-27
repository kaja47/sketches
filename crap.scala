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

	require(loadFactor > 0.0 && loadFactor < 1.0)
	require(freqThreshold > 1)

	private[this] var capacity = math.max(Bits.higherPowerOfTwo(initialSize), 16)
	private[this] var _size = 0
	private[this] var maxSize = (capacity * loadFactor).toInt
	private[this] val realFreqThreshold = Bits.higherPowerOfTwo(freqThreshold)

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


class TopKFloatInt(k: Int) {
  private val heap = new MinFloatIntHeap(k+1)
  private var min = Float.NegativeInfinity

  def insert(key: Float, value: Int) = {
    if (heap.size < k) {
      heap.insert(key, value)

    } else if (key >= min) {
      heap.insert(key, value)
      heap.deleteMin()
      min = heap.minKey
    }
  }

  def values = {
    val res = new Array[Int](heap.size)
    var i = res.length-1 ; while (i >= 0) {
      res(i) = heap.minValue
      heap.deleteMin()
      i -= 1
    }
    res
  }
}

class MinFloatIntHeap(val capacity: Int) {
  private val heap = new MinIntIntHeap(capacity)

  def size = heap.size
  def isEmpty = heap.isEmpty
  def nonEmpty = heap.nonEmpty

  def insert(key: Float, value: Int) = heap.insert(Bits.floatToSortableInt(key), value)
  def minKey: Float = Bits.sortableIntToFloat(heap.minKey)
  def minValue: Int = heap.minValue
  def deleteMin(): Unit = heap.deleteMin()
}


class MinIntIntHeap(val capacity: Int) {

  // both key and index are packed inside one Long value
  // key which is used for comparison forms high 4 bytes of Long
  private val arr = new Array[Long](capacity+2)
  private var head = 0+1

  def size = head-1
  def isEmpty = head == (0+1)
  def nonEmpty = head != (0+1)

  def insert(key: Int, value: Int): Unit = {
    arr(head) = pack(key, value)
    swim(head)
    head += 1
  }

  def minKey: Int = high(arr(0+1))
  def minValue: Int = low(arr(0+1))

  def deleteMin(): Unit = {
    if (head == 0+1) throw new NoSuchElementException("underflow")
    //val minKey = high(arr(0+1))
    head -= 1
    swap(0+1, head)
    arr(head) = 0
    sink(0+1)
  }

  /** This method is equivalent to deleteMin() followed by insert(), but it's
    * more efficient. */
  def deleteMinAndInsert(key: Int, value: Int): Unit = {
    //deleteMin()
    //insert(key, value)
    if (head == 0+1) throw new NoSuchElementException("underflow")
    arr(0+1) = pack(key, value)
    sink(0+1)
  }


  private def pack(hi: Int, lo: Int): Long = hi.toLong << 32 | lo
  private def high(x: Long): Int = (x >>> 32).toInt
  private def low(x: Long): Int = x.toInt

  private def swap(a: Int, b: Int) = {
    val tmp = arr(a)
    arr(a) = arr(b)
    arr(b) = tmp
  }

  //private def parent(pos: Int) = (pos + 1) / 2 - 1
  //private def child(pos: Int) = pos * 2 + 1
  private def parent(pos: Int) = pos / 2
  private def child(pos: Int) = pos * 2

  // moves value at the given position up towards the root
  private def swim(_pos: Int): Unit = {
    var pos = _pos
    while (pos > 0+1 && arr(parent(pos)) > arr(pos)) {
      swap(parent(pos), pos)
      pos = parent(pos)
    }
  }

  // moves value at the given position down towards leaves
  private def sink(_pos: Int): Unit = {
    var pos = _pos
    while (child(pos) < head) {
      var ch = child(pos)
      //if (ch < (head - 1) && arr(ch) > arr(ch+1)) ch += 1
      ch += (((ch.toLong - (head - 1)) & (arr(ch+1) - arr(ch))) >>> 63).toInt
      if (arr(pos) <= arr(ch)) return
      swap(pos, ch)
      pos = ch
    }
  }

}


class Xorshift(var x: Int = System.currentTimeMillis.toInt, var y: Int = 4711, var z: Int = 5485612, var w: Int = 992121) {

  def nextInt(mask: Int): Int =
    math.abs(nextInt() & mask)

  def nextInt(): Int = {
    val t = x ^ (x << 11)
    x = y
    y = z
    z = w
    w = w ^ (w >>> 19) ^ t ^ (t >>> 8)
    w
  }
}
