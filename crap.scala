package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector }
import scala. { specialized => spec }

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


/** Koloboke-style cursors.
  *
  * usage: while (cur.moveNext()) { doSomethingWith(cur.value) }
  * */
trait Cursor[@spec(Int, Long, Float, Double) V] {
  def moveNext(): Boolean
  def value: V
}

trait Cursor2[@spec(Int, Long, Float, Double) K, @spec(Int, Long, Float, Double) V] { self =>
  def moveNext(): Boolean
  def key: K
  def value: V

  def asKeys = new Cursor[K] {
    def moveNext(): Boolean = self.moveNext
    def value: K = self.key
  }

  def asValues = new Cursor[V] {
    def moveNext(): Boolean = self.moveNext
    def value: V = self.value
  }

  def swap = new Cursor2[V, K] {
    def moveNext() = self.moveNext
    def key        = self.value
    def value      = self.key
  }
}


/** Set specialized for int values that uses direct hashing.
  *
  * slots states: free (00) → occupied (10) → deleted (11)
  */
class IntSet(initialSize: Int = 16, loadFactor: Double = 0.5) {
  require(loadFactor > 0.0 && loadFactor < 1.0)

  private[this] var capacity    = math.max(Bits.higherPowerOfTwo(initialSize), 8)
  private[this] var bitmapWords = getBitmapWords(capacity)
  private[this] var maxSize     = getMaxSize(capacity)
  private[this] var arr: Array[Int] = new Array[Int](capacity + bitmapWords)
  private[this] var filled = 0
  private[this] var _size = 0

  def size = _size

  def += (k: Int): this.type = {
    val i = findIdx(k)
    if (!isOccupied(arr, i)) {
      _size += 1
      filled += 1
    }
    setOccupied(arr, i)
    arr(bitmapWords + i) = k
    if (filled > maxSize) {
      if (size <= filled / 2) grow(1)
      else grow(2)
    }
    this
  }

  def ++= (cur: Cursor[Int]): this.type = {
    while (cur.moveNext) { this += cur.value }
    this
  }


  def -= (k: Int): this.type = {
    val i = findIdx(k)
    if (!isOccupied(arr, i)) return this
    // if the next slot is not occupied (and therefore also not deleted), delete directly
    if (!isOccupied(arr, (i + 1) & (capacity - 1))) {
      _size -= 1
      filled -= 1
      setUnoccupied(arr, i)
    } else {
      _size -= 1
      setDeleted(arr, i)
    }
    this
  }

  def contains(k: Int): Boolean = {
    val i = findIdx(k)
    isOccupied(arr, i) && !isDeleted(arr, i) && arr(bitmapWords + i) == k
  }

  def toArray: Array[Int] = toArray(new Array[Int](size), 0)

  def toArray(res: Array[Int], off: Int): Array[Int] = {
    var i, j = 0
    while (i < capacity) {
      if (isOccupied(arr, i) && !isDeleted(arr, i)) {
        res(j+off) = arr(bitmapWords + i)
        j += 1
      }
      i += 1
    }
    res
  }

  def clear() = {
    filled = 0
    _size = 0
    var i = 0 ; while (i < arr.length) {
      arr(i) = 0
      i += 1
    }
  }

  // Get position of the first empty slot or slot containing value k.
  // Must never return deleted slot.
  private def findIdx(k: Int) = {
    val mask = capacity - 1
    var pos = k & mask
    var i = pos
    while (isDeleted(arr, i) | (isOccupied(arr, i) && arr(bitmapWords + i) != k)) {
      i = (i + 1) & mask
    }
    i
  }

  protected def getBitmapWords(capacity: Int) = ((capacity * 2) + 31) / 32
  protected def getMaxSize(capacity: Int) = (capacity * loadFactor).toInt

  def getBit(arr: Array[Int], bit: Int) = (arr(bit / 32) & (1 << (bit % 32))) != 0
  def setBit(arr: Array[Int], bit: Int) = arr(bit / 32) |= (1 << (bit % 32))
  def clrBit(arr: Array[Int], bit: Int) = arr(bit / 32) &= ~(1 << (bit % 32))

  private def isOccupied(arr: Array[Int], idx: Int)    = getBit(arr, idx*2)
  private def setOccupied(arr: Array[Int], idx: Int)   = setBit(arr, idx*2)
  private def setUnoccupied(arr: Array[Int], idx: Int) = clrBit(arr, idx*2)
  private def isDeleted(arr: Array[Int], idx: Int)     = getBit(arr, idx*2+1)
  private def setDeleted(arr: Array[Int], idx: Int)    = setBit(arr, idx*2+1)

  private def grow(factor: Int): Unit = {
    val oldCap = capacity
    val oldBmw = bitmapWords
    val oldArr = arr

    capacity *= factor
    bitmapWords = getBitmapWords(capacity)
    maxSize = getMaxSize(capacity)
    arr = new Array[Int](capacity + bitmapWords)
    filled = 0
    _size = 0

    var i = 0
    while (i < oldCap) {
      if (isOccupied(oldArr, i) && !isDeleted(oldArr, i)) {
        val k = oldArr(oldBmw + i)
        this += k
      }
      i += 1
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

  def cursor = new Cursor2[Int, Int] {
    private var pos = -1
    def moveNext() = {
      do { pos += 1 } while (pos < capacity && freq(pos) <= 0)
      pos < capacity
    }
    def key = keys(pos)
    def value = freq(pos)
  }

}


class TopKFloatInt(k: Int, distinct: Boolean = false) extends BaseMinFloatIntHeap(k) {
//  private var valueSet: IntSet = if (distinct) new IntSet() else null
  protected var min = Float.NegativeInfinity

  /** returns value that was deleted or Int.MinValue */
  def insert(key: Float, value: Int): Unit = {
    if (size < k) {
      if (!distinct || !_containsValue(value)) {
        _insertFloat(key, value)
        min = _minKeyFloat
//        if (distinct) {
//          valueSet += value
//        }
      }

    } else if (key > min) {
      if (!distinct || !_containsValue(value)) {
        _deleteMinAndInsertFloat(key, value)
        min = _minKeyFloat
//        if (distinct) {
//          valueSet -= _minValue
//          valueSet += value
//        }
      }

    }
  }

  /** Scanning the whole heap is faster than search in a auxiliary set for
    * reasonable small heaps (and consume dramatically less mememory). This is
    * caused by the fact that search in the auxiliary set needs two dependent
    * dereferences which most likely lead to cache misses. When scanning costs
    * more than those 2 misses, it's preferable to use the auxiliary set. */
  protected def _containsValue(value: Int): Boolean = {
    var i = 0 ; while (i < top) {
      if (low(arr(i)) == value) return true
      i += 1
    }
    false
  }

  def += (key: Float, value: Int) = insert(key, value)

  def ++= (tk: TopKFloatInt) {
    // backwrds iterations because that way heap is filled by big values and
    // rest is filered out by `key > min` condition in the insert method
    var i = tk.top - 1; while (i >= 1) {
      val key = Bits.sortableIntToFloat(high(tk.arr(i)))
      val value = low(tk.arr(i))
      insert(key, value)
      i -= 1
    }
  }

  /** Return the content (the value part of key-value pair) of this heap sorted
    * by the key part. This collection is emptied. */
  def drainToArray() = {
    val res = new Array[Int](size)
    var i = res.length-1 ; while (i >= 0) {
      res(i) = _minValue
      deleteMin()
      i -= 1
    }
    res
  }

  def head: Int = _minValue
  def minKey: Float = _minKeyFloat
  def minValue: Int = _minValue

  override def toString = arr.drop(1).take(size).map(l => (Bits.sortableIntToFloat(high(l)), low(l))).mkString("TopKFloatInt(", ",", ")")

  def cursor = new Cursor2[Float, Int] {
    private var pos = -1
    def moveNext() = { pos += 1 ; pos < top }
    def key = Bits.sortableIntToFloat(high(arr(pos)))
    def value = low(arr(pos))
  }

  def valuesCursor = cursor.asValues
}


class MinFloatIntHeap(capacity: Int) extends BaseMinFloatIntHeap(capacity) {
  def insert(key: Float, value: Int) = _insertFloat(key, value)
  def minKey: Float = _minKeyFloat
  def minValue: Int = _minValue
  def deleteMinAndInsert(key: Float, value: Int) = _deleteMinAndInsertFloat(key, value)
}

abstract class BaseMinFloatIntHeap(capacity: Int) extends BaseMinIntIntHeap(capacity) {
  def _insertFloat(key: Float, value: Int) = _insert(Bits.floatToSortableInt(key), value)
  def _minKeyFloat: Float = Bits.sortableIntToFloat(_minKey)
  def _deleteMinAndInsertFloat(key: Float, value: Int) = _deleteMinAndInsert(Bits.floatToSortableInt(key), value)
}


class MinIntIntHeap(capacity: Int) extends BaseMinIntIntHeap(capacity) {
  def insert(key: Int, value: Int) = _insert(key, value)
  def minKey: Int = _minKey
  def minValue: Int = _minValue
  def deleteMinAndInsert(key: Int, value: Int) = _deleteMinAndInsert(key, value)
}

abstract class BaseMinIntIntHeap(val capacity: Int) {

  // Both key and index are packed inside one Long value.
  // Key which is used for comparison forms high 4 bytes of said Long.
  protected val arr = new Array[Long](capacity)
  // top points behind the last element
  protected var top = 0

  def size = top
  def isEmpty = top == (0)
  def nonEmpty = top != (0)

  protected def _insert(key: Int, value: Int): Unit = {
    arr(top) = pack(key, value)
    swim(top)
    top += 1
  }

  protected def _minKey: Int = high(arr(0))
  protected def _minValue: Int = low(arr(0))

  def deleteMin(): Unit = {
    if (top == 0) throw new NoSuchElementException("underflow")
    //val minKey = high(arr(0))
    top -= 1
    swap(0, top)
    arr(top) = 0
    sink(0)
  }

  /** This method is equivalent to deleteMin() followed by insert(), but it's
    * more efficient. */
  protected def _deleteMinAndInsert(key: Int, value: Int): Unit = {
    //deleteMin()
    //insert(key, value)
    if (top == 0) throw new NoSuchElementException("underflow")
    arr(0) = pack(key, value)
    sink(0)
  }


  protected def pack(hi: Int, lo: Int): Long = hi.toLong << 32 | lo
  protected def high(x: Long): Int = (x >>> 32).toInt
  protected def low(x: Long): Int = x.toInt

  private def swap(a: Int, b: Int) = {
    val tmp = arr(a)
    arr(a) = arr(b)
    arr(b) = tmp
  }

  private def parent(pos: Int) = (pos - 1) / 2
  private def child(pos: Int) = pos * 2 + 1

  // moves value at the given position up towards the root
  private def swim(_pos: Int): Unit = {
    var pos = _pos
    while (pos > 0 && arr(parent(pos)) > arr(pos)) {
      swap(parent(pos), pos)
      pos = parent(pos)
    }
  }

  // moves value at the given position down towards leaves
  private def sink(_pos: Int): Unit = {
    val key = arr(_pos)
    var pos = _pos
    while (child(pos) < top) {
      var ch = child(pos)
      if (ch < (top - 1) && arr(ch) > arr(ch+1)) ch += 1
//      if (arr(pos) <= arr(ch)) return
      if (key <= arr(ch)) {
        arr(pos) = key
        return
      }
//      swap(pos, ch)
      arr(pos) = arr(ch)
      pos = ch
    }
    arr(pos) = key
  }

}


class Xorshift(var x: Int = System.currentTimeMillis.toInt, var y: Int = 4711, var z: Int = 5485612, var w: Int = 992121) {
  def nextInt(): Int = {
    val t = x ^ (x << 11)
    x = y
    y = z
    z = w
    w = w ^ (w >>> 19) ^ t ^ (t >>> 8)
    w
  }
}
