package atrox

import scala. { specialized => spec }

/** Koloboke-style cursors.
  *
  * usage: while (cur.moveNext()) { doSomethingWith(cur.value) }
  * */
abstract class Cursor[@spec(Int, Long, Float, Double) V] {
  def moveNext(): Boolean
  def value: V
}

abstract class Cursor2[@spec(Int, Long, Float, Double) K, @spec(Int, Long, Float, Double) V] { self =>
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




class ReusableIntArrayBuilder(initialSize: Int = 16) {
  import java.util.Arrays

  require(initialSize > 0)

  private[this] var capacity = initialSize
  private[this] var pos = 0 // points behing last element
  private[this] var arr = new Array[Int](initialSize)

  def size = pos

  def apply(i: Int) = {
    if (i >= pos) throw new IndexOutOfBoundsException
    arr(i)
  }

  def += (x: Int) = {
    arr(pos) = x
    pos += 1
    if (pos == capacity) {
      arr = Arrays.copyOfRange(arr, 0, capacity * 2)
      capacity = arr.length
    }
  }

  def ++= (xs: Array[Int]) = {
    if (pos + xs.length >= capacity) {
      arr = Arrays.copyOfRange(arr, 0, Bits.higherPowerOfTwo(pos + xs.length))
      capacity = arr.length
    }

    System.arraycopy(xs, 0, arr, pos, xs.length)
    pos += xs.length
  }

  /** Produces an array from added elements. The builder's contents is
    * empty after this operation and can be safely used again. */
  def result: Array[Int] = {
    val res = Arrays.copyOfRange(arr, 0, pos)
    pos = 0
    res
  }

  def nonEmptyResult =
    if (pos == 0) null else result

  def foreach(f: Int => Unit) = {
    var i = 0; while (i < pos) {
      f(arr(i))
      i += 1
    }
  }
}
