package atrox


object TopKFloatIntEstimate {
  import sketch.HashFunc
  protected[atrox] val hf = Array.tabulate[HashFunc[Int]](256)(i => HashFunc.random(i * 4747))
}


/* Probabilistic version of TopK data structure that produces only distinct elements.
 * It's based on ideas of cuckoo hashing and Robin Hood hashing.
 * - https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf
 * - http://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/
 * - https://www.pvk.ca/Blog/more_numerical_experiments_in_hashing.html
 * */
class TopKFloatIntEstimate(k: Int, hashFunctions: Int, oversample: Int = 0) { self =>

  require(k > 0)
  require(hashFunctions < TopKFloatIntEstimate.hf.length)

  private[this] val kpow = Bits.higherPowerOfTwo(math.max(k, oversample))
  private[this] val arr = {
    val arr = new Array[Long](kpow)
    java.util.Arrays.fill(arr, Long.MinValue)
    arr
  }
  private[this] var min = Long.MinValue

  private def f(h: Int, pair: Long) =
    TopKFloatIntEstimate.hf(h)(keyint(pair) ^ value(pair)) & (kpow - 1)

  private def place(_pair: Long, h: Int): Unit = {
    var pair = _pair
    var i = 0 ; while (i < h) {
      val pos = f(i, pair)
      if (pair == arr(pos)) return // new value is the same as the value in array, filtering out duplicate
      if (pair > arr(pos)) {       // new value is bigger than the old value, try to place the old value to another position
        val oldPair = arr(pos)
        arr(pos) = pair
        if (oldPair <= min) {   // less then or equal comparison is there for handling array initialized to MinValue
          min = findMin()       // just removed current minimum, find a new one, this should be triggered rarely enough (1/kpow?)
          return                // encountered smaller value, it's not necessary to try to place it somewhere else
        }
        //place(oldPair, h)       // place old value
        //return                  // if index of hash function that hashes value to this position was encoded in the array slot,
                                // it wouldn't be necessary to try every hash position all over again (TODO?)
        pair = oldPair
        i = 0                   // manual tail recursion, baby!
      } // else try next hash
      i += 1
    }
  }

  protected def findMin() = {
    var min = Long.MaxValue
    var i = 0; while (i < arr.length) {
      min = java.lang.Math.min(min, arr(i))
      i += 1
    }
    min
  }

  def insert(key: Float, value: Int): Unit = {
    val pair = pack(key, value)
    if (pair == Long.MinValue) throw new IllegalArgumentException()
    if (pair > min) place(pair, hashFunctions)
  }

  def += (key: Float, value: Int) = insert(key, value)

  def ++= (tk: TopKFloatIntEstimate) {
    val cur = tk.cursor
    while (cur.moveNext) { insert(cur.key, cur.value) }
  }

  def drainToArray(): Array[Int] = {
    // TODO: heapify arr and return k items
    val buff = new collection.mutable.ArrayBuilder.ofInt
    buff.sizeHint(k)

    var i = 0 ; while (i < arr.length) {
      if (arr(i) != Long.MinValue) {
        buff += value(arr(i))
      }
      i += 1
    }
    buff.result
  }

  def cursor = rawCursor

  def rawCursor = new Cursor2[Float, Int] {
    private var pos = -1
    def moveNext() = { do { pos += 1 } while (pos < arr.length && arr(pos) == Long.MinValue) ; pos < arr.length }
    def key = self.key(arr(pos))
    def value = self.value(arr(pos))
  }

  private def pack(key: Float, value: Int) = Bits.packSortable(key, value)
  private def keyint(pair: Long): Int   = Bits.unpackIntHi(pair)
  private def key   (pair: Long): Float = Bits.unpackSortableFloatHi(pair)
  private def value (pair: Long): Int   = Bits.unpackIntLo(pair)
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
  def drainToArray(): Array[Int] = {
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
  protected def _insertFloat(key: Float, value: Int) = _insert(Bits.floatToSortableInt(key), value)
  protected def _minKeyFloat: Float = Bits.sortableIntToFloat(_minKey)
  protected def _deleteMinAndInsertFloat(key: Float, value: Int) = _deleteMinAndInsert(Bits.floatToSortableInt(key), value)
}


class MinIntIntHeap(capacity: Int) extends BaseMinIntIntHeap(capacity) {
  def insert(key: Int, value: Int) = _insert(key, value)
  def minKey: Int = _minKey
  def minValue: Int = _minValue
  def deleteMinAndInsert(key: Int, value: Int) = _deleteMinAndInsert(key, value)
}

object MinIntIntHeap {
  def builder(capacity: Int) = new MinIntIntHeapBuilder(capacity)
}

class MinIntIntHeapBuilder(capacity: Int) {
  private[this] var heap = new MinIntIntHeap(capacity)

  def insert(key: Int, value: Int) =
    heap._insertNoSwim(key, value)

  def result = {
    require(heap != null, "MinIntIntHeapBuilder cannot be reused")
    heap.makeHeap()
    val res = heap
    heap = null
    res
  }
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

  protected[atrox] def _insertNoSwim(key: Int, value: Int): Unit = {
    arr(top) = pack(key, value)
    top += 1
  }

  protected def _insert(key: Int, value: Int): Unit = {
    arr(top) = pack(key, value)
    swim(top)
    top += 1
  }

  protected def _minKey: Int = {
    if (top == 0) throw new NoSuchElementException
    high(arr(0))
  }
  protected def _minValue: Int = {
    if (top == 0) throw new NoSuchElementException
    low(arr(0))
  }

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
      if ((ch+1) < top && arr(ch+1) < arr(ch)) ch += 1
      if (key <= arr(ch)) {
        arr(pos) = key
        return
      }
      arr(pos) = arr(ch)
      pos = ch
    }
    arr(pos) = key
  }

  protected[atrox] def makeHeap() = {
    var i = capacity/2-1
    while (i >= 0) {
      sink(i)
      i -= 1
    }
  }

  private def isValidHeap =
    0 until capacity forall { i =>
      val ch = child(i)
      (ch   >= capacity || arr(ch)   >= arr(i)) &&
      (ch+1 >= capacity || arr(ch+1) >= arr(i))
    }

}



/*
class NakedHeaps(val capacity: Int, heaps: Int) {
  val arr = new Array[Long](heaps * (capacity+1))

  def getCell(idx: Int, pos: Int): Long
  def setCell(idx: Int, pos: Int, value: Long): Unit

  def top(idx: Int): Int = getCell(idx, 0)
  def setTop(idx: Int, top: Int): Unit = setCell(idx, 0, top)
}
*/
