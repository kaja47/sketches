package atrox

/** StringIndex is an extension of StringIntDictionary that assigns ordinal number
  * for every string key and stores compact inversion or original string -> int
  * mapping. Therefore it's possible to lookup values by keys and keys by
  * values. But by nature how data are stored (inlined or packed in one big
  * char array), every lookup by value alocates new string object. */
sealed class StringIndex(initialCapacity: Int = 1024) extends Serializable {
  class SID(initialCapacity: Int) extends StringIntDictionary(initialCapacity = initialCapacity) {
    var max = 0
    var arrTop = 0
    // either (offset, length) or (inlined string, length) pairs, packed in same way as the `assoc` array
    var arr = new Array[Int](initialCapacity)

    def _index(str: CharSequence) = {
      val idx = getOrElseUpdate(str, max)
      if (idx == max) { // new string inserted
        if ((arrTop+2) >= arr.length) growArr()

        val word = tryInline(str)
        if (word != 0xffffffffffffffffL) {
          setInlinedWord(arr, arrTop, word)
          setInlined(arr, arrTop)
          setInlinedStringLength(arr, arrTop, str.length)
        } else {
          val pos = findPos(str, word)
          val offset = stringOffset(assoc, pos)
          setStringOffset(arr, arrTop, offset)
          setPackedStringLength(arr, arrTop, str.length)
        }

        arrTop += 2
        max += 1
      }
      idx
    }

    def _get(pos: Int) = makeString(arr, pos * 2)

    def growArr() = {
      val newArr = new Array[Int](arr.length * 2)
      System.arraycopy(arr, 0, newArr, 0, arr.length)
      arr = newArr
    }
  }

  private val sid = new SID(initialCapacity)

  /** Returns an integer index for the given string, adding it to the
    * index if it is not already present. */
  def index(str: CharSequence): Int = sid._index(str)

  /** Returns the int id of the given element (0-based) or -1 if not
    * found in the index. This method never changes the index. */
  def apply(str: CharSequence): Int = sid.getOrDefault(str, -1)

  /** Returns a string at the given position or throws
    * IndexOutOfBoundsException if it's not found. */
  def get(pos: Int): String   = sid._get(pos)

  /** Number of elements in this index. */
  def size = sid.size

  /** Returns true if this index contains the string t. */
  def contains(str: CharSequence) = sid.contains(str)
}



final class ConcurrentStringIndex(initialCapacity: Int = 1024) extends StringIndex(initialCapacity) {

  private val rwlock = new java.util.concurrent.locks.ReentrantReadWriteLock
  private val rlock = rwlock.readLock
  private val wlock = rwlock.writeLock

  /** Returns an integer index for the given string, adding it to the
    * index if it is not already present. */
  override def index(str: CharSequence): Int = {
    rlock.lock()
    val idx = try { super.apply(str) }
    finally { rlock.unlock() }
    if (idx != -1) {
      idx
    } else {
      wlock.lock()
      try { super.index(str) }
      finally { wlock.unlock() }
    }
  }

  /** Returns the int id of the given element (0-based) or -1 if not
    * found in the index. This method never changes the index. */
  override def apply(str: CharSequence): Int = {
    rlock.lock()
    try { super.apply(str) }
    finally { rlock.unlock() }
  }

  /** Returns a string at the given position or throws
    * IndexOutOfBoundsException if it's not found. */
  override def get(pos: Int): String = {
    rlock.lock()
    try { super.get(pos) }
    finally { rlock.unlock() }
  }

  /** Number of elements in this index. */
  override def size = {
    rlock.lock()
    try { super.size }
    finally { rlock.unlock() }
  }

  /** Returns true if this index contains the string t. */
  override def contains(str: CharSequence) = {
    rlock.lock()
    try { super.contains(str) }
    finally { rlock.unlock() }
  }
}
