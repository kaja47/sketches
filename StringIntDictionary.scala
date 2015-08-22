package atrox

import scala.language.postfixOps
import java.lang.Integer.highestOneBit
import java.lang.Long.{ reverse, reverseBytes }
import java.lang.Math.{ min, max }


/** Class StringIntDictionary implements idiosyncratic map with string keys and
  * int values. This map is specialized to hold english words and other short
  * alphanumeric strings as it's keys. Strings shorter than 10 characters are
  * compressed and inlined into associative part of the map. Long strings (up to
  * 255 characters) are stored in a one continuous char arraym reducing overhead
  * of object headers a and internal string structure. Consequence of this
  * layout is compactness of the StringIntDictionary and very good cache
  * behavior. Lookup of inlined string needs only 1 memory access and lookup of
  * uninlined string needs 2 dependent memory accesses. The j.u.HashMap needs at
  * least 4 memory access in order to read a string key and another one to read
  * a value.
  *
  * Data are packed in array of segments of 3 ints. It's either in plain format
  * [ 32 bit offsets into char array | 24 bit nothing, 8 low order bit length | 32 bit payload ]
  * or in inlined format
  * [ 32 bit packed chars (A) | empty bit, isInlined bit, 22 bits of packed chars (B) | 32 bit payload ]
  * Charcters are packed in 6 bit chunks of a word that's constructed in this way:
  * A | ((B & 0xffffff00L) << 24)
  *
  * The StringIntDictionary should be faster than j.u.HashMap[String, Int] in
  * most cases. When it fits into cache, it's only slightly faster,
  * but if it no loger fits into cache, lookup can be up to 3 times faster.
  * But when string hashes starts to collide, performance degenerate very quickly.
  *
  * Warning: Might contains subtle and not-so-soubtle errors.
  */
class StringIntDictionary(initialCapacity: Int = 1024, loadFactor: Double = 0.45)
    extends StringDictionaryBase(initialCapacity, loadFactor, 3) {

  protected val segmentLength = 3

  /** Associates the specified value with the specified key in this map. */
  def put(str: CharSequence, value: Int): Unit =
    _put(str, value)

  /** Associates the specified value with the specified key in this map if the
    * key is not already present. */
  def putIfAbsent(str: CharSequence, value: Int): Unit =
    _putIfAbsent(str, value)

  def remove(key: CharSequence): Int =
    _remove(key).toInt
  def get(str: CharSequence) =
    _getOrDefault(str, defaultValue).toInt

  def getOrDefault(str: CharSequence, defaultValue: Int): Int =
    _getOrDefault(str, defaultValue).toInt

  def getOrElseUpdate(str: CharSequence, value: Int): Int =
    _getOrElseUpdate(str, value).toInt

  /** Adds the given addition value to the value associated with the specified
    * key, or defaultValue if this map contains no mapping for the key, and
    * associates the resulting value with the key. */
  def addValue(str: CharSequence, addition: Int): Int =
    addValue(str, addition, defaultValue.toInt)

  def addValue(str: CharSequence, addition: Int, defaultValue: Int): Int =
    _addValue(str, addition, defaultValue).toInt

  def iterator: Iterator[(String, Int)] = Iterator.tabulate(capacity) { i =>
    val pos = i * segmentLength
    if (!isAlive(assoc, pos)) null
    else (makeString(assoc, pos), payload(assoc, pos).toInt)
  } filter (_ != null)

  protected def payload(assoc: Array[Int], pos: Int): Long = assoc(pos+2)
  protected def setPayload(assoc: Array[Int], pos: Int, value: Long) = assoc(pos+2) = value.toInt
}


/** see StringIntDictionary
  * This version stores every long value in 2 cells of int array. */
class StringLongDictionary(initialCapacity: Int = 1024, loadFactor: Double = 0.45)
    extends StringDictionaryBase(initialCapacity, loadFactor, 4) {

  protected val segmentLength = 4

  def put(str: CharSequence, value: Long): Unit =
    _put(str, value)

  def remove(key: CharSequence): Long =
    _remove(key)
  def putIfAbsent(str: CharSequence, value: Long): Unit =
    _putIfAbsent(str, value)

  def get(str: CharSequence) =
    _getOrDefault(str, defaultValue)

  def getOrDefault(str: CharSequence, defaultValue: Long): Long =
    _getOrDefault(str, defaultValue)

  def getOrElseUpdate(str: CharSequence, value: Long): Long =
    _getOrElseUpdate(str, value)

  def addValue(str: CharSequence, addition: Long): Long =
    addValue(str, addition, defaultValue)

  def addValue(str: CharSequence, addition: Long, defaultValue: Long): Long =
    _addValue(str, addition, defaultValue)

  def iterator: Iterator[(String, Long)] = Iterator.tabulate(capacity) { i =>
    val pos = i * segmentLength
    if (!isAlive(assoc, pos)) null
    else (makeString(assoc, pos), payload(assoc, pos))
  } filter (_ != null)

  protected def payload(assoc: Array[Int], pos: Int): Long = assoc(pos+2).toLong << 32 | assoc(pos+3)
  protected def setPayload(assoc: Array[Int], pos: Int, value: Long) = {
    assoc(pos+2) = ((value >>> 32) & 0xffffffffL).toInt
    assoc(pos+3) = (value & 0xffffffffL).toInt
  }

}


class StringSet(initialCapacity: Int = 1024, loadFactor: Double = 0.45)
    extends StringDictionaryBase(initialCapacity, loadFactor, 2) with (CharSequence => Boolean) {

  protected val segmentLength = 2

  def put(str: CharSequence): Unit = _put(str, 0)

  def += (str: CharSequence): Unit = put(str)
  def apply(str: CharSequence): Boolean = contains(str)

  def iterator: Iterator[String] = Iterator.tabulate(capacity) { i =>
    val pos = i * segmentLength
    if (!isAlive(assoc, pos)) null
    else makeString(assoc, pos)
  } filter (_ != null)

  protected def payload(assoc: Array[Int], pos: Int): Long = 0
  protected def setPayload(assoc: Array[Int], pos: Int, value: Long) = {}
}



abstract class StringDictionaryBase(initialCapacity: Int = 1024, val loadFactor: Double = 0.45, sl: Int) {
  require(loadFactor > 0 && loadFactor < 1, "load factor must be in range from 0 to 1 (exclusive)")

  protected def segmentLength: Int

  protected var capacity = max(higherPowerOfTwo(initialCapacity), 16)
  protected var occupied = 0
  protected var removed  = 0
  protected var maxOccupied = min(capacity * loadFactor toInt, capacity - 1)
  protected var charsTop = 0

  protected var assoc = new Array[Int](capacity * sl) // sl is equals to segmentLength, it's passed as arg to bypass early initialization problem
  protected var chars = new Array[Char](capacity * 8)

  protected val defaultValue = 0L

  // debug information
  var _puts       = 0L
  var _gets       = 0L
  var _dels       = 0L
  var _fastdels   = 0L
  var _reputs     = 0L
  var _probes     = 0L
  var _growProbes = 0L
  var _leak = 0L
  def _byteSize   = assoc.length * 4 + chars.length * 2
  def _capacity   = capacity

  private def higherPowerOfTwo(x: Int) =
    highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)


  // public API methods
  // These methods are protected because real public facing API might need to
  // do some value mangling to translate Longs to a appropriate type.

  protected def _put(str: CharSequence, value: Long): Unit =
    putInternal(str, value, true)

  protected def _putIfAbsent(str: CharSequence, value: Long): Unit =
    putInternal(str, value, false)

  protected def _remove(key: CharSequence): Long = {
    _dels += 1
    val inlinedKey = tryInline(key)
    var pos = findPos(key, inlinedKey)
    removePos(pos)
  }

  protected def _getOrDefault(str: CharSequence, defaultValue: Long): Long = {
    _gets += 1
    val pos = findPos(str, tryInline(str))
    getInternal(pos, defaultValue)
  }

  protected def _getOrElseUpdate(str: CharSequence, value: Long): Long =
    putInternal(str, value, false)

  protected def _addValue(str: CharSequence, addition: Long, defaultValue: Long): Long = {
    val inlinedStr = tryInline(str)
    var pos = findPos(str, inlinedStr)
    val value = _add(getInternal(pos, defaultValue), addition)
    putInternalToPos(str, inlinedStr, pos, value, true)
    value
  }

  /** Performs addition on two values encoded into bits of two Long values */
  protected def _add(x: Long, y: Long): Long = x + y



  def contains(str: CharSequence): Boolean =
    isAlive(assoc, findPos(str, tryInline(str)))

  def size = occupied - removed


  // internals

  protected def getInternal(pos: Int, defaultValue: Long): Long =
    if (isAlive(assoc, pos)) payload(assoc, pos) else defaultValue

  protected def putInternal(str: CharSequence, value: Long, overwrite: Boolean): Long = {
    _puts += 1
    val inlinedStr = tryInline(str)
    var pos = findPos(str, inlinedStr)
    putInternalToPos(str, inlinedStr, pos, value, overwrite)
  }

  protected def putInternalToPos(str: CharSequence, inlinedStr: Long, pos: Int, value: Long, overwrite: Boolean): Long = {
    if (isAlive(assoc, pos)) { // key `str` is already present
      if (overwrite) {
        setPayload(assoc, pos, value)
        value
      } else {
        payload(assoc, pos)
      }
    } else { // empty slot or removed slot that used to have the same key
      // this is checked early because alive bit is overwritten by setInlinedWord
      val isrem = isRemoved(assoc, pos)

      if (inlinedStr != 0xffffffffffffffffL) {
        setInlinedWord(assoc, pos, inlinedStr)
        setInlined(assoc, pos)
        setInlinedStringLength(assoc, pos, str.length)

      } else {
        val offset = putString(str)
        setStringOffset(assoc, pos, offset)
        setPackedStringLength(assoc, pos, str.length)
      }
      if (!isrem) {
        occupied += 1
      } else {
        removed -= 1
        _reputs += 1
      }
      setAlive(assoc, pos)
      setPayload(assoc, pos, value)

      // grow must be called after insertion of the new element
      // otherwise the `pos` returned by findPos is no longer valid
      if (occupied > maxOccupied) grow()

      value
    }
  }

  protected def removePos(pos: Int): Long = {
    def nextPos(pos: Int) = if (pos + segmentLength >= capacity * segmentLength) 0 else pos + segmentLength

    if (!isAlive(assoc, pos)) return defaultValue

    if (isInlined(assoc, pos) && isEmpty(assoc, nextPos(pos))) {
      // Fast path: Element can be removed in place if it's inlined and it's
      // the last element of probing chain. This way, direct removal doesn't
      // break probing for other keys.
      _fastdels += 1
      val p = payload(assoc, pos)
      setEmpty(assoc, pos)
      occupied -= 1
      p
    } else {
      val p = payload(assoc, pos)
      setRemoved(assoc, pos)
      removed += 1
      p
    }
  }
  protected def encodeWord(str: CharSequence): Long = {
    var word = 0L
    var i = 0
    while (i < str.length) {
      word = setInlinedChar(word, i, encode(str.charAt(i)))
      i += 1
    }
    word
  }


  protected def stringHashCode(str: CharSequence): Int = {
    if (str.isInstanceOf[String]) {
      // String hashCode is defined to be computed as bellow,
      // but its value might be cached in the object instance and this might
      // give us a tiny performance boost
      str.hashCode
    } else {
      var h = 0
      var i = 0
      while (i < str.length) {
        h = 31 * h + str.charAt(i)
        i += 1
      }
      h
    }
  }

  /** must produce same result as stringHashCode */
  protected def charArrHashCode(arr: Array[Char], offset: Int, length: Int): Int = {
    var h = 0
    var i = 0
    while (i < length) {
      h = 31 * h + arr(offset + i)
      i += 1
    }
    h
  }

  protected def inlinedHashCode(inlinedStr: Long): Int = {
    // YOLO hashing scheme: it's faster but it produces more collisions on corpus of english words
    //inlinedStr.toInt ^ (inlinedStr >> 32).toInt

    // poor man's universal hashing
    reverseBytes(inlinedStr * 3542462394158182007L + 1775710242L).toInt

    // slow and boring way
    //var h = 0
    //var i = 0
    //while (i < length) {
    //  h = 31 * h + decode(inlinedChar(inlinedStr, i))
    //  i += 1
    //}
    //h
  }

  protected def makeString(assoc: Array[Int], pos: Int): String =
    if (isInlined(assoc, pos)) {
      inlinedToString(assoc, pos)
    } else {
      charsToString(assoc, pos)
    }

  protected def inlinedToString(assoc: Array[Int], pos: Int) = {
    val len = inlinedStringLength(assoc, pos)
    val word = inlinedWord(assoc, pos)
    val arr = new Array[Char](len)
    var i = 0
    while (i < len) {
      arr(i) = decode(inlinedChar(word, i))
      i += 1
    }
    new String(arr)
  }

  protected def charsToString(assoc: Array[Int], pos: Int) = {
    val len = packedStringLength(assoc, pos)
    val off = stringOffset(assoc, pos)
    new String(chars, off, len)
  }

  /** Returns a position of a slot with the specified key or the first empty
    * slot or removed slot that used to have the specified key. ie. the first
    * slot where the requested pair is located or where it could be inserted. */
  protected def findPos(str: CharSequence, inlined: Long) = {
    val mask = capacity - 1

    if (inlined != 0xffffffffffffffffL) {
      val hash = inlinedHashCode(inlined)
      var i = hash & mask
      var pos = i * segmentLength
      while (!equalOrEmptyInlined(pos, str, inlined)) {
        i = (i + 1) & mask ;
        pos = i * segmentLength ;
        _probes += 1
      }
      pos

    } else {
      val hash = stringHashCode(str)
      var i = hash & mask
      var pos = i * segmentLength
      while (!equalOrEmptyNotInlined(pos, str)) {
        i = (i + 1) & mask ;
        pos = i * segmentLength ;
        _probes += 1
      }
      pos
    }

  }

  // pos refers to the first element in a segment
  //
  // bit patterns (ordered from 31th bit to 0th bit)
  //
  // empty           = 0, 0, 30x 0
  // packed          = 0, 1, 30 bits length
  // inlined         = 1, 1, 22 bits chars, 8 bits length
  // removed packed  = 0, 0, 30 bits length
  // removed inlined = 1, 0, 22 bits chars, 8 bits length
  //
  // zero length strings will always be encoded as inlined strings
  protected def isEmpty  (assoc: Array[Int], pos: Int) = assoc(pos+1) == 0
  protected def isRemoved(assoc: Array[Int], pos: Int) = assoc(pos+1) != 0 && (assoc(pos+1) & (1 << 30)) == 0
  protected def isAlive  (assoc: Array[Int], pos: Int) =                      (assoc(pos+1) & (1 << 30)) != 0
  protected def isInlined(assoc: Array[Int], pos: Int) = (assoc(pos+1) & (1 << 31)) != 0

  protected def setEmpty  (assoc: Array[Int], pos: Int) = { assoc(pos) = 0 ; assoc(pos+1) = 0 }
  protected def setRemoved(assoc: Array[Int], pos: Int) = assoc(pos+1) &= ~(1 << 30)
  protected def setAlive  (assoc: Array[Int], pos: Int) = assoc(pos+1) |=  (1 << 30)
  protected def setInlined(assoc: Array[Int], pos: Int) = assoc(pos+1) |=  (1 << 31)

  protected def packedStringLength (assoc: Array[Int], pos: Int) = assoc(pos+1) & ((1 << 30) - 1)
  protected def inlinedStringLength(assoc: Array[Int], pos: Int) = assoc(pos+1) & ((1 <<  8) - 1)
  protected def stringOffset(assoc: Array[Int], pos: Int) = assoc(pos)

  protected def setPackedStringLength (assoc: Array[Int], pos: Int, length: Int) =
    assoc(pos+1) = assoc(pos+1) | length
  protected def setInlinedStringLength(assoc: Array[Int], pos: Int, length: Int) =
    assoc(pos+1) = (assoc(pos+1) & 0xffffff00) | length
  protected def setStringOffset(assoc: Array[Int], pos: Int, offset: Int) =
    assoc(pos) = offset

  protected def inlinedWord(assoc: Array[Int], pos: Int): Long =
    (assoc(pos).toLong & 0xffffffffL) | ((assoc(pos+1).toLong & 0x3fffff00L) << 24)
  protected def inlinedChar(word: Long, i: Int): Int =
    ((word >>> (6 * i)) & ((1 << 6) - 1)).toInt

  // overwrites inlined/alive bits and length
  protected def setInlinedWord(assoc: Array[Int], pos: Int, inlinedStr: Long) = {
    assoc(pos)   = (inlinedStr & 0xffffffff).toInt
    assoc(pos+1) = (inlinedStr >>> 24).toInt
  }
  private def setInlinedChar(inlinedStr: Long, i: Int, value: Int): Long = {
    //require(value < 64)
    inlinedStr | (value.toLong << (6 * i))
  }

  protected def payload(assoc: Array[Int], pos: Int): Long
  protected def setPayload(assoc: Array[Int], pos: Int, value: Long): Unit


  protected def equalOrEmptyInlined(pos: Int, str: CharSequence, inlinedStr: Long): Boolean = {
    val inl   = isInlined(assoc, pos)

    isEmpty(assoc, pos) || (inl && inlinedWord(assoc, pos) == inlinedStr)
  }

  /** In this case removed elements are ignored */
  protected def equalOrEmptyNotInlined(pos: Int, str: CharSequence): Boolean = {
    if (isEmpty(assoc, pos)) return true
    if (isInlined(assoc, pos)) return false
    val len = packedStringLength(assoc, pos)
    if (len != str.length) return false

    val off = stringOffset(assoc, pos)
    var i = 0
    while (i < len) {
      if (chars(off+i) != str.charAt(i)) return false
      i += 1
    }

    true
  }


  protected def tryInline(str: CharSequence): Long = {
    if (str.length > 9) return 0xffffffffffffffffL

    var word = 0L
    var i = 0
    while (i < str.length) {
      val ch = str.charAt(i)
      if (ch < 128 && encodeTable(ch) != -1) {
        word = setInlinedChar(word, i, encodeTable(ch).toInt)
      } else {
        return 0xffffffffffffffffL
      }
      i += 1
    }

    word
  }


  protected val encodeTable: Array[Byte] = {
    val arr = new Array[Byte](128)
    var ch = 'a'.toInt
    var i = 0

    while (i < 128) {
      arr(i) = -1
      i += 1
    }
    i = 1

    ch = '0' ; while (ch <= '9') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }
    ch = 'A' ; while (ch <= 'Z') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }
    ch = 'a' ; while (ch <= 'z') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }

    arr
  }

  protected def encode(ch: Char): Int = encodeTable(ch).toInt

  protected def decode(x: Int): Char = {
    if (x < 10+1) return (x-1      + '0').toChar
    if (x < 36+1) return (x-1 - 10 + 'A').toChar
    /*if (x <= 62+1)*/ return (x-1 - 36 + 'a').toChar
  }


  protected def grow() = {
    val oldAssoc = assoc
    val oldCapacity = capacity
    this.assoc = new Array[Int](assoc.length * 2)
    this.capacity = capacity * 2
    this.maxOccupied = capacity * loadFactor toInt

    var newOccupied = 0

    var oldPos = 0
    while (oldPos < (oldCapacity * segmentLength)) {
      if (isAlive(oldAssoc, oldPos)) {
        val inl = isInlined(oldAssoc, oldPos)

        val hash = if (inl) {
          inlinedHashCode(inlinedWord(oldAssoc, oldPos))
        } else {
          val len = packedStringLength(oldAssoc, oldPos)
          charArrHashCode(chars, stringOffset(oldAssoc, oldPos), len)
        }

        val mask = capacity - 1
        var i = hash & mask
        var pos = i * segmentLength
        while (!isEmpty(assoc, pos)) { // search for space in newly allocated array
          i = (i + 1) & mask
          pos = i * segmentLength
          _growProbes += 1
        }

        // copy segment
        var j = 0
        while (j < segmentLength) {
          assoc(pos+j) = oldAssoc(oldPos+j)
          j += 1
        }

        newOccupied += 1
      } else if (isRemoved(oldAssoc, oldPos)) {
        // currently removing of packed string causes memory leak
        // this abhorent negligence will be rectified in the near future
      }

      oldPos += segmentLength
    }

    this.occupied = newOccupied
    this.removed = 0
  }



  /** puts string into the `chars` array and returns it's position */
  protected def putString(str: CharSequence): Int = {
    while (charsTop + str.length > chars.length) {
      growChars()
    }

    var i = 0
    while (i < str.length) {
      chars(charsTop+i) = str.charAt(i)
      i += 1
    }

    val pos = charsTop
    charsTop += str.length
    pos
  }

  protected def growChars(): Unit = {
    val newChars = new Array[Char](chars.length * 2)
    System.arraycopy(chars, 0, newChars, 0, chars.length)
    chars = newChars
  }

}
