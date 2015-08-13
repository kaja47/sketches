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
class StringIntDictionary(initialCapacity: Int = 1024, val loadFactor: Double = 0.45) {
  require(loadFactor > 0 && loadFactor < 1, "load factor must be in range from 0 to 1 (exclusive)")

  protected val segmentLength = 3

  protected var capacity = max(higherPowerOfTwo(initialCapacity), 16)
  protected var occupied = 0
  protected var maxOccupied = min(capacity * loadFactor toInt, capacity - 1)
  protected var charsTop = 0

  protected var assoc = new Array[Int](capacity * segmentLength)
  protected var chars = new Array[Char](capacity * 8)

  protected val defaultValue = 0

  // debug information
  var _puts = 0L
  var _gets = 0L
  var _probes = 0L
  var _growProbes = 0L
  def _byteSize = assoc.length * 4 + chars.length * 2
  def _capacity = capacity

  private def higherPowerOfTwo(x: Int) =
    highestOneBit(x) << (if (highestOneBit(x) == x) 0 else 1)


  def put(str: CharSequence, value: Int): Unit =
    putInternal(str, value, true)

  def putIfAbsent(str: CharSequence, value: Int): Unit =
    putInternal(str, value, false)


  def get(str: CharSequence) = getOrDefault(str, defaultValue)

  def getOrDefault(str: CharSequence, defaultValue: Int): Int = {
    _gets += 1

    val pos = findPos(str, tryInline(str))
    if (stringLength(assoc, pos) > 0) payload(assoc, pos) else defaultValue
  }

  def getOrElseUpdate(str: CharSequence, value: Int): Int =
    putInternal(str, value, false)

  def contains(str: CharSequence): Boolean =
    stringLength(assoc, findPos(str, tryInline(str))) > 0

  def size = occupied

  def iterator: Iterator[(String, Int)] = Iterator.tabulate(capacity) { i =>
    val pos = i * segmentLength
    if (stringLength(assoc, pos) == 0) null
    else (makeString(assoc, pos), payload(assoc, pos))
  } filter (_ != null)



  protected def putInternal(str: CharSequence, value: Int, overwrite: Boolean): Int = {
    _puts += 1
    val word = tryInline(str)

    var pos = findPos(str, word)
    if (stringLength(assoc, pos) > 0) { // key `str` is already present
      if (overwrite) {
        setPayload(assoc, pos, value)
        value
      } else {
        payload(assoc, pos)
      }
    } else { // empty slot
      if (word != 0xffffffffffffffffL) {
        setInlinedWord(assoc, pos, word)
        setInlined(assoc, pos)
        setStringLength(assoc, pos, str.length)

      } else {
        val offset = putString(str)
        setStringOffset(assoc, pos, offset)
        setStringLength(assoc, pos, str.length)
      }
      setPayload(assoc, pos, value)
      occupied += 1

      // grow must be called after insertion of the new element
      // otherwise the `pos` returned by findPos is no longer valid
      if (occupied > maxOccupied) grow()

      value
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
    var h = 0
    var i = 0
    while (i < str.length) {
      h = 31 * h + str.charAt(i)
      i += 1
    }
    h
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

  protected def inlinedHashCode(inlinedStr: Long, length: Int): Int = {
    // YOLO hashing scheme: it's faster but it produces more collisions on corpus of english words
    //inlinedStr.toInt ^ (inlinedStr >> 32).toInt

    // poor man's universal hashing
    reverse(inlinedStr * 3542462394158182007L + 1775710242L).toInt

    // slow and boring way
    //var h = 0
    //var i = 0
    //while (i < length) {
    //  h = 31 * h + decode(inlinedChar(assoc, inlinedStr, i))
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
    val len = stringLength(assoc, pos)
    val word = inlinedWord(assoc, pos)
    val arr = new Array[Char](len)
    var i = 0
    while (i < len) {
      arr(i) = decode(inlinedChar(assoc, word, i))
      i += 1
    }
    new String(arr)
  }

  protected def charsToString(assoc: Array[Int], pos: Int) = {
    val len = stringLength(assoc, pos)
    val off = stringOffset(assoc, pos)
    new String(chars, off, len)
  }

  protected def findPos(str: CharSequence, inlined: Long) = {
    require(str.length != 0 && str.length < 256, "string must be non empty and shorter than 256 chars")

    val mask = capacity - 1

    if (inlined != 0xffffffffffffffffL) {
      val hash = inlinedHashCode(inlined, str.length)
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

  // pos refers to the first element in segment
  protected def stringLength(assoc: Array[Int], pos: Int) = assoc(pos+1) & 0xff
  protected def stringOffset(assoc: Array[Int], pos: Int) = assoc(pos)
  protected def inlinedWord(assoc: Array[Int], pos: Int): Long = (assoc(pos).toLong & 0xffffffffL) | ((assoc(pos+1).toLong & 0x3fffff00L) << 24)

  protected def inlinedChar(assoc: Array[Int], word: Long, i: Int): Int = ((word >>> (6 * i)) & ((1 << 6) - 1)).toInt
  protected def payload(assoc: Array[Int], pos: Int) = assoc(pos+2)
  protected def isInlined(assoc: Array[Int], pos: Int) = (assoc(pos+1) & (1 << 30)) != 0

  protected def setStringLength(assoc: Array[Int], pos: Int, length: Int) = assoc(pos+1) = (assoc(pos+1) & 0xffffff00) | length
  protected def setStringOffset(assoc: Array[Int], pos: Int, offset: Int) = assoc(pos) = offset
  protected def setInlinedWord(assoc: Array[Int], pos: Int, word: Long) = {
    assoc(pos) = (word & 0xffffffff).toInt
    assoc(pos+1) = (word >>> 24).toInt
  }
  private def setInlinedChar(word: Long, i: Int, value: Int): Long = {
    //require(value < 64)
    word | (value.toLong << (6 * i))
  }
  protected def setPayload(assoc: Array[Int], pos: Int, value: Int) = assoc(pos+2) = value
  protected def setInlined(assoc: Array[Int], pos: Int) = assoc(pos+1) |= (1 << 30)


  protected def equalOrEmptyInlined(pos: Int, str: CharSequence, inlinedStr: Long): Boolean = {
    val len = stringLength(assoc, pos)
    if (len == 0) return true
    if (!isInlined(assoc, pos)) return false
    if (len != str.length) return false
    inlinedWord(assoc, pos) == inlinedStr
  }

  protected def equalOrEmptyNotInlined(pos: Int, str: CharSequence): Boolean = {
    val len = stringLength(assoc, pos)
    if (len == 0) return true
    if (isInlined(assoc, pos)) return false
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
      //val ok = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
      //if (!ok) return 0xffffffffffffffffL
      //word = setInlinedChar(word, i, encode(ch))
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
    i = 0

    ch = '0' ; while (ch <= '9') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }
    ch = 'A' ; while (ch <= 'Z') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }
    ch = 'a' ; while (ch <= 'z') { arr(ch) = i.toByte ; ch += 1 ; i += 1 }

    arr
  }

  protected def encode(ch: Char): Int = encodeTable(ch).toInt

  protected def decode(x: Int): Char = {
    if (x < 10) return (x      + '0').toChar
    if (x < 36) return (x - 10 + 'A').toChar
    /*if (x <= 62)*/ return (x - 36 + 'a').toChar
  }


  protected def grow() = {
    val oldAssoc = assoc
    val oldCapacity = capacity
    this.assoc = new Array[Int](assoc.length * 2)
    this.capacity = capacity * 2
    this.maxOccupied = capacity * loadFactor toInt

    var oldPos = 0
    while (oldPos < (oldCapacity * segmentLength)) {
      val len = stringLength(oldAssoc, oldPos)
      if (len > 0) {
        val hash = if (isInlined(oldAssoc, oldPos)) {
          inlinedHashCode(inlinedWord(oldAssoc, oldPos), len)
        } else {
          charArrHashCode(chars, stringOffset(oldAssoc, oldPos), len)
        }
        val mask = capacity - 1
        var i = hash & mask
        var pos = i * segmentLength
        while (stringLength(assoc, pos) != 0) { // search for space in newly allocated array
          i = (i + 1) & mask
          pos = i * segmentLength
          _growProbes += 1
        }

        assoc(pos)   = oldAssoc(oldPos)
        assoc(pos+1) = oldAssoc(oldPos+1)
        assoc(pos+2) = oldAssoc(oldPos+2)
      }
      oldPos += segmentLength
    }

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
