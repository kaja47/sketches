package atrox.sketch

import scala.collection.mutable
import atrox.RadixSort
import atrox.Bits
import java.util.Arrays

abstract class Grouping(keyRange: Int) {
  def add(k: Int, v: Int): Unit
  def getAll: Iterator[(Int, Array[Int])]
  def toArray: Array[Array[Int]]

  protected def mkIterator = Iterator.range(0, keyRange)
}

object Grouping {

  def apply(keyRange: Int, numberOfValues: Int = Int.MaxValue, counts: Array[Int] = null) =
    if (numberOfValues < Int.MaxValue && numberOfValues > 0) {
      new Sorted(keyRange, numberOfValues)
    } else if (counts != null) {
      new Counted(keyRange, counts)
    } else {
      new Buffered(keyRange)
    }


  class Buffered(keyRange: Int) extends Grouping(keyRange) {
    val arr = new Array[mutable.ArrayBuilder.ofInt](keyRange)
    def add(k: Int, v: Int): Unit = {
      if (arr(k) == null) {
        arr(k) = new mutable.ArrayBuilder.ofInt
      }
      arr(k) += v
    }

    def getAll = mkIterator collect { case i if arr(i) != null => (i, arr(i).result) }
    def toArray = arr map (x => if (x != null) x.result else null)
  }


  class Counted(keyRange: Int, counts: Array[Int]) extends Grouping(keyRange) {
    private val positions = new Array[Int](keyRange)
    private val arr = new Array[Array[Int]](keyRange)
    for (i <- 0 until keyRange) { if (counts(i) > 0) arr(i) = new Array[Int](counts(i)) }
    
    def add(k: Int, v: Int): Unit = {
      arr(k)(positions(k)) = v
      positions(k) += 1
    }

    def getAll = mkIterator collect { case i if arr(i) != null => (i, arr(i)) }
    def toArray = arr
  }


  class Sorted(keyRange: Int, numberOfValues: Int) extends Grouping(keyRange) {
    var head = 0
    val arr  = new Array[Long](numberOfValues)

    def add(k: Int, v: Int): Unit = {
      arr(head) = Bits.pack(k, v)
      head += 1
    }

    private def getKey(l: Long) = Bits.unpackIntHi(l)
    private def getVal(l: Long) = Bits.unpackIntLo(l)

    def getAll = {
      while (head < numberOfValues) { arr(head) = Int.MaxValue.toLong << 32 ; head += 1 }
      val scratch = new Array[Long](numberOfValues)
      val (sorted, _) = RadixSort.sort(arr, scratch, 0, numberOfValues, 4, 8, false)

      var i = 0
      mkIterator map { key =>
        while (i < numberOfValues && getKey(sorted(i)) < key) { i += 1 }
        var start = i

        while (i < numberOfValues && getKey(sorted(i)) == key) { i += 1 }
        val end = i

        if (start == end) {
          null

        } else {
          val res = new Array[Int](end - start)
          var j = 0
          while (start < end) {
            res(j) = getVal(sorted(start))
            start += 1
            j += 1
          }

          (key, res)
        }
      } filter (_ != null )
    }


    def toArray = ???
  }


  class Mapped(keyRange: Int) extends Grouping(keyRange) {
    private val map = mutable.Map[Int, mutable.ArrayBuilder.ofInt]()
    def add(k: Int, v: Int): Unit = {
      map.getOrElseUpdate(k, new mutable.ArrayBuilder.ofInt) += v
    }
    def getAll = for ((i, a) <- map.iterator) yield (i, a.result)
    def toArray = (0 to map.keys.max) map { i => if (map.contains(i)) map(i).result else null } toArray
  }

}
