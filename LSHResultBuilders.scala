package atrox.sketch

import atrox.Cursor2
import atrox.Bits
import atrox.{ TopKIntInt, TopKIntIntEstimate/*, BruteForceTopKIntInt*/ }


object IndexResultBuilder {
  def make(distinct: Boolean, maxResults: Int): IndexResultBuilder =
    if (maxResults == 1) {
      new SingularIndexResultBuilder
    } else if (maxResults == Int.MaxValue && !distinct) {
      new AllIndexResultBuilder
    } else if (maxResults == Int.MaxValue && distinct) {
      new AllDistinctIndexResultBuilder
    } else {
      new TopKIndexResultBuilder(maxResults, distinct)
    }
}

trait IndexResultBuilder {
  def size: Int
  def += (idx: Int, score: Int): Unit
  def ++= (rb: IndexResultBuilder): Unit
  def result: Array[Int]
  def idxScoreCursor: Cursor2[Int, Int]
}


class SingularIndexResultBuilder extends IndexResultBuilder {
  private var empty = true
  private var idx, score = 0

  def size: Int = if (empty) 0 else 1
  def += (idx: Int, score: Int): Unit = {
    if (empty || score > this.score) {
      this.idx = idx
      this.score = score
      this.empty = false
    }
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: SingularIndexResultBuilder =>
      if (!rb.empty) this += (rb.idx, rb.score)
    case _ => sys.error("this should never happen")
  }
  def result: Array[Int] = if (empty) Array.empty[Int] else Array(idx)
  def idxScoreCursor: Cursor2[Int, Int] = new Cursor2[Int, Int] {
    var valid = false
    def moveNext() = { valid = !valid ; valid && !empty }
    def key = idx
    def value = score
  }

}



class AllIndexResultBuilder extends IndexResultBuilder {
  private val res = new collection.mutable.ArrayBuilder.ofLong
  private var _size = 0

  def size = _size

  def += (idx: Int, score: Int): Unit = {
    res += Bits.pack(hi = idx, lo = score)
    _size += 1
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: AllIndexResultBuilder =>
      val xs = rb.res.result
      res ++= xs
      _size += xs.length
    case _ => sys.error("this should never happen")
  }

  def result = {
    val arr = res.result
    arr.sortBy(~_).map(x => Bits.unpackIntHi(x))
  }
  def idxScoreCursor = new Cursor2[Int, Int] {
    private val arr = res.result
    private var pos = -1

    def moveNext() = {
      if (pos < arr.length) pos += 1
      pos < arr.length
    }
    def key   = Bits.unpackIntHi(arr(pos))
    def value = Bits.unpackIntLo(arr(pos))
  }
}

/** Inefficient version just for completeness sake. */
class AllDistinctIndexResultBuilder extends IndexResultBuilder {
  private val set = collection.mutable.Set[(Int, Int)]()
  def size = set.size
  def += (idx: Int, score: Int): Unit = set += ((idx, score))
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: AllDistinctIndexResultBuilder => this.set ++= rb.set
    case _ => sys.error("this should never happen")
  }
  def result = set.map(_._1).toArray.sortBy(~_)

  def idxScoreCursor = new Cursor2[Int, Int] {
    var key, value = 0

    def moveNext() = {
      if (set.isEmpty) false else {
        val h @ (kk, vv) = set.head
        key = kk
        value = vv
        set.remove(h)
        true
      }
    }
  }
}


class TopKIndexResultBuilder(k: Int, distinct: Boolean) extends IndexResultBuilder {
  private var res: TopKIntInt = null // top-k is allocated only when it's needed
  private def createTopK() = if (res == null) res = new TopKIntInt(k, distinct)

  def size = if (res == null) 0 else res.size

  def += (idx: Int, score: Int): Unit = {
    createTopK()
    res.add(score, idx)
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: TopKIndexResultBuilder =>
      if (rb.res != null) {
        createTopK()
        res addAll rb.res
      }
    case _ => sys.error("this should never happen")
  }

  def result = if (res == null) Array() else res.drainToArray()
  def idxScoreCursor = { createTopK() ; res.cursor.swap }
}

class TopKEstimateIndexResultBuilder(k: Int) extends IndexResultBuilder {
  private var res: TopKIntIntEstimate = null // top-k is allocated only when it's needed
  private def createTopK() = if (res == null) res = ??? // new TopKIntIntEstimate(k, 3)

  def size = if (res == null) 0 else res.size

  def += (idx: Int, score: Int): Unit = {
    createTopK()
    res.add(score, idx)
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: TopKEstimateIndexResultBuilder =>
      if (rb.res != null) {
        createTopK()
        res addAll rb.res
      }
    case _ => sys.error("this should never happen")
  }

  def result = if (res == null) Array() else res.drainToArray()
  def idxScoreCursor = { createTopK() ; res.cursor.swap }
}
