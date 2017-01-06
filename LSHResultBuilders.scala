package atrox.sketch

import atrox.Cursor2
import atrox.Bits
import atrox.{ TopKIntInt, TopKIntIntEstimate/*, BruteForceTopKIntInt*/ }

object IndexResultBuilder {
  def make(distinct: Boolean, maxResults: Int): IndexResultBuilder =
    if (maxResults == Int.MaxValue) {
      new AllIndexResultBuilder(distinct)
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

class AllIndexResultBuilder(distinct: Boolean) extends IndexResultBuilder {
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
    val arr = if (distinct) res.result.distinct else res.result
    arr.map(x => Bits.unpackIntHi(x))
  }
  def idxScoreCursor = new Cursor2[Int, Int] {
    private val arr = if (distinct) res.result.distinct else res.result
    private var pos = -1

    def moveNext() = {
      if (pos < arr.length) pos += 1
      pos < arr.length
    }
    def key   = Bits.unpackIntHi(arr(pos))
    def value = Bits.unpackIntLo(arr(pos))
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
