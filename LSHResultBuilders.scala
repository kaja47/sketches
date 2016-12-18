package atrox.sketch

import atrox.Cursor2
import atrox.Bits
import atrox.{ TopKFloatInt, TopKFloatIntEstimate, BruteForceTopKFloatInt }

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
  def += (idx: Int, sim: Double): Unit
  def ++= (rb: IndexResultBuilder): Unit
  def result: Array[Int]
  def idxSimCursor: Cursor2[Int, Float]
  def minLowerBound: Float
}

class AllIndexResultBuilder(distinct: Boolean) extends IndexResultBuilder {
  private val res = new collection.mutable.ArrayBuilder.ofLong
  private var _size = 0

  def size = _size

  def += (idx: Int, sim: Double): Unit = {
    res += Bits.pack(hi = idx, lo = sim.toFloat)
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
  def idxSimCursor = new Cursor2[Int, Float] {
    private val arr = if (distinct) res.result.distinct else res.result
    private var pos = -1

    def moveNext() = {
      if (pos < arr.length) pos += 1
      pos < arr.length
    }
    def key   = Bits.unpackIntHi(arr(pos))
    def value = Bits.unpackFloatLo(arr(pos))
  }
  def minLowerBound: Float = Float.MinValue
}

class TopKIndexResultBuilder(k: Int, distinct: Boolean) extends IndexResultBuilder {
  private var res: BruteForceTopKFloatInt = null // top-k is allocated only when it's needed
  private def createTopK() = if (res == null) res = new BruteForceTopKFloatInt(k)

  def size = if (res == null) 0 else res.size

  def += (idx: Int, sim: Double): Unit = {
    createTopK()
    res.insert(sim.toFloat, idx)
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: TopKIndexResultBuilder =>
      if (rb.res != null) {
        createTopK()
        res ++= rb.res
      }
    case _ => sys.error("this should never happen")
  }

  def result = if (res == null) Array() else res.drainToArray()
  def idxSimCursor = { createTopK() ; res.cursor.swap }
  def minLowerBound: Float = if (res == null || res.size != k) Float.MinValue else res.minKey
}

class TopKEstimateIndexResultBuilder(k: Int) extends IndexResultBuilder {
  private var res: TopKFloatIntEstimate = null // top-k is allocated only when it's needed
  private def createTopK() = if (res == null) res = ??? // new TopKFloatIntEstimate(k, 3)

  def size = if (res == null) 0 else res.size

  def += (idx: Int, sim: Double): Unit = {
    createTopK()
    res.insert(sim.toFloat, idx)
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: TopKEstimateIndexResultBuilder =>
      if (rb.res != null) {
        createTopK()
        res ++= rb.res
      }
    case _ => sys.error("this should never happen")
  }

  def result = if (res == null) Array() else res.drainToArray()
  def idxSimCursor = { createTopK() ; res.cursor.swap }
  def minLowerBound: Float = if (res == null) Float.MinValue else res.keyThreshold
}
