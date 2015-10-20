package atrox.sketch

import atrox.Cursor2
import atrox.Bits

object IndexResultBuilder {
  def make(distinct: Boolean, maxResults: Int): IndexResultBuilder = 
    (distinct, maxResults) match {
      case (d, Int.MaxValue) => new AllIndexResultBuilder(d)
      case (d, mr)           => new TopKIndexResultBuilder(mr, d)
    }
}

trait IndexResultBuilder {
  def += (idx: Int, sim: Double): Unit
  def ++= (rb: IndexResultBuilder): Unit
  def result: Array[Int]
  def cursor: Cursor2[Int, Float]
}

class AllIndexResultBuilder(distinct: Boolean) extends IndexResultBuilder {
  private val res = new collection.mutable.ArrayBuilder.ofLong
  def += (idx: Int, sim: Double): Unit = res += Bits.pack(hi = idx, lo = sim.toFloat)
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: AllIndexResultBuilder =>
      res ++= rb.res.result
    case _ => ???
  }
  def result = if (distinct) ??? else res.result.map(x => Bits.unpackIntHi(x))
  def cursor = if (distinct) ??? else new Cursor2[Int, Float] {
    private val arr = res.result
    private var pos = -1

    def moveNext() = {
      if (pos < arr.length) pos += 1
      pos < arr.length
    }
    def key   = Bits.unpackIntHi(arr(pos))
    def value = Bits.unpackFloatLo(arr(pos))
  }
}

class TopKIndexResultBuilder(k: Int, distinct: Boolean) extends IndexResultBuilder {
  private var res: atrox.TopKFloatInt = null // top-k is allocated only when it's needed
  private def createTopK() = if (res == null) res = new atrox.TopKFloatInt(k, distinct)
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
    case _ => ???
  }
  def result = if (res == null) Array() else res.drainToArray()
  def cursor = { createTopK() ; res.cursor.swap }
}
