package atrox.sketch

object IndexResultBuilder {
  def make(distinct: Boolean, maxResults: Int): IndexResultBuilder = 
    (distinct, maxResults) match {
      case (_, 1)            => new Top1IndexResultBuilder
      case (d, Int.MaxValue) => new AllIndexResultBuilder(d)
      case (d, mr)           => new TopKIndexResultBuilder(mr, d)
    }
}

trait IndexResultBuilder {
  def += (idx: Int, sim: Double): Unit
  def ++= (rb: IndexResultBuilder): Unit
  def result: Array[Int]
}

class AllIndexResultBuilder(distinct: Boolean) extends IndexResultBuilder {
  private val res = new collection.mutable.ArrayBuilder.ofInt
  def += (idx: Int, sim: Double): Unit = res += idx
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: AllIndexResultBuilder =>
      res ++= rb.res.result
    case _ => ???
  }
  def result = if (!distinct) res.result else res.result.distinct
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
}

class Top1IndexResultBuilder extends IndexResultBuilder {
  private var _idx = -1
  private var _sim   = Float.NegativeInfinity
  def += (idx: Int, sim: Double): Unit = {
    if (sim > _sim) {
      _idx = idx
      _sim = sim.toFloat
    }
  }
  def ++= (rb: IndexResultBuilder): Unit = rb match {
    case rb: Top1IndexResultBuilder =>
      this += (rb._idx, rb._sim)
    case _ => ???
  }
  def result: Array[Int] = if (_sim == Float.NegativeInfinity) Array() else Array(_idx)
}
