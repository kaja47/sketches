package atrox.sketch

object IndexResultBuilder {
  def make(distinct: Boolean, maxResults: Int): IndexResultBuilder = 
    (distinct, maxResults) match {
      case (_, 1)                => new Top1IndexResultBuilder
      case (true, Int.MaxValue)  => new DistinctIndexResultBuilder
      case (false, Int.MaxValue) => new AllIndexResultBuilder
      case (distinct, mr)        => new TopKIndexResultBuilder(mr, distinct)
    }
}

trait IndexResultBuilder {
  def += (idx: Int, sim: Double): Unit
  def ++= (rb: IndexResultBuilder): Unit
  def result: Array[Int]
}

class AllIndexResultBuilder extends IndexResultBuilder {
  val res = new collection.mutable.ArrayBuilder.ofInt
  def += (idx: Int, sim: Double): Unit = res += idx
  def ++= (rb: IndexResultBuilder): Unit = ???
  def result = res.result
}

class TopKIndexResultBuilder(k: Int, distinct: Boolean) extends IndexResultBuilder {
  var res: atrox.TopKFloatInt = null 
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

class DistinctIndexResultBuilder extends IndexResultBuilder {
  val res = new collection.mutable.ArrayBuilder.ofInt
  def += (idx: Int, sim: Double): Unit = res += idx
  def ++= (rb: IndexResultBuilder): Unit = ???
  def result = res.result.distinct
}

class Top1IndexResultBuilder extends IndexResultBuilder {
  var _idx = -1
  var _sim   = Float.NegativeInfinity
  def += (idx: Int, sim: Double): Unit = {
    if (sim > _sim) {
      _idx = idx
      _sim = sim.toFloat
    }
  }
  def ++= (rb: IndexResultBuilder): Unit = ???
  def result: Array[Int] = if (_sim == Float.NegativeInfinity) Array() else Array(_idx)
}
