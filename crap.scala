package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector }

object crap {

  def sum(xs: Seq[SparseVector[Double]]): DenseVector[Double] = {
    val s = DenseVector.zeros[Double](xs.head.size)
    for (x <- xs) {
      s += x
    }
    s
  }

  def df(xs: Seq[SparseVector[Double]]): DenseVector[Double] = {
    val s = DenseVector.zeros[Double](xs.head.size)
    for (vec <- xs) {
      var offset = 0
      while (offset < vec.activeSize) {
        val i = vec.indexAt(offset)
        s(i) += 1
        offset += 1
      }
    }
    s
  }

  def tfBoolean(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
    for (vec <- fs) yield vec mapActiveValues { _ => 1.0 }

  def tfLog(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
    for (vec <- fs) yield vec mapActiveValues { f => 1.0 + math.log(f) }

  def tfAugmented(fs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
    for (vec <- fs) yield {
      val m = breeze.linalg.max(vec)
      vec mapActiveValues { f => 0.5 + (0.5 * f) / m }
    }


  def tfidf(tfs: Seq[SparseVector[Double]]): Seq[SparseVector[Double]] =
    tfidf(tfs, df(tfs))


  def tfidf(tfs: Seq[SparseVector[Double]], df: DenseVector[Double]): Seq[SparseVector[Double]] = {
    val N = tfs.size
    for (vec <- tfs) yield {
      vec mapActivePairs { case (idx, tf) => tf * math.log(N / df(idx))  }
    }
  }

}



class Xorshift(var x: Int = System.currentTimeMillis.toInt, var y: Int = 4711, var z: Int = 5485612, var w: Int = 992121) {
  def nextInt(): Int = {
    val t = x ^ (x << 11)
    x = y
    y = z
    z = w
    w = w ^ (w >>> 19) ^ t ^ (t >>> 8)
    w
  }
}
