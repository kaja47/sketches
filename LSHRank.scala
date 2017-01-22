package atrox.sketch

import atrox.Bits


/** Trait implementing querying into LSH tables. */
trait Query[-Q, SketchArray] {
  def query(q: Q): (SketchArray, Int)
  def query(idx: Int): (SketchArray, Int)
}

/** Sketching can be fully materialized Sketch table or dataset wrapped in
  * Sketching class */
case class SketchingQuery[Q, SketchArray](sk: Sketching[Q, SketchArray]) extends Query[Q, SketchArray] {
  def query(q: Q) = (sk.sketchers.getSketchFragment(q), 0)
  def query(idx: Int) = (sk.getSketchFragment(idx), 0)

  // TODO Is this specialization needed? Does it have any speed benefit? If
  // not, it might not be nesessary to produce pair (skarr, skidx) but only
  // allocated skarr value.
  //def query(idx: Int) = (sketchTable.sketchArray, idx)
}


trait Rank[-Q, S] {
  def map(q: Q): S
  def map(idx: Int): S

  /** Similarity/distance function. Result must be mapped to integer such as
    * higher number means bigger similarity or smaller distance. */
  def rank(a: S, b: S): Int

  def rank(a: S, b: Int): Int = rank(a, map(b))

  /** Index based rank. It's intended for bulk methods that operate only on
    * internal tables. In that case it should be overwritten to use indexes
    * into Sketch object without any extra allocations. */
  def rank(a: Int, b: Int): Int = rank(map(a), map(b))

  def rank(r: Double): Int

  /** Recover similarity/distance encoded in integer rank value. */
  def derank(r: Int): Double
}


trait SimRank[@specialized(Long) S] extends Rank[S, S] {
  def apply(a: S, b: S): Double

  def rank(a: S, b: S): Int = rank(apply(a, b))
  def rank(d: Double): Int = Bits.floatToSortableInt(d.toFloat)
  def derank(r: Int): Double = Bits.sortableIntToFloat(r)
}

case class SimFun[S](f: (S, S) => Double, dataset: IndexedSeq[S]) extends SimRank[S] {
  def map(q: S): S = q
  def map(idx: Int): S = dataset(idx)

  def apply(a: S, b: S) = f(a, b)
}


trait DistRank[@specialized(Long) S] extends Rank[S, S] {
  def apply(a: S, b: S): Double

  def rank(a: S, b: S): Int = rank(apply(a, b))
  def rank(d: Double): Int = ~Bits.floatToSortableInt(d.toFloat)
  def derank(r: Int): Double = Bits.sortableIntToFloat(~r)
}

case class DistFun[@specialized(Long) S](f: (S, S) => Double, dataset: IndexedSeq[S]) extends DistRank[S] {
  def map(q: S): S = q
  def map(idx: Int): S = dataset(idx)

  def apply(a: S, b: S) = f(a, b)
}

case class SketchRank[Q, SketchArray](sk: Sketch[Q, SketchArray]) extends Rank[Q, (SketchArray, Int)] {

  type S = (SketchArray, Int)
  def es = sk.estimator

  def map(q: Q): S = (sk.sketchers.getSketchFragment(q), 0)
  def map(idx: Int): S = (sk.sketchArray, idx)

  def rank(a: S, b: S): Int = {
    val (skarra, idxa) = a
    val (skarrb, idxb) = b
    es.sameBits(skarra, idxa, skarrb, idxb)
  }
  override def rank(a: S, b: Int): Int = {
    val (skarra, idxa) = a
    es.sameBits(skarra, idxa, sk.sketchArray, b)
  }
  override def rank(a: Int, b: Int): Int = es.sameBits(sk.sketchArray, a, sk.sketchArray, b)

  def rank(d: Double): Int = d.toInt

  /** Recover similarity/distance encoded in integer rank value. */
  def derank(r: Int): Double = es.estimateSimilarity(r)
}


case class InlineSketchRank[Q, SketchArray](sketch: Sketch[Q, SketchArray], sketchers: Sketchers[Q, SketchArray]) extends Rank[Q, SketchArray] {

  type S = SketchArray
  def es = sketch.estimator

  def map(q: Q): S = sketchers.getSketchFragment(q)
  def map(idx: Int): S = sketch.getSketchFragment(idx)

  def rank(a: S, b: S): Int = es.sameBits(a, 0, b, 0)
  override def rank(a: S, b: Int): Int = es.sameBits(a, 0, sketch.sketchArray, b)
  override def rank(a: Int, b: Int): Int = es.sameBits(sketch.sketchArray, a, sketch.sketchArray, b)

  def rank(d: Double): Int = d.toInt

  /** Recover similarity/distance encoded in integer rank value. */
  def derank(r: Int): Double = es.estimateSimilarity(r)
}
