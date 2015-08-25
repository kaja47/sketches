package atrox

import breeze.linalg.{ SparseVector, DenseVector, BitVector }


/** Sparse-sparse vector dot product that is much simpler than standard one from breeze.
 *  In many cases can be much faster. Someimes even 2.5 times faster. It's
 *  caused by the fact that it contains lot less instruction and only one
 *  semi-unpredictable conditional jump in hot path.
 */
object fastDotProduct extends breeze.generic.UFunc.UImpl2[breeze.linalg.operators.OpMulInner.type, SparseVector[Double], SparseVector[Double], Double] {
  def apply(a: SparseVector[Double], b: SparseVector[Double]): Double = {
    require(a.size == b.size, "Vectors must be the same length!")

    val ak: Array[Int]    = a.array.index
    val av: Array[Double] = a.array.data

    val bk: Array[Int]    = b.array.index
    val bv: Array[Double] = b.array.data

    var prod = 0.0
    var ai, bi = 0
    while (ai != ak.length && bi != bk.length) {
      val a = ak(ai)
      val b = bk(bi)
      if (a == b) {
        prod += av(ai) * bv(bi)
      }

      // progress counter with smaller key
      ai += (if (a <= b) 1 else 0)
      bi += (if (a >= b) 1 else 0)
    }
    prod
  }
}


object fastSparse {

  /** arguments must be sorted arrays */
  def intersectionSize(a: Array[Int], b: Array[Int]): Int = {
    var size, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }
    size
  }

  /** This method tried to look ahead and skip some unnecessary iterations. In
    * some cases it can be faster than straightforward code, but it's rarely
    * slower. */
  def intersectionSizeWithSkips(a: Array[Int], b: Array[Int], skip: Int): Int = {
    var size, ai, bi = 0

    val alen = a.length - skip
    val blen = b.length - skip

    while (ai < alen && bi < blen) {
      val av = a(ai)
      val bv = b(bi)
      val _ai = ai
      val _bi = bi
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) (if (a(_ai+skip) < bv) skip else 1) else 0)
      bi   += (if (av >= bv) (if (b(_bi+skip) < av) skip else 1) else 0)
    }

    while (ai < a.length && bi < b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) 1 else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }

    size
  }

  def unionSize(a: Array[Int], b: Array[Int]): Int =
    a.length + b.length - intersectionSize(a, b)

  /** result = |a -- b| */
  def diffSize(a: Array[Int], b: Array[Int]): Int =
    a.length - intersectionSize(a, b)


  def intersectionAndUnionSize(a: Array[Int], b: Array[Int]): (Int, Int) = {
    val is = intersectionSize(a, b)
    (is, a.length + b.length - is)
  }

  def jaccardSimilarity(a: Array[Int], b: Array[Int]): Double = {
    val is = intersectionSize(a, b)
    val un = a.length + b.length - is
    is.toDouble / un
  }


  def union(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](unionSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      if (av == bv) {
        res(i) = av
        i += 1
        ai += 1
        bi += 1

      } else if (av < bv) {
        res(i) = av
        i += 1
        ai += 1

      } else {
        res(i) = bv
        i += 1
        bi += 1
      }
    }

    while (ai != a.length) {
      res(i) = a(ai)
      i += 1
      ai += 1
    }

    while (bi != b.length) {
      res(i) = b(bi)
      i += 1
      bi += 1
      }

      res
  }

  def intersection(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](intersectionSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)

      if (av == bv) {
        res(i) = av
        i += 1
      }

      ai += (if (av <= bv) 1 else 0)
      bi += (if (av >= bv) 1 else 0)
    }

    res
  }


  /** result = a -- b */
  def diff(a: Array[Int], b: Array[Int]): Array[Int] = {
    val res = new Array[Int](diffSize(a, b))
    var i, ai, bi = 0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      if (av == bv) {
        ai += 1
        bi += 1

      } else if (av < bv) {
        res(i) = av
        i += 1
        ai += 1

      } else {
        bi += 1
      }
    }

    while (ai != a.length) {
      res(i) = a(ai)
      i += 1
      ai += 1
    }

    res
  }


  def weightedIntersectionSize(a: Array[Int], b: Array[Int], ws: Array[Double]): Double = {
    var ai, bi = 0
    var size = 0.0
    while (ai != a.length && bi != b.length) {
      val av = a(ai)
      val bv = b(bi)
      size += (if (av == bv) ws(av) else 0)
      ai   += (if (av <= bv) 1 else 0)
      bi   += (if (av >= bv) 1 else 0)
    }
    size
  }

  private def _sum(a: Array[Int], ws: Array[Double]): Double = {
    var s = 0.0
    var i = 0
    while (i < a.length) {
      s += ws(a(i))
      i += 1
    }
    s
  }

  def weightedJaccardSimilarity(a: Array[Int], b: Array[Int], ws: Array[Double]): Double = {
    val is = weightedIntersectionSize(a, b, ws)
    val un = _sum(a, ws) + _sum(b, ws) - is
    is.toDouble / un
  }

}
