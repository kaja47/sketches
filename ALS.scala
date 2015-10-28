package atrox

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand
import java.util.Arrays


object ALS {
  def apply(R: CSCMatrix[Float], factors: Int, α: Float, λ: Float, iterations: Int) =
    new ALS_Full(R, factors, α, λ, iterations).run

  def apply(R: Array[Array[Int]], factors: Int, α: Float, λ: Float, iterations: Int) =
    new ALS_Unary(UnaryMatrix(R), factors, α, λ, iterations).run
}

/** Sparse matrix whose values are all ones. It's column major just like CSCMatrix */
case class UnaryMatrix(data: Array[Array[Int]]) { self =>
  val cols = data.length
  val rows = data.map(ks => if (ks.isEmpty) 0 else ks.max).max+1

  def t: UnaryMatrix = {
    val lengths = new Array[Int](rows)
    val positions = new Array[Int](rows)

    var r = 0
    while (r < data.length) {
      var c = 0
      while (c < data(r).length) {
        val col = data(r)(c)
        lengths(col) += 1
        c += 1
      }
      r += 1
    }

    val res = Array.tabulate(rows) { c => new Array[Int](lengths(c)) }

    r = 0
    while (r < data.length) {
      var c = 0
      while (c < data(r).length) {
        val col = data(r)(c)
        res(col)(positions(col)) = r
        positions(col) += 1
        c += 1
      }
      r += 1
    }

    new UnaryMatrix(res) {
      override val cols = self.rows
      override val rows = self.cols
    }
  }
}

/** ALS for dataset with only ones as values.
  * It needs little less memory and it might be marginally faster */
final class ALS_Unary(val R: UnaryMatrix, val factors: Int, val α: Float, val λ: Float, val iterations: Int) extends ALS[UnaryMatrix] {

  val rows = R.rows
  val cols = R.cols
  def transpose(R: UnaryMatrix) = R.t

  def sliceVec(m: UnaryMatrix, col: Int): SparseVector[Float] = {
    val index = m.data(col)
    val ones = new Array[Float](index.length)
    var i = 0; while (i < index.length) { ones(i) = 1.0f ; i += 1 }
    new SparseVector[Float](new breeze.collection.mutable.SparseArray[Float](index, ones, index.length, m.rows, 0.0f))
  }

  private val lotOfOnes: Array[Float] = Array.fill(math.max(rows, cols))(1.0f) 

  def copyAndPremultiply(Y: DenseMatrix[Float], R: UnaryMatrix, u: Int): (Array[Float], Int) = {
    val index = R.data(u)
    val activeSize = index.length
    val sqα = math.sqrt(α).toFloat

    val temp = new Array[Float](activeSize * factors)
    for (f <- 0 until factors) {
      var offset = 0
      while (offset < activeSize) {
        temp(offset + f * activeSize) = (Y.data(index(offset) + f * Y.majorStride) * sqα) // no need to multiply by √value it's 1 anyway
        offset += 1
      }
    }

    (temp, activeSize)
  }

}

final class ALS_Full(val R: CSCMatrix[Float], val factors: Int, val α: Float, val λ: Float, val iterations: Int) extends ALS[CSCMatrix[Float]] {


  def transpose(R: CSCMatrix[Float]): CSCMatrix[Float] = R.t
  val cols = R.cols
  val rows = R.rows

  def sliceVec(m: CSCMatrix[Float], col: Int): SparseVector[Float] = {
    val start = m.colPtrs(col)
    val end   = m.colPtrs(col+1)
    val data = Arrays.copyOfRange(m.data, start, end)
    val idxs = Arrays.copyOfRange(m.rowIndices, start, end)
    new SparseVector(idxs, data, m.rows)
  }

  def copyAndPremultiply(Y: DenseMatrix[Float], R: CSCMatrix[Float], u: Int): (Array[Float], Int) = {

    val ru = sliceVec(R, u)
    val activeSize = ru.activeSize
    val sqα = math.sqrt(α).toFloat


    var offset = 0
    val indexArr = ru.array.index
    val sqDataArr = new Array[Float](activeSize)
    while (offset < activeSize) {
      sqDataArr(offset) = math.sqrt(ru.valueAt(offset)).toFloat
      offset += 1
    }

    // copy sparse data for better cache locality and pre-multiply them by √α and √cu
    val temp = new Array[Float](activeSize * factors)
    for (f <- 0 until factors) {
      var offset = 0
      while (offset < activeSize) {
        //val index = ru.indexAt(offset)
        //val value = ru.valueAt(offset)
        //val sqValue = math.sqrt(value).toFloat
        val index   = indexArr(offset)
        val sqValue = sqDataArr(offset)

        temp(offset + f * activeSize) = (Y.data(index + f * Y.majorStride) * sqα * sqValue)
        offset += 1
      }
    }

    (temp, ru.activeSize)
  }

}



abstract class ALS[Dataset] {

  def rep[T](f: => T) = { println('rep) ; Iterator.continually(f).drop(1000000000).toSeq.head }

  def R: Dataset
  def factors: Int
  def α: Float
  def λ: Float
  def iterations: Int

  def transpose(R: Dataset): Dataset
  def cols: Int
  def rows: Int
  def sliceVec(m: Dataset, col: Int): SparseVector[Float]

  /** Returns relavant portion of matrix Y, premultiplied by √α and √cu.
    * Yt * (Cu - I) * Y  can be compute by multiplication of resulting matrix with it's own transpose. */
  def copyAndPremultiply(Y: DenseMatrix[Float], R: Dataset, u: Int): (Array[Float], Int)

  // R  - user-item (users' ratings are in columns)
  // Rt - item-user
  def run = {
    print(s"users: $cols\nitems: $rows\n")
    val Rt = transpose(R)

    val X = DenseMatrix.ones[Float](rows = cols, cols = factors) // user factors
    val Y = DenseMatrix.ones[Float](rows = rows, cols = factors) // item factors

//    println((X.rows, X.cols))
//    println((Y.rows, Y.cols))
//    println((R.asInstanceOf[{def rows: Int}]rows, R.asInstanceOf[{def cols: Int}].cols))
//    println((Rt.asInstanceOf[{def rows: Int}].rows, Rt.asInstanceOf[{def cols: Int}].cols))

    for (iter <- 0 until iterations) {
      fix(R,  X, Y, s"$iter - fit X")
      fix(Rt, Y, X, s"$iter - fit Y")
    }

    (X,Y)
  }

  def CuPu(Ru: SparseVector[Float]): Ru.type = {
    var offset = 0
    while (offset < Ru.activeSize) {
      Ru.data(offset) = Ru.data(offset) * α + 1.0f
      offset += 1
    }
    Ru
  }

  def fix(R: Dataset, X: DenseMatrix[Float], Y: DenseMatrix[Float], stage: String) = {
    println(stage)

    val s = System.nanoTime
    val YtY = Y.t * Y
    println("YtY "+(System.nanoTime-s)+"ns")

    for (u <- 0 until X.rows par) {
      //val Ru = sliceVec(R, u)
      //val YtCuPu = Y.t * Ru.mapActiveValues(v => 1 + v * α)

      val YtCuPu = Y.t * CuPu(sliceVec(R, u))

      val m = mult(Y, R, u)
      m :+= YtY
      X(u, ::) := (invInPlace(m) * YtCuPu).t
    }
  }

  def invInPlace(m: DenseMatrix[Float]): m.type = {
    val invM = inv(dfm2ddm(m))

    val src  = invM.data
    val dest = m.data

    require(
      m.rows == invM.rows &&
      m.cols == invM.cols &&
      m.offset == invM.offset &&
      m.majorStride == invM.majorStride &&
      m.isTranspose == invM.isTranspose
    )

    var i = 0
    while (i < src.length) {
      dest(i) = src(i).toFloat
      i += 1
    }

    m
  }


  def ddm2dfm(ddm: DenseMatrix[Double]): DenseMatrix[Float] = {
    val src: Array[Double] = ddm.data
    val arr = new Array[Float](src.length)
    var i = 0
    while (i < src.length) {
      arr(i) = src(i).toFloat
      i += 1
    }

    new DenseMatrix[Float](ddm.rows, ddm.cols, arr, ddm.offset, ddm.majorStride, ddm.isTranspose)
  }

  def dfm2ddm(ddm: DenseMatrix[Float]): DenseMatrix[Double] = {
    val src: Array[Float] = ddm.data
    val arr = new Array[Double](src.length)
    var i = 0
    while (i < src.length) {
      arr(i) = src(i).toDouble
      i += 1
    }

    new DenseMatrix[Double](ddm.rows, ddm.cols, arr, ddm.offset, ddm.majorStride, ddm.isTranspose)
  }



  def mult(Y: DenseMatrix[Float], R: Dataset, u: Int): DenseMatrix[Float] = {
    // Yt * Y + ...
    val res = DenseMatrix.zeros[Float](factors, factors)

    // ... + Yt * (Cu - I) * Y + ...
    val (temp, activeSize) = copyAndPremultiply(Y, R, u)

    var i = 0
    while (i < factors) {
      var j = 0
      while (j < factors) {
        var prod = 0.0f
        var offset = 0
        while (offset < activeSize) {
          //prod += Y(index, i) * value * α * Y(index, j)
          prod += temp(offset + i * activeSize) * temp(offset + j * activeSize)

          offset += 1
        }
        //res(i, j) = prod
        res.unsafeUpdate(i, j, prod)
        j += 1
      }
      i += 1
    }

    // ... + λI
    i = 0
    while (i < factors) {
      res(i, i) += λ
      i += 1
    }

    res
  }


}
