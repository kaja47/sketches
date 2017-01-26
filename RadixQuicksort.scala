package atrox.sort

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import atrox.Bits

// Three-way radix quicksort aka Multi-key quicksort
//
// Fast Algorithms for Sorting and Searching Strings
// http://www.cs.princeton.edu/~rs/strings/paper.pdf


abstract class RadixQuicksortElement[S] {
  def charAt(s: S, pos: Int): Int
}

object RadixQuicksortElement {
  implicit val String: RadixQuicksortElement[String] = new RadixQuicksortElement[String] {
    def charAt(str: String, pos: Int): Int = if (pos < str.length) str.charAt(pos) else -1
  }

  implicit val UnsignedByteArray: RadixQuicksortElement[Array[Byte]] = new RadixQuicksortElement[Array[Byte]] {
    def charAt(str: Array[Byte], pos: Int): Int = if (pos < str.length) str(pos) & 0xff else -1
  }

  val SignedByteArray: RadixQuicksortElement[Array[Byte]] = new RadixQuicksortElement[Array[Byte]] {
    def charAt(str: Array[Byte], pos: Int): Int = if (pos < str.length) (str(pos)+0x80) & 0xff else -1
  }

  implicit val IntElement: RadixQuicksortElement[Int] = new RadixQuicksortElement[Int] {
    def charAt(str: Int, pos: Int): Int = if (pos >= 4) -1 else ((str+0x80000000) >>> ((3-pos) * 8)) & 0xff
  }

  implicit val LongElement: RadixQuicksortElement[Long] = new RadixQuicksortElement[Long] {
    def charAt(str: Long, pos: Int): Int = if (pos >= 8) -1 else (((str+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
  }

  implicit val FloatElement: RadixQuicksortElement[Float] = new RadixQuicksortElement[Float] {
    def charAt(str: Float, pos: Int): Int = if (pos >= 4) -1 else ((Bits.floatToSortableInt(str)+0x80000000) >>> ((3-pos) * 8)) & 0xff
  }

  implicit val DoubleElement: RadixQuicksortElement[Double] = new RadixQuicksortElement[Double] {
    def charAt(str: Double, pos: Int): Int = if (pos >= 8) -1 else (((Bits.doubleToSortableLong(str)+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
  }

  def Mapped[T, S](f: T => S)(implicit el: RadixQuicksortElement[S]) = new RadixQuicksortElement[T] {
    def charAt(str: T, pos: Int): Int = el.charAt(f(str), pos)
  }

  implicit def Option[T](implicit el: RadixQuicksortElement[T]) = new RadixQuicksortElement[Option[T]] {
    def charAt(str: Option[T], pos: Int): Int =
      if (pos == 0) {
        if (str.isEmpty) -1 /*None*/ else 0 /*Some*/
      } else {
        el.charAt(str.get, pos-1)
      }
  }

  implicit def Either[A, B](implicit ela: RadixQuicksortElement[A], elb: RadixQuicksortElement[B]) = new RadixQuicksortElement[Either[A, B]] {
    def charAt(str: Either[A, B], pos: Int): Int =
      if (pos == 0) {
        if (str.isLeft) 1 else 2
      } else {
        str match {
          case Left(x)  => ela.charAt(x, pos-1)
          case Right(x) => elb.charAt(x, pos-1)
        }
      }
  }
}


object RadixQuicksort {


  def sort[S](arr: Array[S])(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, 0, arr.length, 0)(el)

  def sortBy[T, S](arr: Array[T], f: T => S)(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, 0, arr.length, 0)(RadixQuicksortElement.Mapped(f))

  def sortBySchwartzianTransform[T, S](arr: Array[T], f: T => S)(implicit el: RadixQuicksortElement[S]) = {
    val tmp = Array.tabulate[(T, S)](arr.length){ i => (arr(i), f(arr(i))) }
    sort0(tmp, 0, arr.length, 0)(RadixQuicksortElement.Mapped(_._2))
    for (i <- 0 until arr.length) {
      arr(i) = tmp(i)._1
    }
  }


  def sort[S](arr: Array[S], from: Int, to: Int)(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, from, to-from, 0)(el)

  def sortBy[T, S](arr: Array[T], f: T => S, from: Int, to: Int)(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, from, to-from, 0)((RadixQuicksortElement.Mapped(f)))


  def sort[S](arr: Array[S], from: Int, to: Int, depth: Int = 0)(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, from, to-from, depth)(el)

  def sortBy[T, S](arr: Array[T], f: T => S, from: Int, to: Int, depth: Int = 0)(implicit el: RadixQuicksortElement[S]) =
    sort0(arr, from, to-from, depth)((RadixQuicksortElement.Mapped(f)))



  private def sort0[S](arr: Array[S], base: Int, len: Int, depth: Int)(el: RadixQuicksortElement[S]): Unit = {

    // |---equal---|---lt---|---not yet partitioned---|---gt---|---equal---|
    //            aEq       a                         b       bEq

    if (len <= 1)
      return

    // insertion sort
//    if (len < 8) {
//      val start = base
//      val end   = base + len
//
//      var i = start + 1
//      while (i < end) {
//        val item = arr(i)
//        var hole = i
//        while (hole > start && arr(hole - 1) > item) {
//          arr(hole) = arr(hole - 1)
//          hole -= 1
//        }
//        arr(hole) = item
//        i += 1
//      }
//
//      return
//    }

    var r = ThreadLocalRandom.current().nextInt(len)

    swap(arr, base, base+r)
    val pivot = el.charAt(arr(base), depth)

    var aEq = base
    var a   = base
    var b   = base + len - 1
    var bEq = base + len - 1

    do {
      while (a <= b && el.charAt(arr(a), depth) <= pivot) {
        if (el.charAt(arr(a), depth) == pivot) {
          swap(arr, aEq, a)
          aEq += 1
        }
        a += 1
      }

      while (a <= b && el.charAt(arr(b), depth) >= pivot) {
        if (el.charAt(arr(b), depth) == pivot) {
          swap(arr, bEq, b)
          bEq -= 1
        }
        b -= 1
      }

      if (a <= b) { // if (a > b) break
        swap(arr, a, b)

        a += 1
        b -= 1
      }
    } while (a <= b)

    val aEqLen = math.min(aEq-base, a-aEq)
    vecswap(base, a-aEqLen, aEqLen, arr);

    val bEqLen = math.min(bEq-b, base+len-bEq-1)
    vecswap(a, base+len-bEqLen, bEqLen, arr);

    r = a-aEq
    sort0(arr, base, r, depth)(el)

    if (el.charAt(arr(base+r), depth) != -1) {
      sort0(arr, base + r, aEq + len-bEq-1, depth+1)(el)
    }

    r = bEq-b
    sort0(arr, base + len-r, r, depth)(el)
  }

  private def swap[S](arr: Array[S], a: Int, b: Int) = {
    val x = arr(a)
    arr(a) = arr(b)
    arr(b) = x
  }

  private def vecswap[S](i: Int, j: Int, len: Int, arr: Array[S]): Unit = {
    var nn = len
    var ii = i
    var jj = j
    while (nn > 0) {
      swap(arr, ii, jj)
      ii += 1
      jj += 1
      nn -= 1
    }
  }

}
