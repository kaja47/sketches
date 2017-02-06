package atrox.sort

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import atrox.Bits
import scala.reflect.ClassTag

// Three-way radix quicksort aka Multi-key quicksort
//
// Fast Algorithms for Sorting and Searching Strings
// http://www.cs.princeton.edu/~rs/strings/paper.pdf




object RadixQuicksort {


  def sort[S](arr: Array[S])(implicit res: RadixElement[S]) =
    sort0(arr, 0, arr.length, 0)(res)

  def sortBy[T, S](arr: Array[T], f: T => S)(implicit res: RadixElement[S], clt: ClassTag[T]) =
    sort0(arr, 0, arr.length, 0)(RadixElement.Mapped(f))

  def sortBySchwartzianTransform[T, S](arr: Array[T], f: T => S)(implicit res: RadixElement[S], ret: RadixElement[T]) = {
    val tmp = Array.tabulate[(T, S)](arr.length){ i => (arr(i), f(arr(i))) }
    sort0(tmp, 0, arr.length, 0)(RadixElement.Mapped(_._2))
    for (i <- 0 until arr.length) {
      arr(i) = tmp(i)._1
    }
  }


  def sort[S](arr: Array[S], from: Int, to: Int)(implicit res: RadixElement[S]) =
    sort0(arr, from, to-from, 0)(res)

  def sortBy[T, S](arr: Array[T], f: T => S, from: Int, to: Int)(implicit res: RadixElement[S], clt: ClassTag[T]) =
    sort0(arr, from, to-from, 0)((RadixElement.Mapped(f)))


  def sort[S](arr: Array[S], from: Int, to: Int, depth: Int = 0)(implicit res: RadixElement[S]) =
    sort0(arr, from, to-from, depth)(res)

  def sortBy[T, S](arr: Array[T], f: T => S, from: Int, to: Int, depth: Int = 0)(implicit res: RadixElement[S], clt: ClassTag[T]) =
    sort0(arr, from, to-from, depth)((RadixElement.Mapped(f)))



  private def sort0[S](arr: Array[S], base: Int, len: Int, depth: Int)(re: RadixElement[S]): Unit = {

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
    val pivot = re.byteAt(arr(base), depth)

    var aEq = base
    var a   = base
    var b   = base + len - 1
    var bEq = base + len - 1

    do {
      while (a <= b && re.byteAt(arr(a), depth) <= pivot) {
        if (re.byteAt(arr(a), depth) == pivot) {
          swap(arr, aEq, a)
          aEq += 1
        }
        a += 1
      }

      while (a <= b && re.byteAt(arr(b), depth) >= pivot) {
        if (re.byteAt(arr(b), depth) == pivot) {
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
    sort0(arr, base, r, depth)(re)

    if (re.byteAt(arr(base+r), depth) != -1) {
      sort0(arr, base + r, aEq + len-bEq-1, depth+1)(re)
    }

    r = bEq-b
    sort0(arr, base + len-r, r, depth)(re)
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
