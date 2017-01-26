package atrox.sort

import java.util.Arrays
import scala.annotation.tailrec
import scala.reflect.ClassTag
import atrox.Bits

// TODO
// - ASCII strings, int arrays, long arrays
// - reverse sorting, lazy sorting
// - unify RQ and BS typeclasses
// depth/2 ???


/** BurstSort
  *
  * Cache-Conscious Sorting of Large Sets of Strings with Dynamic Tries
  * http://goanna.cs.rmit.edu.au/~jz/fulltext/alenex03.pdf
  *
  * Ecient Trie-Based Sorting of Large Sets of Strings
  * http://goanna.cs.rmit.edu.au/~jz/fulltext/acsc03sz.pdf
  **/
object BurstSort {
  /** in-place sorting */
  def sort[S <: AnyRef](arr: Array[S])(implicit el: BurstElement[S]) = {
    implicit val ct = el.classTag
    (new BurstTrie[S, S] ++= arr).sortInto(arr, s => s)
  }

  def sortBy[T <: AnyRef, S](arr: Array[T])(f: T => S)(implicit ct: ClassTag[T], el: BurstElement[S], rqel: RadixQuicksortElement[S]) = {
    implicit val rqelts = RadixQuicksortElement.Mapped[T, S](f)
    (new BurstTrie[T, T]()(BurstElement.Mapped(f)) ++= arr).sortInto(arr, s => s)
  }

  /*
  def sortBySchwartzianTransform[T <: AnyRef, S](arr: Array[T], f: T => S)(implicit ctts: ClassTag[(T, S)], cts: ClassTag[S], els: BurstElement[S], rqel: RadixQuicksortElement[S]) = {
    implicit val rqelts = RadixQuicksortElement.Mapped[(T, S), S](_._2)
    val trie = new BurstTrie[(T, S), T]()(BurstElement.Mapped(_._2))
    for (x <- arr) trie += (x, f(x))
    trie.sortInto(arr, ts => ts._1)
  }
  */

  def sorted[S <: AnyRef](arr: TraversableOnce[S])(implicit ct: ClassTag[S], el: BurstElement[S]) = {
    val trie = (new BurstTrie[S, S] ++= arr)
    val res = new Array[S](trie.size)
    trie.sortInto(res, s => s)
    res
  }

  /*
  def sortedBy[T, S](arr: TraversableOnce[T], f: T => S)(implicit ctts: ClassTag[(T, S)], cts: ClassTag[S], ctt: ClassTag[T], els: BurstElement[S], rqel: RadixQuicksortElement[S]) = {
    val trie = new BurstTrie[(T, S), T]()(BurstElement.Mapped(f))
    for (x <- arr) trie += ((x, f(x)))
    val res = new Array[T](trie.size)
    trie.sortInto(res, ts => ts._1)
    res
  }
  */
}



class BurstTrie[S <: AnyRef, R <: AnyRef](implicit el: BurstElement[S]) {

  implicit def ct = el.classTag

  val initSize = 16
  val resizeFactor = 8
  val maxSize = 1024*8

  private var _size = 0
  private val root: Array[AnyRef] = new Array[AnyRef](257)

  def size = _size


  def sortInto(res: Array[R], f: S => R): Unit =
    sortInto0(res, f)


  def ++= (strs: TraversableOnce[S]): this.type = {
    for (str <- strs) { this += str } ; this
  }


  def += (str: S): this.type = {

    @tailrec
    def add(str: S, depth: Int, node: Array[AnyRef]): Unit = {

      val char = el.byteAt(str, depth)

      node(char) match {
        case null =>
          val leaf = new BurstLeaf[S](initSize)
          leaf.add(str)
          node(char) = leaf

        case leaf: BurstLeaf[S @unchecked] =>
          // add string, resize or burst
          if (leaf.size == leaf.values.length) {
            if (leaf.size * resizeFactor <= maxSize || char == 256 /* || current byte is the last one */) { // resize
              leaf.resize(resizeFactor)
              leaf.add(str)

            } else { // burst
              val newNode = new Array[AnyRef](257)
              node(char) = newNode

              for (i <- 0 until leaf.size) {
                add0(leaf.values(i), depth+1, newNode)
              }

              add(str, depth+1, newNode)
            }

          } else {
            leaf.add(str)
          }

        case child: Array[AnyRef] =>
          add(str, depth+1, child)
      }
    }

    def add0(str: S, depth: Int, node: Array[AnyRef]): Unit = add(str, depth, node)

    add(str, 0, root)
    _size += 1
    this
  }


  protected def sortInto0(res: Array[R], f: S => R): Unit = {
    var pos = 0

    def run(node: Array[AnyRef], depth: Int): Unit = {
      doNode(node(256), false, depth, f)

      var i = 0 ; while (i < 256) {
        doNode(node(i), true, depth, f)
        i += 1
      }
    }

    def doNode(n: AnyRef, sort: Boolean, depth: Int, f: S => R) = n match {
      case null =>
      case leaf: BurstLeaf[S @unchecked] =>
        if (sort) {
          el.sort(leaf.values, leaf.size, depth)
        }
        //System.arraycopy(leaf.values, 0, res, pos, leaf.size)
        var i = 0 ; while (i < leaf.size) {
          res(i+pos) = f(leaf.values(i))
          i += 1
        }

        pos += leaf.size
      case node: Array[AnyRef] => run(node, depth + 1)
    }

    run(root, 0)
  }

}



private final class BurstLeaf[S <: AnyRef](initSize: Int)(implicit ct: ClassTag[S]) {
  var size: Int = 0
  var values: Array[S] = new Array[S](initSize)

  def add(str: S) = {
    values(size) = str
    size += 1
  }

  def resize(factor: Int) = {
    values = Arrays.copyOfRange(values.asInstanceOf[Array[AnyRef]], 0, values.length * factor).asInstanceOf[Array[S]]
  }

  override def toString = values.mkString("BurstLeaf(", ",", ")")
}



abstract class BurstElement[S] {
  val classTag: ClassTag[S]
  def byteAt(str: S, pos: Int): Int
  def sort(arr: Array[S], len: Int, depth: Int): Unit
}



object BurstElement {

  implicit val StringElement: BurstElement[String] = new BurstElement[String] {
    def byteAt(str: String, pos: Int): Int =
      if (str.length*2 > pos) {
        (str.charAt(pos/2) >> ((~pos & 1) * 8)) & 0xff
      } else 256

    def sort(arr: Array[String], len: Int, depth: Int): Unit =
      RadixQuicksort.sort(arr, 0, len, depth/2)

    val classTag: ClassTag[String] = implicitly[ClassTag[String]]
  }

  implicit val UnsignedByteArray: BurstElement[Array[Byte]] = new BurstElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int =
      if (pos < str.length) str(pos) & 0xff else 256

    def sort(arr: Array[Array[Byte]], len: Int, depth: Int): Unit =
      RadixQuicksort.sort[Array[Byte]](arr, 0, len, depth)(RadixQuicksortElement.UnsignedByteArray)
      //Arrays.sort(arr, 0, len, byteArrayComparator)

    val classTag: ClassTag[Array[Byte]] = implicitly[ClassTag[Array[Byte]]]
  }

  val SignedByteArray: BurstElement[Array[Byte]] = new BurstElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int =
      if (pos < str.length) (str(pos)+0x80) & 0xff else 256

    def sort(arr: Array[Array[Byte]], len: Int, depth: Int): Unit =
      RadixQuicksort.sort[Array[Byte]](arr, 0, len, depth)(RadixQuicksortElement.SignedByteArray)
      //Arrays.sort(arr, 0, len, byteArrayComparator)

    val classTag: ClassTag[Array[Byte]] = implicitly[ClassTag[Array[Byte]]]
  }

  val unsignedByteArrayComparator = new java.util.Comparator[Array[Byte]] {
    def compare(l: Array[Byte], r: Array[Byte]): Int = {
      var i = 0
      while (i < l.length && i < r.length) {
        val a = (l(i) & 0xff)
        val b = (r(i) & 0xff)
        if (a != b) { return a - b }
        i += 1
      }
      l.length - r.length
    }
  }

  val signedByteArrayComparator = new java.util.Comparator[Array[Byte]] {
    def compare(l: Array[Byte], r: Array[Byte]): Int = {
      var i = 0
      while (i < l.length && i < r.length) {
        val a = l(i)
        val b = r(i)
        if (a != b) { return a - b }
        i += 1
      }
      l.length - r.length
    }
  }

  implicit val IntElement: BurstElement[Int] = new BurstElement[Int] {
    def byteAt(str: Int, pos: Int): Int = if (pos >= 4) 256 else ((str+0x80000000) >>> ((3-pos) * 8)) & 0xff
    def sort(arr: Array[Int], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Int] = implicitly[ClassTag[Int]]
  }

  implicit val LongElement: BurstElement[Long] = new BurstElement[Long] {
    def byteAt(str: Long, pos: Int): Int = if (pos >= 8) 256 else (((str+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    def sort(arr: Array[Long], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Long] = implicitly[ClassTag[Long]]
  }

  implicit val FloatElement: BurstElement[Float] = new BurstElement[Float] {
    def byteAt(str: Float, pos: Int): Int = if (pos >= 4) 256 else ((Bits.floatToSortableInt(str)+0x80000000) >>> ((3-pos) * 8)) & 0xff
    def sort(arr: Array[Float], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Float] = implicitly[ClassTag[Float]]
  }

  implicit val DoubleElement: BurstElement[Double] = new BurstElement[Double] {
    def byteAt(str: Double, pos: Int): Int = if (pos >= 8) 256 else (((Bits.doubleToSortableLong(str)+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    def sort(arr: Array[Double], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Double] = implicitly[ClassTag[Double]]
  }


  class Mapped[T, S](f: T => S)(implicit els: BurstElement[S], ctts: ClassTag[T], rqel: RadixQuicksortElement[T]) extends BurstElement[T] {
    val classTag: ClassTag[T] = ctts
    def byteAt(str: T, pos: Int): Int = els.byteAt(f(str), pos)
    def sort(arr: Array[T], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
  }

  def Mapped[T, S](f: T => S)(implicit els: BurstElement[S], ctts: ClassTag[T], rqel: RadixQuicksortElement[T]) = new Mapped[T, S](f)


  implicit def Option[T](implicit el: BurstElement[T], rqel: RadixQuicksortElement[T]) = new BurstElement[Option[T]] {
    val classTag: ClassTag[Option[T]] = implicitly[ClassTag[Option[T]]]
    def byteAt(str: Option[T], pos: Int): Int =
      if (pos == 0) {
        if (str.isEmpty) 256 /*None*/ else 0 /*Some*/
      } else {
        el.byteAt(str.get, pos-1)
      }
    def sort(arr: Array[Option[T]], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
  }

  implicit def Either[A, B](implicit ela: BurstElement[A], elb: BurstElement[B], rqela: RadixQuicksortElement[A], rqelb: RadixQuicksortElement[B]) = new BurstElement[Either[A, B]] {
    val classTag: ClassTag[Either[A, B]] = implicitly[ClassTag[Either[A, B]]]
    def byteAt(str: Either[A, B], pos: Int): Int =
      if (pos == 0) {
        if (str.isLeft) 1 else 2
      } else {
        str match {
          case Left(x)  => ela.byteAt(x, pos-1)
          case Right(x) => elb.byteAt(x, pos-1)
        }
      }
    def sort(arr: Array[Either[A, B]], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
  }

}
