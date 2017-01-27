package atrox.sort

import scala.reflect.ClassTag
import atrox.Bits



abstract class RadixElement[S] {
  val classTag: ClassTag[S]
  /** 0-255 or -1 as terminal symbol */
  def byteAt(str: S, pos: Int): Int
  def sort(arr: Array[S], len: Int, depth: Int): Unit =
    RadixQuicksort.sort(arr, 0, len, depth)(this)
}


object RadixElement {

  implicit val Strings: RadixElement[String] = new RadixElement[String] {
    def byteAt(str: String, pos: Int): Int = if (pos >= str.length*2) -1 else (str.charAt(pos/2) >> ((~pos & 1) * 8)) & 0xff
    val classTag: ClassTag[String] = implicitly[ClassTag[String]]
  }

  val ASCIIStrings: RadixElement[String] = new RadixElement[String] {
    def byteAt(str: String, pos: Int): Int = if (pos >= str.length) -1 else str.charAt(pos) & 0xff
    val classTag: ClassTag[String] = implicitly[ClassTag[String]]
  }

  implicit val UnsignedByteArrays: RadixElement[Array[Byte]] = new RadixElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int = if (pos < str.length) str(pos) & 0xff else -1
    val classTag: ClassTag[Array[Byte]] = implicitly[ClassTag[Array[Byte]]]
  }

  val SignedByteArrays: RadixElement[Array[Byte]] = new RadixElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int = if (pos < str.length) (str(pos)+0x80) & 0xff else -1
    val classTag: ClassTag[Array[Byte]] = implicitly[ClassTag[Array[Byte]]]
  }

  implicit val IntArrays: RadixElement[Array[Int]] = new RadixElement[Array[Int]] {
    def byteAt(str: Array[Int], pos: Int): Int = if (pos >= str.length*4) -1 else ((str(pos/4)+0x80000000) >> (3-(pos%4) * 8)) & 0xff
    val classTag: ClassTag[Array[Int]] = implicitly[ClassTag[Array[Int]]]
  }

  implicit val LongArrays: RadixElement[Array[Long]] = new RadixElement[Array[Long]] {
    def byteAt(str: Array[Long], pos: Int): Int = if (pos >= str.length*8) -1 else (((str(pos/8)+0x8000000000000000L) >> (7-(pos%8) * 8)) & 0xff).toInt
    val classTag: ClassTag[Array[Long]] = implicitly[ClassTag[Array[Long]]]
  }

  implicit val Ints: RadixElement[Int] = new RadixElement[Int] {
    def byteAt(str: Int, pos: Int): Int = if (pos >= 4) -1 else ((str+0x80000000) >>> ((3-pos) * 8)) & 0xff
    val classTag: ClassTag[Int] = implicitly[ClassTag[Int]]
  }

  implicit val Longs: RadixElement[Long] = new RadixElement[Long] {
    def byteAt(str: Long, pos: Int): Int = if (pos >= 8) -1 else (((str+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    val classTag: ClassTag[Long] = implicitly[ClassTag[Long]]
  }

  implicit val Floats: RadixElement[Float] = new RadixElement[Float] {
    def byteAt(str: Float, pos: Int): Int = if (pos >= 4) -1 else ((Bits.floatToSortableInt(str)+0x80000000) >>> ((3-pos) * 8)) & 0xff
    val classTag: ClassTag[Float] = implicitly[ClassTag[Float]]
  }

  implicit val Doubles: RadixElement[Double] = new RadixElement[Double] {
    def byteAt(str: Double, pos: Int): Int = if (pos >= 8) -1 else (((Bits.doubleToSortableLong(str)+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    val classTag: ClassTag[Double] = implicitly[ClassTag[Double]]
  }

  class Mapped[T, S](f: T => S)(implicit res: RadixElement[S], clt: ClassTag[T]) extends RadixElement[T] {
    val classTag: ClassTag[T] = clt
    def byteAt(str: T, pos: Int): Int = res.byteAt(f(str), pos)
  }

  def Mapped[T, S](f: T => S)(implicit els: RadixElement[S], clt: ClassTag[T]) = new Mapped[T, S](f)

  implicit def Options[T](implicit el: RadixElement[T]) = new RadixElement[Option[T]] {
    val classTag: ClassTag[Option[T]] = implicitly[ClassTag[Option[T]]]
    def byteAt(str: Option[T], pos: Int): Int =
      if (pos == 0) {
        if (str.isEmpty) -1 /*None*/ else 0 /*Some*/
      } else {
        el.byteAt(str.get, pos-1)
      }
  }

  implicit def Eithers[A, B](implicit ela: RadixElement[A], elb: RadixElement[B]) = new RadixElement[Either[A, B]] {
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
}
