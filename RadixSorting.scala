package atrox.sort

import scala.reflect.ClassTag
import atrox.Bits



abstract class RadixElement[S] {
  val classTag: ClassTag[S]
  /** 0-255 or -1 as terminal symbol */
  def byteAt(str: S, pos: Int): Int
  def sort(arr: Array[S], len: Int, depth: Int): Unit
}


object RadixElement {

  implicit val StringElement: RadixElement[String] = new RadixElement[String] {
    def byteAt(str: String, pos: Int): Int =
      if (str.length*2 > pos) {
        (str.charAt(pos/2) >> ((~pos & 1) * 8)) & 0xff
      } else -1

    def sort(arr: Array[String], len: Int, depth: Int): Unit =
      RadixQuicksort.sort(arr, 0, len, depth)

    val classTag: ClassTag[String] = implicitly[ClassTag[String]]
  }

  implicit val UnsignedByteArray: RadixElement[Array[Byte]] = new RadixElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int =
      if (pos < str.length) str(pos) & 0xff else -1

    def sort(arr: Array[Array[Byte]], len: Int, depth: Int): Unit =
      RadixQuicksort.sort[Array[Byte]](arr, 0, len, depth)(RadixElement.UnsignedByteArray)
      //Arrays.sort(arr, 0, len, byteArrayComparator)

    val classTag: ClassTag[Array[Byte]] = implicitly[ClassTag[Array[Byte]]]
  }

  val SignedByteArray: RadixElement[Array[Byte]] = new RadixElement[Array[Byte]] {
    def byteAt(str: Array[Byte], pos: Int): Int =
      if (pos < str.length) (str(pos)+0x80) & 0xff else -1

    def sort(arr: Array[Array[Byte]], len: Int, depth: Int): Unit =
      RadixQuicksort.sort[Array[Byte]](arr, 0, len, depth)(RadixElement.SignedByteArray)
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

  implicit val IntElement: RadixElement[Int] = new RadixElement[Int] {
    def byteAt(str: Int, pos: Int): Int = if (pos >= 4) -1 else ((str+0x80000000) >>> ((3-pos) * 8)) & 0xff
    def sort(arr: Array[Int], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Int] = implicitly[ClassTag[Int]]
  }

  implicit val LongElement: RadixElement[Long] = new RadixElement[Long] {
    def byteAt(str: Long, pos: Int): Int = if (pos >= 8) -1 else (((str+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    def sort(arr: Array[Long], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Long] = implicitly[ClassTag[Long]]
  }

  implicit val FloatElement: RadixElement[Float] = new RadixElement[Float] {
    def byteAt(str: Float, pos: Int): Int = if (pos >= 4) -1 else ((Bits.floatToSortableInt(str)+0x80000000) >>> ((3-pos) * 8)) & 0xff
    def sort(arr: Array[Float], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Float] = implicitly[ClassTag[Float]]
  }

  implicit val DoubleElement: RadixElement[Double] = new RadixElement[Double] {
    def byteAt(str: Double, pos: Int): Int = if (pos >= 8) -1 else (((Bits.doubleToSortableLong(str)+0x8000000000000000L) >>> ((7-pos) * 8)) & 0xff).toInt
    def sort(arr: Array[Double], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)
    val classTag: ClassTag[Double] = implicitly[ClassTag[Double]]
  }


  class Mapped[T, S](f: T => S)(implicit res: RadixElement[S], clt: ClassTag[T]) extends RadixElement[T] { self =>
    val classTag: ClassTag[T] = clt
    def byteAt(str: T, pos: Int): Int = res.byteAt(f(str), pos)
    def sort(arr: Array[T], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)(self)
  }

  def Mapped[T, S](f: T => S)(implicit els: RadixElement[S], clt: ClassTag[T]) = new Mapped[T, S](f)


  implicit def Option[T](implicit el: RadixElement[T]) = new RadixElement[Option[T]] { self =>
    val classTag: ClassTag[Option[T]] = implicitly[ClassTag[Option[T]]]
    def byteAt(str: Option[T], pos: Int): Int =
      if (pos == 0) {
        if (str.isEmpty) -1 /*None*/ else 0 /*Some*/
      } else {
        el.byteAt(str.get, pos-1)
      }
    def sort(arr: Array[Option[T]], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)(self)
  }

  implicit def Either[A, B](implicit ela: RadixElement[A], elb: RadixElement[B]) = new RadixElement[Either[A, B]] { self =>
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
    def sort(arr: Array[Either[A, B]], len: Int, depth: Int): Unit = RadixQuicksort.sort(arr, 0, len, depth)(self)
  }

}
