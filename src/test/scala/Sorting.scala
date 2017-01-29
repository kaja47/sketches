package atrox.test

import org.scalatest._
import atrox._
import atrox.sort._
import atrox.sketch._
import fastSparse._
import java.util.Arrays

class BurstSort extends FlatSpec {

  private val chars = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')) mkString ""
  def randomWord(len: Int, prefix: String = "", suffix: String = "")(implicit rand: util.Random) = {
    val sb = new StringBuilder(len + prefix.length + suffix.length)
    sb.append(prefix)
    var i = 0 ; while (i < len) {
      sb.append(chars.charAt(rand.nextInt(chars.length)))
      i += 1
    } 
    sb.append(suffix)
    sb.toString
  }

  def randomByteArray(len: Int)(implicit rand: util.Random) = {
    val arr = new Array[Byte](len)
    rand.nextBytes(arr)
    arr
  }

  def shouldBeSorted(arr: Array[String]): Unit = {
    {
      val res = arr.sorted
      BurstSort.sort(arr)
      assert(arr === res)
    }
    {
      val res = arr.sorted.reverse
      BurstSort.reverseSort(arr)
      assert(arr === res)
    }
  }

  def shouldBeSorted(arr: Array[Array[Byte]]): Unit = {
    val arr1 = arr.clone
    val res1 = arr.clone
    Arrays.sort(res1, RadixElement.unsignedByteArrayComparator)
    BurstSort.sort(arr1)
    //RadixQuicksort.sort(arr1)
    assert(arr1 === res1, s"unsigned")

    val arr2 = arr.clone
    val res2 = arr.clone
    Arrays.sort(res2, RadixElement.signedByteArrayComparator)
    BurstSort.sort(arr2)(RadixElement.SignedByteArrays)
    //RadixQuicksort.sort(arr2)(RadixElement.SignedByteArray)
    assert(arr2 === res2, s"signed\n${arr2.toSeq.map(_.mkString("[", ", ", "]"))}\n${res2.toSeq.map(_.mkString("[", ", ", "]"))}")
  }

  def shouldBeSortedT[T <: AnyRef](arr: Array[T])(implicit ord: Ordering[T], bsel: RadixElement[T]): Unit = {
    val res = arr.sorted
    BurstSort.sort(arr)
    assert(arr === res)
  }


  "BurstSort" should "not crash sorting empty arrays" in {

    BurstSort.sort(Array[String]())
    BurstSort.sort(Array[Array[Byte]]())
    BurstSort.sortBy(Array[String]())(_.length)
    BurstSort.sortBy(Array[String]())(_.length.toLong)

    BurstSort.sorted(Array[String]())
    BurstSort.sorted(Array[Array[Byte]]())

  }


  "BurstSort" should "sort by string" in {
    shouldBeSorted(Array[String]())
    shouldBeSorted(Array[String](""))
    shouldBeSorted(Array[String]("", ""))
    shouldBeSorted(Array[String]("a", "c", "b"))
    shouldBeSorted(Array[String]("a", "", "aa"))
  }


  "BurstSort" should "be able to sort random strings" in {
    implicit val rand = new util.Random(4747)

    for (_ <- 0 until 100) {
      val strings = Vector.fill(256)(randomWord(12))
      for (_ <- 0 until 10) {
        val shuffled = rand.shuffle(strings).toArray
        shouldBeSorted(shuffled)
      }
    }

    for (_ <- 0 until 100) {
      val strings = Vector.fill(256)(randomWord(rand.nextInt(16)))
      for (_ <- 0 until 10) {
        shouldBeSorted(rand.shuffle(strings).toArray)
      }
    }

    val strings = Array.fill(1<<15)(randomWord(rand.nextInt(16)))
    shouldBeSorted(strings)
  }


  "BurstSort" should "be able to sort random byte arrays" in {
    implicit val rand = new util.Random(4747)

    for (_ <- 0 until 100) {
      val strings = Vector.fill(4)(randomByteArray(12))
      for (_ <- 0 until 10) {
        val shuffled = rand.shuffle(strings).toArray
        shouldBeSorted(shuffled)
      }
    }

    for (_ <- 0 until 100) {
      val strings = Vector.fill(256)(randomByteArray(rand.nextInt(16)))
      for (_ <- 0 until 10) {
        shouldBeSorted(rand.shuffle(strings).toArray)
      }
    }
  }

  "BurstSort" should "sort by integer" in {
    case class C(a: Int, ord: Int)

    def shouldBeSorted(arr: Array[Int]): Unit =
      shouldBeSortedC(arr.zipWithIndex map C.tupled)

    def shouldBeSortedC(arr: Array[C]): Unit = {
      val res = arr.sortBy(_.a).map(_.a)
      BurstSort.sortBy(arr)(_.a)
      assert(arr.map(_.a) === res)
    }

    shouldBeSorted(Array(1,2,3,4,5,6,7,8,9,10))
    shouldBeSorted(Array(1,1,1,1,1,1,1))
    shouldBeSorted(Array(10,9,8,7,6,5,4,3,2,1))

    shouldBeSorted(Array(1, 1<<8, 1<<16, 1<<24))
    shouldBeSorted(Array(1, 2, 1<<8, 1<<8+1, 1<<16, 1<<16+1, 1<<24, 1<<24+1))
    shouldBeSorted(Array(-1, 0, -2, 1, -3, 2, -2, 0))

  }


  "BurstSort" should "sort Options" in {
    val xs = Array[Option[Int]](Some(1), None, Some(2), None, Some(-1))
    shouldBeSortedT(xs)
  }

  "BurstSort" should "sort Eithers" in {
    val xs = Array[Either[Int, String]](Left(99), Right("z"), Left(5), Left(0), Right(""), Right("a"))
    implicit val ord = Ordering.by[Either[Int, String], (Int, Option[Int], Option[String])](e => e match {
      case Left(x)  => (1, Some(x), None)
      case Right(x) => (2, None, Some(x))
    })
    shouldBeSortedT(xs)
  }


}
