package atrox.sort

import java.util.Arrays
import scala.annotation.tailrec
import scala.reflect.ClassTag

// TODO
// - ASCII strings, int arrays, long arrays
// - reverse sorting, lazy sorting


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
  def sort[S <: AnyRef](arr: Array[S])(implicit el: RadixElement[S]) = {
    implicit val ct = el.classTag
    (new BurstTrie[S, S] ++= arr).sortInto(arr, s => s)
  }

  def sortBy[T <: AnyRef, S](arr: Array[T])(f: T => S)(implicit res: RadixElement[S], ct: ClassTag[T]) = {
    implicit val rqelts = RadixElement.Mapped[T, S](f)
    (new BurstTrie[T, T]()(RadixElement.Mapped(f)) ++= arr).sortInto(arr, s => s)
  }

  /*
  def sortBySchwartzianTransform[T <: AnyRef, S](arr: Array[T], f: T => S)(implicit ctts: ClassTag[(T, S)], cts: ClassTag[S], els: RadixElement[S]) = {
    implicit val rqelts = RadixQuicksortElement.Mapped[(T, S), S](_._2)
    val trie = new BurstTrie[(T, S), T]()(RadixElement.Mapped(_._2))
    for (x <- arr) trie += (x, f(x))
    trie.sortInto(arr, ts => ts._1)
  }
  */

  def sorted[S <: AnyRef](arr: TraversableOnce[S])(implicit ct: ClassTag[S], el: RadixElement[S]) = {
    val trie = (new BurstTrie[S, S] ++= arr)
    val res = new Array[S](trie.size)
    trie.sortInto(res, s => s)
    res
  }

  /*
  def sortedBy[T, S](arr: TraversableOnce[T], f: T => S)(implicit ctts: ClassTag[(T, S)], cts: ClassTag[S], ctt: ClassTag[T], els: RadixElement[S]) = {
    val trie = new BurstTrie[(T, S), T]()(RadixElement.Mapped(f))
    for (x <- arr) trie += ((x, f(x)))
    val res = new Array[T](trie.size)
    trie.sortInto(res, ts => ts._1)
    res
  }
  */
}



class BurstTrie[S <: AnyRef, R <: AnyRef](implicit el: RadixElement[S]) {

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

      val char = el.byteAt(str, depth)+1 // 0-th slot is for strings with terminal symbol at depth

      node(char) match {
        case null =>
          val leaf = new BurstLeaf[S](initSize)
          leaf.add(str)
          node(char) = leaf

        case leaf: BurstLeaf[S @unchecked] =>
          // add string, resize or burst
          if (leaf.size == leaf.values.length) {
            if (leaf.size * resizeFactor <= maxSize || char == 0 /* || current byte is the last one */) { // resize
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
      doNode(node(0), false, depth, f)

      var i = 1 ; while (i < 257) {
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
