package atrox.sort

import java.util.Arrays
import scala.annotation.tailrec
import scala.reflect.ClassTag


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
  def sort[S <: AnyRef: RadixElement](arr: Array[S]) =
    (new BurstTrie[S, S]() ++= arr).sortInto(arr, s => s)

  def reverseSort[S <: AnyRef: RadixElement](arr: Array[S]) =
    (new BurstTrie[S, S]() ++= arr).sortInto(arr, s => s, true)

  def sortBy[T <: AnyRef: ClassTag, S: RadixElement](arr: Array[T])(f: T => S) =
    (new BurstTrie[T, T]()(RadixElement.Mapped(f)) ++= arr).sortInto(arr, s => s)

  /*
  def sortBySchwartzianTransform[T <: AnyRef, S](arr: Array[T], f: T => S)(implicit ctts: ClassTag[(T, S)], cts: ClassTag[S], els: RadixElement[S]) = {
    implicit val rqelts = RadixElement.Mapped[(T, S), S](_._2)
    val trie = new BurstTrie[(T, S), T]()(RadixElement.Mapped(_._2))
    for (x <- arr) trie += (x, f(x))
    trie.sortInto(arr, ts => ts._1)
  }
  */

  def sorted[S <: AnyRef : ClassTag : RadixElement](arr: TraversableOnce[S]) = {
    val trie = (new BurstTrie[S, S] ++= arr)
    val res = new Array[S](trie.size)
    trie.sortInto(res, s => s)
    res
  }

  def sortedBy[T <: AnyRef: ClassTag, S: RadixElement](arr: TraversableOnce[T], f: T => S) = {
    val trie = new BurstTrie[T, T]()(RadixElement.Mapped(f))
    for (x <- arr) trie += x
    val res = new Array[T](trie.size)
    trie.sortInto(res, x => x)
    res
  }
}



class BurstTrie[S <: AnyRef, R <: AnyRef](implicit el: RadixElement[S]) {

  implicit def ct = el.classTag

  val initSize = 16
  val resizeFactor = 8
  val maxSize = 1024*4

  private var _size = 0
  private val root: Array[AnyRef] = new Array[AnyRef](257)

  def size = _size


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


  def sortInto(res: Array[R], f: S => R, reverse: Boolean = false): Unit = {
    var pos = 0

    def run(node: Array[AnyRef], depth: Int): Unit = {
      if (!reverse) {
        var i = 0 ; while (i < 257) {
          doNode(node(i), i != 0, depth, f)
          i += 1
        }
      } else {
        var i = 256 ; while (i >= 0) {
          doNode(node(i), i != 0, depth, f)
          i -= 1
        }
      }
    }

    def doNode(n: AnyRef, sort: Boolean, depth: Int, f: S => R) = n match {
      case null =>
      case leaf: BurstLeaf[S @unchecked] =>
        if (sort) {
          el.sort(leaf.values, leaf.size, depth)
        }
        //System.arraycopy(leaf.values, 0, res, pos, leaf.size)
        if (reverse) {
          var i = 0 ; var j = leaf.size-1 ; while (i < leaf.size) {
            res(pos+j) = f(leaf.values(i))
            i += 1
            j -= 1
          }

        } else {
          var i = 0 ; while (i < leaf.size) {
            res(pos+i) = f(leaf.values(i))
            i += 1
          }
        }

        pos += leaf.size
      case node: Array[AnyRef] => run(node, depth + 1)
    }

    run(root, 0)
  }


  private def inorder: Iterator[(Int, BurstLeaf[S])] = {
    def iterate(node: AnyRef, depth: Int): Iterator[(Int, BurstLeaf[S])] = node match {
      case null => Iterator()
      case leaf: BurstLeaf[S @unchecked] => Iterator((depth, leaf))
      case node: Array[AnyRef] => node.iterator.flatMap(n => iterate(n, depth+1))
    }

    iterate(root, 0)
  }

  def lazySort = inorder.flatMap { case (depth, leaf) =>
    el.sort(leaf.values, leaf.size, depth)
    leaf.values
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
