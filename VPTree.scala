package atrox

import scala.language.postfixOps
import collection.mutable
import VPTree._


/** VP-tree (vantage point tree) is a datastructure that can be used for
  * nearest-neighbour queries in arbitrary metric space.
  *
  * TODO http://boytsov.info/pubs/nips2013.pdf
  */
final class VPTree[T](val root: Tree[T], val distance: Distance[T]) {
  def approximateNearest(t: T): T = root.approxNear(t, distance)
  def approximateNearestN(t: T, n: Int): IndexedSeq[T] = root.approxNearN(t, n, distance)
  def nearest(t: T, maxDist: Double) = root.nearN(t, maxDist, distance)
}


object VPTree {
  type Distance[T] = (T, T) => Double

  /** Main constructor of VP-trees */
  def apply[T](items: IndexedSeq[T], distance: Distance[T], leafSize: Int): VPTree[T] =
    new VPTree(mkNode(items, distance, leafSize), distance)

  sealed trait Tree[T] {
    def size: Int
    def toSeq: IndexedSeq[T]
    def approxNear(t: T, f: Distance[T]): T
    def approxNearN(t: T, n: Int, f: Distance[T]): IndexedSeq[T]
    def nearN(t: T, maxDist: Double, f: Distance[T]): IndexedSeq[T]
  }

  final case class Node[T](point: T, radius: Double, size: Int, in: Tree[T], out: Tree[T]) extends Tree[T] {
    def toSeq = in.toSeq ++ out.toSeq
    def approxNear(t: T, f: Distance[T]): T = {
      val d = f(point, t)
      if (d < radius) in.approxNear(t, f)
      else out.approxNear(t, f)
    }
    def approxNearN(t: T, n: Int, f: Distance[T]): IndexedSeq[T] =
      if (n <= 0) IndexedSeq()
      else if (n > size) toSeq
      else {
        val d = f(point, t)
        if (d < radius) {
          in.approxNearN(t, n, f)  ++ out.approxNearN(t, n - in.size, f)
        } else {
          out.approxNearN(t, n, f) ++ in.approxNearN(t, n - out.size, f)
        }
      }

    def nearN(t: T, maxDist: Double, f: Distance[T]): IndexedSeq[T] = {
      val d = f(t, point)
      if (d + maxDist < radius) {
        in.nearN(t, maxDist, f)
      } else if (d - maxDist >= radius) {
        out.nearN(t, maxDist, f)
      } else {
        in.nearN(t, maxDist, f) ++ out.nearN(t, maxDist, f)
      }
    }
  }

  final case class Leaf[T](points: IndexedSeq[T]) extends Tree[T] {
    def size = points.length
    def toSeq = points
    def approxNear(t: T, f: Distance[T]): T = points minBy (p => f(t, p))
    def approxNearN(t: T, n: Int, f: Distance[T]): IndexedSeq[T] =
      if (n <= 0) IndexedSeq()
      else if (n >= size) points
      else points sortBy (p => f(p, t)) take n

    def nearN(t: T, maxDist: Double, f: Distance[T]): IndexedSeq[T] =
      points filter { p => f(t, p) <= maxDist }
  }

  def mkNode[T](items: IndexedSeq[T], f: Distance[T], leafSize: Int): Tree[T] = {
    if (items.length <= leafSize) {
      Leaf(items)
    } else {
      val vp = items(util.Random.nextInt(items.length))

      val radius = {
        val numSamples = math.sqrt(items.length).toInt * 2
        val distances = pickSample(items, numSamples).map(i => f(vp, i)).toArray
        java.util.Arrays.sort(distances)
        distances(distances.length / 2)
      }

      val (in, out) = items partition { item => f(item, vp) < radius }

      if (in.length == 0) Leaf(out)
      else if (out.length == 0) Leaf(in)
      else Node(vp, radius, items.length, mkNode(in, f, leafSize), mkNode(out, f, leafSize))
    }
  }

  def pickSample[T](items: IndexedSeq[T], size: Int): IndexedSeq[T] =
    if (items.length <= size) items
    else IndexedSeq.fill(size)(items(util.Random.nextInt(items.length)))

  def balance[T](t: Tree[T]): List[(Int, Int)] = t match {
    case Leaf(_) => Nil
    case Node(_, _, _, in, out) => List((in.size, out.size)) ::: balance(in) ::: balance(out)
  }

  def prettyPrint[T](n: Tree[T], offset: Int = 0): String = n match {
    case Leaf(points) =>
      (" "*offset)+"Leaf("+points.mkString(",")+")\n"
    case n: Node[_] =>
      (" "*offset)+"Node(point = "+n.point+", radius = "+n.radius+"\n"+
      prettyPrint(n.in, offset+2)+
      prettyPrint(n.out, offset+2)+
      (" "*offset)+")\n"
  }
}
