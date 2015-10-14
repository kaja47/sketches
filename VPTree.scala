package atrox

import scala.language.postfixOps
import collection.mutable
import VPTree._


/** VP-tree (vantage point tree) is a datastructure that can be used for
  * nearest-neighbour queries in arbitrary metric space.
  *
  * Because I'm lazy man, I implemented only methods for rough approximate
  * queries.
  */
class VPTree[T](val root: Tree[T]) {
  def approximateNearest(t: T): T = root.approxNear(t)
  def approximateNearestN(t: T, n: Int): IndexedSeq[T] = root.approxNearN(t, n)
}


object VPTree {
  type Distance[T] = (T, T) => Double

  /** Main constructor of VP-trees */
  def apply[T](items: IndexedSeq[T], distance: Distance[T], leafSize: Int) =
    new VPTree(mkNode(items, distance, leafSize))

  sealed trait Tree[T] {
    def size: Int
    def distance: Distance[T]
    def toSeq: IndexedSeq[T]
    def approxNear(t: T): T
    def approxNearN(t: T, n: Int): IndexedSeq[T]
  }

  final case class Node[T](point: T, radius: Double, size: Int, in: Tree[T], out: Tree[T], distance: Distance[T]) extends Tree[T] {
    def toSeq = in.toSeq ++ out.toSeq
    def approxNear(t: T): T = {
      val d = distance(point, t)
      if (d < radius) in.approxNear(t)
      else out.approxNear(t)
    }
    def approxNearN(t: T, n: Int): IndexedSeq[T] =
      if (n <= 0) IndexedSeq()
      else if (n > size) toSeq
      else {
        val d = distance(point, t)
        if (d < radius) {
          in.approxNearN(t, n)  ++ out.approxNearN(t, n - in.size)
        } else {
          out.approxNearN(t, n) ++ in.approxNearN(t, n - out.size)
        }
      }
  }

  final case class Leaf[T](points: IndexedSeq[T], distance: Distance[T]) extends Tree[T] {
    def size = points.length
    def toSeq = points
    def approxNear(t: T): T = points minBy (p => distance(t, p))
    def approxNearN(t: T, n: Int): IndexedSeq[T] =
      if (n <= 0) IndexedSeq()
      else if (n >= size) points
      else points sortBy (p => distance(p, t)) take n
  }

  def mkNode[T](items: IndexedSeq[T], distance: Distance[T], leafSize: Int): Tree[T] = {
    if (items.length <= leafSize) {
      Leaf(items, distance)
    } else {
      // find vantage point
      /*
      val vp = pickBestCandidate((candidate: T, sample: Seq[T]) => {
        val distances = (sample map { s => distance(s, candidate) }).toArray
        java.util.Arrays.sort(distances)
        val median = distances(distances.length / 2)

        var spread = 0.0
        for (d <- distances) spread += math.pow(d - median, 2)
        -spread
      }, items, selectCandidates = 128, testSampleSize = 100)
      */

      val vp = items(util.Random.nextInt(items.length))

      val radius = {
        val numSamples = math.sqrt(items.length).toInt * 2
        val distances = pickSample(items, numSamples).map(i => distance(vp, i)).toArray
        java.util.Arrays.sort(distances)
        distances(distances.length / 2)
      }

      val (in, out) = items partition { item => distance(item, vp) < radius }

      if (in.length == 0) Leaf(out, distance)
      else if (out.length == 0) Leaf(in, distance)
      else Node(vp, radius, items.length, mkNode(in, distance, leafSize), mkNode(out, distance, leafSize), distance)
    }
  }

  def pickBestCandidate[T](score: (T, IndexedSeq[T]) => Double, items: IndexedSeq[T], selectCandidates: Int, testSampleSize: Int): T = {
    var candidates = pickSample(items, selectCandidates)
    val testSample = pickSample(items, testSampleSize)
    candidates minBy (c => score(c, testSample))
  }

  def pickSample[T](items: IndexedSeq[T], size: Int): IndexedSeq[T] =
    if (items.length <= size) items
    else IndexedSeq.fill(size)(items(util.Random.nextInt(items.length)))

  def balance[T](t: Tree[T]): List[(Int, Int)] = t match {
    case Leaf(_, _) => Nil
    case Node(_, _, _, in, out, _) => List((in.size, out.size)) ::: balance(in) ::: balance(out)
  }

  def prettyPrint[T](n: Tree[T], offset: Int = 0): String = n match {
    case Leaf(points, _) =>
      (" "*offset)+"Leaf("+points.mkString(",")+")\n"
    case n: Node[_] =>
      (" "*offset)+"Node(point = "+n.point+", radius = "+n.radius+"\n"+
      prettyPrint(n.in, offset+2)+
      prettyPrint(n.out, offset+2)+
      (" "*offset)+")\n"
  }
}
