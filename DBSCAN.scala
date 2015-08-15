package atrox

import scala.specialized
import scala.reflect.ClassTag


object DBSCAN {
  def apply[Point: ClassTag](dataset: IndexedSeq[Point], eps: Double, minPts: Int, dist: (Point, Point) => Double): Result[Point] =
    new DBSCAN(dataset.toArray, eps, minPts, dist).run

  def apply[Point: ClassTag](dataset: Array[Point], eps: Double, minPts: Int, dist: (Point, Point) => Double): Result[Point] =
    new DBSCAN(dataset, eps, minPts, dist).run

  def apply[Point: ClassTag](dataset: Array[Point], minPts: Int, regionQueryFunc: Int => IndexedSeq[Int]): Result[Point] =
    new DBSCAN(dataset, -1, minPts, ???) {
      override def regionQuery(pIdx: Int): IndexedSeq[Int] = regionQueryFunc(pIdx)
    }.run

  case class Result[Point](clusters: IndexedSeq[Map[Int, Point]], noise: Map[Int, Point])
}


class DBSCAN[Point: ClassTag](dataset: Array[Point], val eps: Double, val minPts: Int, val dist: (Point, Point) => Double) {

  val NotVisited = -1
  val Noise = -2

  // NotVisited, Noise, clutserId
  val pointClusters = Array.fill(dataset.size)(NotVisited)

  // @return seq of clusters and seq of points without cluster
  def run: DBSCAN.Result[Point] = {
    var currentClusterId = 0
    for (pIdx <- 0 until dataset.size) {
      if (pointClusters(pIdx) == NotVisited) {
        val neighborPts = regionQuery(pIdx)
        if (neighborPts.size < minPts) {
          pointClusters(pIdx) = Noise
          currentClusterId += 1
        } else {
          expandCluster(pIdx, neighborPts, currentClusterId)
        }
      }
    }

    assert(pointClusters forall (_ != NotVisited))

    val grouped = (
      (0 until pointClusters.length)
        .groupBy(pointClusters)
        .mapValues { idxs => idxs.map { idx => (idx, dataset(idx)) }.toMap }
    )

    val clusters = (grouped - Noise).values.toVector
    val noise    = grouped(Noise)
    DBSCAN.Result(clusters, noise)
  }

  def expandCluster(pIdx: Int, _neighborPts: IndexedSeq[Int], clusterId: Int) = {
    val neighborPts = scala.collection.mutable.Set[Int](_neighborPts: _*)
    pointClusters(pIdx) = clusterId

    while (!neighborPts.isEmpty) {
      val ppIdx = neighborPts.head
      neighborPts.remove(ppIdx)
      if (pointClusters(ppIdx) == NotVisited) {
        val newNeighborPts = regionQuery(ppIdx)
        if (newNeighborPts.size >= minPts) {
          //neighborPts ++= newNeighborPts
          newNeighborPts foreach { idx =>
            if (pointClusters(idx) == NotVisited) {
              neighborPts += idx
            }
          }
        }
      }
      if (pointClusters(ppIdx) < 0) { // P' is not yet member of any cluster
        pointClusters(ppIdx) = clusterId
      }
    }
  }

  /** Return all points within P's eps-neighborhood (including P).
    * If this method is overwriten, it must not return duplicate points. */
  def regionQuery(pIdx: Int): IndexedSeq[Int] = {
    val res = new collection.immutable.VectorBuilder[Int]
    val p = dataset(pIdx)

    var ppIdx = 0
    while (ppIdx < dataset.length) {
      if (dist(p, dataset(ppIdx)) <= eps) {
        res += ppIdx
      }
      ppIdx += 1
    }

    res.result
  }


}
