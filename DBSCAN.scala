package atrox

import breeze.linalg._
import breeze.numerics._
import breeze.util.HashIndex
import scala.specialized
import scala.reflect.ClassTag


object DBSCAN {
	def apply[@specialized(Int, Long) Point: ClassTag](dataset: IndexedSeq[Point], eps: Double, minPts: Int, dist: (Point, Point) => Double): (IndexedSeq[Map[Int, Point]], Map[Int, Point]) =
		new DBSCAN(dataset, eps, minPts, dist).run
}


class DBSCAN[@specialized(Int, Long) Point: ClassTag](dataset: IndexedSeq[Point], val eps: Double, val minPts: Int, val dist: (Point, Point) => Double) {

	//val datasetArr = dataset.asInstanceOf[IdexedSeq[AnyRef]].toArray.asInstanceOf[Array[Point]]
	val datasetArr = dataset.toArray

	val NotVisited = -1
	val Noise = -2

	// NotVisited, Noise, clutserId
	val pointClusters = Array.fill(datasetArr.size)(NotVisited)

	// @return seq of clusters and seq of points without cluster
	def run: (IndexedSeq[Map[Int, Point]], Map[Int, Point]) = {
		var currentClusterId = 0
		for {
			pIdx <- 0 until datasetArr.size
			if (pointClusters(pIdx) == NotVisited)
		} {
			val neighborPts = regionQuery(pIdx)
			if (neighborPts.size < minPts) {
				pointClusters(pIdx) = Noise
				currentClusterId += 1
			} else {
				expandCluster(pIdx, neighborPts, currentClusterId)
			}
		}

		assert(pointClusters forall (_ != NotVisited))

		val inClusters = groupByKey(pointClusters.zipWithIndex filter { 
			case (cluster, idx) => cluster >= 0
		} map {
			case (cluster, idx) => (cluster, (idx, datasetArr(idx)))
		}).values.toIndexedSeq.map(_.toMap)

		val noise = pointClusters.zipWithIndex filter { 
			case (cluster, idx) => cluster == Noise
		} map {
			case (cluster, idx) => (idx, datasetArr(idx))
		} toMap

		(inClusters, noise)
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
		var ppIdx = 0
		val res = new collection.immutable.VectorBuilder[Int]
		val p = datasetArr(pIdx)

		while (ppIdx < datasetArr.length) {
			if (dist(p, datasetArr(ppIdx)) <= eps) {
				res += ppIdx
			}
			ppIdx += 1
		}
		
		res.result
	}

	def groupByKey[K, V](xs: Iterable[(K, V)]): Map[K, Seq[V]] =
		xs groupBy (_._1) map { case (k, xs) => (k, xs map (_._2) toSeq) }

}
