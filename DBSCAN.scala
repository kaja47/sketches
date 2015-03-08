package atrox

import breeze.linalg._
import breeze.numerics._
import breeze.util.HashIndex


object DBSCAN {

	// @return seq of clusters and seq of points without cluster
	def DBSCAN[Point](dataset: IndexedSeq[Point], eps: Double, minPts: Int, dist: (Point, Point) => Double): (IndexedSeq[Map[Int, Point]], Map[Int, Point]) = {


		val NotVisited = -1
		val Noise = -2

		// NotVisited, Noise, clutserId
		val pointClusters = Array.fill(dataset.size)(NotVisited)
	
		def run() = {
			var currentClusterId = 0
			for {
				(p, pIdx) <- dataset.zipWithIndex
				if (pointClusters(pIdx) == NotVisited)
			} {
				val neighborPts = regionQuery(p)
				if (neighborPts.size < minPts) {
					//println("Noise "+p)
					pointClusters(pIdx) = Noise
					currentClusterId += 1
				} else {
					expandCluster(pIdx, neighborPts, currentClusterId)
				}
			}
		}

		def expandCluster(pIdx: Int, _neighborPts: IndexedSeq[(Int, Point)], clusterId: Int) = {
			val neighborPts = scala.collection.mutable.Queue[(Int, Point)](_neighborPts: _*)
			pointClusters(pIdx) = clusterId

			while (!neighborPts.isEmpty) {
				val (ppIdx, pp) = neighborPts.dequeue()
				if (pointClusters(ppIdx) == NotVisited) {
					val newNeighborPts = regionQuery(pp)
					if (newNeighborPts.size >= minPts) {
						neighborPts.enqueue(newNeighborPts: _*)
					}
				}
				if (pointClusters(ppIdx) < 0) //P' is not yet member of any cluster
					pointClusters(ppIdx) = clusterId
			}
		}

		// return all points within P's eps-neighborhood (including P)
		def regionQuery(p: Point): IndexedSeq[(Int, Point)] =
			(for {
				(pp, ppIdx) <- dataset.zipWithIndex//.par
				d = dist(p, pp)
				if d <= eps
			} yield (ppIdx, pp)).toVector

		run()

		assert(pointClusters forall (_ != NotVisited))

		val inClusters = groupByKey(pointClusters.zipWithIndex filter { 
			case (cluster, idx) => cluster >= 0
		} map {
			case (cluster, idx) => (cluster, (idx, dataset(idx)))
		}).values.toIndexedSeq.map(_.toMap)

		val noise = pointClusters.zipWithIndex filter { 
			case (cluster, idx) => cluster == Noise
		} map {
			case (cluster, idx) => (idx, dataset(idx))
		} toMap

		(inClusters, noise)
	}

	def groupByKey[K, V](xs: Iterable[(K, V)]): Map[K, Seq[V]] =
		xs groupBy (_._1) map { case (k, xs) => (k, xs map (_._2) toSeq) }
}
