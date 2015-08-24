package atrox

import breeze.linalg._
import breeze.numerics._
import breeze.util.HashIndex
//import collection.mutable.PriorityQueue
import java.util.PriorityQueue
import scala.collection.JavaConverters._
import collection.mutable.ArrayBuffer


object OPTICS {

	def OPTICS[Point](dataset: IndexedSeq[Point], eps: Double, minPts: Int, dist: (Point, Point) => Double): IndexedSeq[(Point, Int, Double)] = {

		type PointIdxDist = (Point, Int, Double)

		val UNDEFINED = -1.0
		val reachabilityDistance = Array.fill(dataset.size)(UNDEFINED)
		val processed = Array.fill(dataset.size)(false)

		def run = {
			val orderedList = ArrayBuffer[Int]()

			for ((p, pIdx) <- dataset.zipWithIndex if !processed(pIdx)) {
				processed(pIdx) = true
				orderedList += pIdx
				val seeds = new PriorityQueue[PointIdxDist](16, Ordering.by(_._3))
				val neighbors = getNeighbors(p)
				if (coreDistance(p, neighbors) != UNDEFINED) {
					update(neighbors, p, seeds)

					while (!seeds.isEmpty) {
						val (q, qIdx, dist) = seeds.poll()
						processed(qIdx) = true
						orderedList += qIdx
						val newNeighbors = getNeighbors(q)
						if (coreDistance(q, newNeighbors) != UNDEFINED) {
							update(newNeighbors, q, seeds)
						}
					}
				}
			}
			orderedList.toVector map { idx => (dataset(idx), idx, reachabilityDistance(idx)) }
		}

		def update(neighbors: IndexedSeq[PointIdxDist], p: Point, seeds: PriorityQueue[PointIdxDist]): Unit = {
			val coredist = coreDistance(p, neighbors)
			for (old @ (o, oIdx, oDist) <- neighbors if !processed(oIdx)) {
				val newReachDist = math.max(coredist, dist(p, o))
				if (reachabilityDistance(oIdx) == UNDEFINED) { // o is not in seeds
					reachabilityDistance(oIdx) = newReachDist
					seeds.add((o, oIdx, newReachDist))
				} else if (newReachDist < reachabilityDistance(oIdx)) { 
					reachabilityDistance(oIdx) = newReachDist
					val el = seeds.iterator.asScala.find(_._2 == oIdx).get
					seeds.remove(el)
					seeds.add((o, oIdx, newReachDist))
				}
			}
		}

		def getNeighbors(p: Point): IndexedSeq[PointIdxDist] =
			(for {
				(pp, ppIdx) <- dataset.zipWithIndex
				d = dist(p, pp)
				if d <= eps
			} yield (pp, ppIdx, d)).toVector.sortBy(_._3)

		// `neighbors` must be sorted by distance from `p`
		def coreDistance(p: Point, neighbors: IndexedSeq[PointIdxDist]): Double = {
			if (neighbors.size < minPts) UNDEFINED
			else neighbors(minPts-1)._3
		}

		run

	}
}
