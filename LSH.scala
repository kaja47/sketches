package atrox.sketch

import atrox.{ fastSparse, IntFreqMap, Bits }
import java.lang.System.arraycopy
import java.lang.Long.{ bitCount, rotateLeft }
import java.util.Arrays
import java.util.concurrent.{ CopyOnWriteArrayList, ThreadLocalRandom }
import scala.util.hashing.MurmurHash3
import scala.collection.{ mutable, GenSeq }
import scala.collection.mutable.ArrayBuilder
import math.{ pow, log }


case class LSHBuildCfg(
  /** every bucket that has more than this number of elements is discarded */
  maxBucketSize: Int = Int.MaxValue,

  /** Every bucket that has less than this number of elements is discarded. It
    * might have sense to set this to 2 for closed datasets. It also filters
    * out a lot of tiny arrays which reduce memory usage. */
  minBucketSize: Int = 1,

  /** Determine how many bands are computed during one pass over data.
    * Multiple band groups can be execute in parallel.
    *
    * Increasing this number may lower available parallelisms. On the other
    * hand it increases locality or reference which may lead to better
    * performance but need to instantiate all sketchers and those might be
    * rather big (in case of RandomHyperplanes and RandomProjections)
    *
    * 1         - bands are computed one after other in multiple passes,
    *             if computeBandsInParallel is set, all of them are compute in parallel.
    * lsh.bands - all bands are computed in one pass over data
    *             This setting needs to instantiate all sketchers at once, but
    *             it's necessary when data source behave like an iterator.
    */
  bandsInOnePass: Int = 1,

  /** If this is set to true, program does one prior pass to count number of
    * values associated with every hash. It needs to do more work but allocates
    * only necessary amount of memory. */
  collectCounts: Boolean = false,

  computeBandsInParallel: Boolean = false
)

case class LSHCfg(
  /** Size of the biggest bucket that will be used for selecting candidates.
    * Elimitating huge buckets will not harm recall very much, because they
    * might be result of some hashing anomaly and their items are with high
    * probability present in another bucket. */
  maxBucketSize: Int = Int.MaxValue,

  /** How many candidates to select. */
  maxCandidates: Int = Int.MaxValue,

  /** How many final results to return. */
  maxResults: Int = Int.MaxValue,

  /** Forces LSH object to use only estimate to select top-k results.
    * This option has effect only for allSimilarItems methods called with
    * option maxResults set. */
  orderByEstimate: Boolean = false,

  /** If this is set to false, some operations might use faster code, that
    * needs to store all similar items in memory */
  compact: Boolean = true,

  /** Perform bulk query in parallel? */
  parallel: Boolean = false,

  // TODO
  parallelPartialResultSize: Double = 1.0
) {
  def accept(idxs: Array[Int]) = idxs != null && idxs.length <= maxBucketSize
}


case class Sim(a: Int, b: Int, estimatedSimilatity: Double, similarity: Double) {
  def this(a: Int, b: Int, estimatedSimilatity: Double) = this(a, b, estimatedSimilatity, estimatedSimilatity)
}


object LSH {

  def pickBands(threshold: Double, hashes: Int) = {
    require(threshold > 0.0)
    val target = hashes * -1 * log(threshold)
    var bands = 1
    while (bands * log(bands) < target) {
      bands += 1
    }
    bands
  }

  /** Picks best number of sketch hashes and LSH bands for given similarity
    * threshold */
  def pickHashesAndBands(threshold: Double, maxHashes: Int) = {
    val bands = pickBands(threshold, maxHashes)
    val hashes = (maxHashes / bands) * bands
    (hashes, bands)
  }


  // === BitLSH =================

  def apply(sk: BitSketching, bands: Int): BitLSH =
    apply(sk, bands, LSHBuildCfg())

  def apply(sk: BitSketching, bands: Int, cfg: LSHBuildCfg): BitLSH = {
    require(sk.sketchLength % 64 == 0 && bands > 0)

    val bandBits = sk.sketchLength / bands
    val bandMask = (1L << bandBits) - 1
    val bandSize = (1 << bandBits)
    val longsLen = sk.sketchLength / 64

    require(bandBits < 32)

    val idxs     = new Array[Array[Int]](bands * bandSize)
    val sketches = null//new Array[Array[Long]](bands * bandSize)

    for ((bs, bandGroup) <- bandGrouping(bands, cfg)) {

      val skslices = bs map { b => sk.slice(b * bandBits, (b+1) * bandBits) } toArray

      def runItems(f: (Int, Int) => Unit) = {
        val base = bs(0)
        val end = bs.last
        var i = 0 ; while (i < sk.length) {
          var b = base ; while (b <= end) {
            val h = skslices(b-base).getSketchFragmentAsLong(i, 0, bandBits).toInt
            assert(h <= bandSize)
            f((b - base) * bandSize + h, i)
            b += 1
          }
          i += 1
        }
      }

      val bandMap = if (cfg.collectCounts) {
        val counts = new Array[Int](cfg.bandsInOnePass * bandSize)
        runItems((h, i) => counts(h) += 1)
        Grouping(cfg.bandsInOnePass * bandSize, Int.MaxValue, counts) // Grouping.Counted
      } else {
        Grouping(cfg.bandsInOnePass * bandSize, cfg.bandsInOnePass * sk.length) // Grouping.Sorted
      }

      runItems((h, i) => bandMap.add(h, i))

      bandMap.getAll foreach { case (h, is) =>
        require(fastSparse.isDistinctIncreasingArray(is))
        if (is.length <= cfg.maxBucketSize && is.length >= cfg.minBucketSize) {
          idxs(bandGroup * bandSize * cfg.bandsInOnePass + h) = is
        }
      }
    }

    val _sk = sk match { case sk: BitSketch => sk ; case _ => null }
    new BitLSH(_sk, sk.estimator, LSHCfg(), idxs, sketches, sk.length, sk.sketchLength, bands, bandBits)
  }


  def ripBits(sketchArray: Array[Long], bitsPerSketch: Int, i: Int, band: Int, bandBits: Int): Int =
    Bits.getBitsOverlapping(sketchArray, bitsPerSketch * i + band * bandBits, bandBits).toInt


  // === IntLSH =================

  def apply(sk: IntSketching, bands: Int): IntLSH =
    apply(sk, bands, 32 - Integer.numberOfLeadingZeros(sk.length), LSHBuildCfg())

  def apply(sk: IntSketching, bands: Int, hashBits: Int): IntLSH =
    apply(sk, bands, hashBits, LSHBuildCfg())

  def apply(sk: IntSketching, bands: Int, hashBits: Int, cfg: LSHBuildCfg): IntLSH = {
    require(bands > 0 && hashBits > 0)

    val bandElements = sk.sketchLength / bands // how many elements from sketch is used in one band
    val hashMask = (1 << hashBits) - 1
    val bandSize = (1 << hashBits) // how many hashes are possible in one band

    val idxs     = new Array[Array[Int]](bands * bandSize)
    val sketches = null//new Array[Array[Long]](bands * bandSize)

    for ((bs, bandGroup) <- bandGrouping(bands, cfg)) {

      val scratchpads = Array.ofDim[Int](cfg.bandsInOnePass, bandElements)
      val skslices    = bs map { b => sk.slice(b * bandElements, (b+1) * bandElements) } toArray

      def runItems(f: (Int, Int) => Unit) = {
        val base = bs(0)
        val end = bs.last
        var i = 0 ; while (i < sk.length) {
          var b = base ; while (b <= end) {
            val h = if (sk.isInstanceOf[IntSketch]) {
              // if sk is IntSketch instance directly access it's internal sketch array, this saves some copying
              val _sk = sk.asInstanceOf[IntSketch]
              hashSlice(_sk.sketchArray, _sk.sketchLength, i, b, bandElements, hashBits)
            } else {
              skslices(b-base).writeSketchFragment(i, scratchpads(b-base), 0)
              hashSlice(scratchpads(b-base), bandElements, 0, 0, bandElements, hashBits)
            }
            f((b - base) * bandSize + h, i)
            b += 1
          }
          i += 1
        }
      }

      val bandMap = if (cfg.collectCounts) {
        val counts = new Array[Int](cfg.bandsInOnePass * bandSize)
        runItems((h, i) => counts(h) += 1)
        Grouping(cfg.bandsInOnePass * bandSize, Int.MaxValue, counts) // Grouping.Counted
      } else {
        Grouping(cfg.bandsInOnePass * bandSize, cfg.bandsInOnePass * sk.length) // Grouping.Sorted
      }

      runItems((h, i) => bandMap.add(h, i))

      bandMap.getAll foreach { case (h, is) =>
        require(fastSparse.isDistinctIncreasingArray(is))
        if (is.length <= cfg.maxBucketSize && is.length >= cfg.minBucketSize) {
          idxs(bandGroup * bandSize * cfg.bandsInOnePass + h) = is
        }
      }
    }

    val _sk = sk match { case sk: IntSketch => sk ; case _ => null }
    new IntLSH(_sk, sk.estimator, LSHCfg(), idxs, sketches, sk.length, sk.sketchLength, bands, bandElements, hashBits)
  }

  def hashSlice(sketchArray: Array[Int], sketchLength: Int, i: Int, band: Int, bandLength: Int, hashBits: Int) = {
    val start = i * sketchLength + band * bandLength
    val end   = start + bandLength
    _hashSlice(sketchArray, start, end) & ((1 << hashBits) - 1)
  }


  /** based on scala.util.hashing.MurmurHash3.arrayHash */
  private def _hashSlice(arr: Array[Int], start: Int, end: Int): Int = {
    var h = MurmurHash3.arraySeed
    var i = start
    while (i < end) {
      h = MurmurHash3.mix(h, arr(i))
      i += 1
    }
    MurmurHash3.finalizeHash(h, end-start)
  }

  // band groups -> (band indexes, band group index)
  def bandGrouping(bands: Int, cfg: LSHBuildCfg): GenSeq[(Seq[Int], Int)] = {
    val bss = (0 until bands grouped cfg.bandsInOnePass).zipWithIndex.toSeq ;
    if (!cfg.computeBandsInParallel) bss else bss.par
  }

}


/** LSH configuration
  *
  * LSH, full Sketch
  * LSH, empty Sketch, embedded sketches
  *  - embedded sketches greatly increase memory usage but can make search
  *    faster because they provide better locality
  * LSH, empty Sketch, no embedded sketches
  *  - can only provide list of candidates, cannot estimate similarities
  *
  * - Methods with "raw" prefix might return duplicates and are not affected by
  *   maxBucketSize, maxCandidates and maxResults config options.
  * - Methods without "raw" prefix must never return duplicates and queried item itself
  *
  * idxs must be sorted
  */
abstract class LSH { self =>
  type SketchArray
  type Sketching <: atrox.sketch.Sketching[T] forSome { type T <: SketchArray}
  type Sketch <: atrox.sketch.Sketch[T] forSome { type T <: SketchArray}
  type Idxs = Array[Int]
  type SimFun = (Int, Int) => Double

  def sketch: Sketch
  def length: Int
  def estimator: Estimator[SketchArray]
  def cfg: LSHCfg
  def bands: Int

  protected def getSketchArray: SketchArray = if (sketch != null) sketch.sketchArray else null.asInstanceOf[SketchArray]

  def withConfig(cfg: LSHCfg): LSH

  def probabilityOfInclusion(sim: Double) = {
    val bandLen = estimator.sketchLength / bands
    1.0 - pow(1.0 - pow(sim, bandLen), bands)
  }

  val estimatedThreshold = {
    val bandLen = estimator.sketchLength / bands
    pow(1.0 / bands, 1.0 / bandLen)
  }

  trait Query[Res] {
    def apply(sketch: SketchArray, idx: Int, minEst: Double, minSim: Double, f: SimFun): Res
    def apply(sketch: SketchArray, idx: Int, minEst: Double): Res =
      apply(sketch, idx, minEst, 0.0, null)
    def apply(sketch: Sketching, idx: Int, minEst: Double): Res =
      apply(sketch.getSketchFragment(idx), 0, minEst, 0.0, null)
    def apply(sketch: Sketching, idx: Int, minEst: Double, minSim: Double, f: SimFun): Res =
      apply(sketch.getSketchFragment(idx), 0, minEst, minSim, f)

    def apply(idx: Int, minEst: Double, minSim: Double, f: SimFun): Res = {
      require(idx < 0 || idx >= length, s"index $idx is out of range (0 until $length)")
      apply(getSketchArray, idx, minEst, minSim, f)
    }
    def apply(idx: Int, minEst: Double): Res = apply(idx, minEst, 0.0, null)
  }

  // =====

  def rawStream: Iterator[(Idxs, SketchArray)]
  def rawStreamIndexes: Iterator[Idxs]
  /** same as rawStreamIndexes, but arrays are limited by maxBucketSize */
  def streamIndexes: Iterator[Idxs] = rawStreamIndexes filter cfg.accept

  def rawCandidates(sketch: SketchArray, idx: Int): Array[(Idxs, SketchArray)]
  def rawCandidates(sketch: Sketching, idx: Int): Array[(Idxs, SketchArray)] = rawCandidates(sketch.getSketchFragment(idx), 0)
  def rawCandidates(idx: Int): Array[(Idxs, SketchArray)] = rawCandidates(sketch.sketchArray, idx)

  def rawCandidateIndexes(sketch: SketchArray, idx: Int): Array[Idxs]
  def rawCandidateIndexes(sketch: Sketching, idx: Int): Array[Idxs] = rawCandidateIndexes(sketch.getSketchFragment(idx), 0)
  def rawCandidateIndexes(idx: Int): Array[Idxs] = rawCandidateIndexes(sketch.sketchArray, idx)

  def candidateIndexes(sketch: SketchArray, idx: Int): Idxs = fastSparse.union(rawCandidateIndexes(sketch, idx))
  def candidateIndexes(sketch: Sketching, idx: Int): Idxs = candidateIndexes(sketch.getSketchFragment(idx), 0)
  def candidateIndexes(idx: Int): Idxs = candidateIndexes(sketch.sketchArray, idx)

  // Following methods need valid sketch object.
  // If minEst is set to Double.NegativeInfinity, no estimates are computed.
  // Instead candidates are directly filtered through similarity function.
  val rawSimilarIndexes = new Query[Idxs] {
    def apply(sketch: SketchArray, idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs = {
      val res = new ArrayBuilder.ofInt

      if (isNegInf(minEst)) {
        requireSimFun(f)
        for (idxs <- rawCandidateIndexes(sketch, idx)) {
          var i = 0 ; while (i < idxs.length) {
            if (f(idxs(i), idx) >= minSim) { res += idxs(i) }
            i += 1
          }
        }

      } else {
        val minBits = estimator.minSameBits(minEst)
        for (idxs <- rawCandidateIndexes(sketch, idx)) {
          var i = 0 ; while (i < idxs.length) {
            val bits = estimator.sameBits(sketch, idxs(i), sketch, idx)
            if (bits >= minBits && (f == null || f(idxs(i), idx) >= minSim)) { res += idxs(i) }
            i += 1
          }
        }

      }

      res.result
    }
  }

  // Following methods need valid sketch object
  // similarIndexes().toSet == (rawSimilarIndexes().distinct.toSet - idx)
  def similarIndexes = new Query[Idxs] {
    def apply(sketch: SketchArray, idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs = {
      val candidates = selectCandidates(idx)
      val res = newIndexResultBuilder()

      if (isNegInf(minEst)) {
        requireSimFun(f)
        var i = 0 ; while (i < candidates.length) {
          var sim = 0.0
          if (idx != candidates(i) && { sim = f(candidates(i), idx) ; sim >= minSim }) {
            res += (candidates(i), sim)
          }
          i += 1
        }

      } else {
        val minBits = estimator.minSameBits(minEst)
        var i = 0 ; while (i < candidates.length) {
          val bits = estimator.sameBits(sketch, candidates(i), sketch, idx)
          var sim = 0.0
          if (bits >= minBits && idx != candidates(i) && (f == null || { sim = f(candidates(i), idx) ; sim >= minSim })) {
            res += (candidates(i), if (f == null) estimator.estimateSimilarity(bits) else sim)
          }
          i += 1
        }
      }

      res.result
    }
  }

  // Following methods need valid sketch object
  def similarItems = new Query[Iterator[Sim]] {
    def apply(sketch: SketchArray, idx: Int, minEst: Double, minSim: Double, f: SimFun): Iterator[Sim] = {
      // Estimates and similarities are recomputed once more for similar items.
      val ff = if (cfg.orderByEstimate) null else f
      val simIdxs = similarIndexes(sketch, idx, minEst, minSim, ff)
      indexesToSims(idx, simIdxs, f, sketch, isNegInf(minEst))
    }
  }

  /** Needs to store all similar indexes in memory + some overhead, but it's
    * much faster, because it needs to do only half of iterations and accessed
    * data have better cache locality. */
  protected def _allSimilarIndexes_notCompact(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, Idxs)] = {

    // maxCandidates option is simulated via sampling
    val comparisons = streamIndexes.map { idxs => (idxs.length - 1) * idxs.length / 2 } sum
    val ratio = 1.0 * length * cfg.maxCandidates / comparisons

    if (isNegInf(minSim)) requireSimFun(f)

    val res =
      if (cfg.parallel) {
        val partialResults = new CopyOnWriteArrayList[Array[IndexResultBuilder]]()
        val tl = new ThreadLocal[Array[IndexResultBuilder]] {
          override def initialValue = {
            val local = Array.fill(length)(newIndexResultBuilder(distinct = true))
            partialResults.add(local)
            local
          }
        }

        streamIndexes.grouped(1024).toVector.par.foreach { idxsgr =>
          val local = tl.get
          for (idxs <- idxsgr) {
            runTile(idxs, ratio, minEst, minSim, f, local)
          }
        }

        val pr = partialResults.toArray(Array[Array[IndexResultBuilder]]())
        val res = pr.head // reuse the first partial result as an accumulator
        pr.tail.par foreach { local =>
          var i = 0 ; while (i < local.length) {
            res(i).synchronized { res(i) ++= local(i) }
            i += 1
          }
        }
        res

      } else {
        val res = Array.fill(length)(newIndexResultBuilder(distinct = true))
        for (idxs <- streamIndexes) {
          runTile(idxs, ratio, minEst, minSim, f, res)
        }
        res
      }

    Iterator.tabulate(length) { idx => (idx, res(idx).result) }
  }

  protected def runTile(idxs: Idxs, ratio: Double, minEst: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]) =
    if (isNegInf(minEst)) {
      runTileNoEstimate(idxs, ratio, minSim, f, res)
    } else {
      runTileYesEstimate(idxs, ratio, minEst, minSim, f, res)
    }

  protected def runTileNoEstimate(idxs: Idxs, ratio: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]) = {
    var i = 0 ; while (i < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        var j = i+1 ; while (j < idxs.length) {
          var sim = 0.0
          if ({ sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
            res(idxs(i)) += (idxs(j), sim)
            res(idxs(j)) += (idxs(i), sim)
          }
          j += 1
        }
      }
      i += 1
    }
  }

  protected def runTileYesEstimate(idxs: Idxs, ratio: Double, minEst: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]) = {
    val minBits = sketch.minSameBits(minEst)
    val skarr = getSketchArray
    var i = 0 ; while (i < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        var j = i+1 ; while (j < idxs.length) {
          val bits = estimator.sameBits(skarr, idxs(i), skarr, idxs(j))
          var sim = 0.0
          if (bits >= minBits && (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim })) {
            sim = if (f == null) estimator.estimateSimilarity(bits) else sim
            res(idxs(i)) += (idxs(j), sim)
            res(idxs(j)) += (idxs(i), sim)
          }
          j += 1
        }
        i += 1
      }
    }
  }


  def allSimilarIndexes(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, Idxs)] =
    (cfg.compact, cfg.parallel) match {
      case (true, false) => Iterator.tabulate(sketch.length) { idx => (idx, similarIndexes(idx, minEst, minSim, f)) }
      case (true, true)  => (0 until sketch.length grouped 1024) flatMap { idxs => idxs.par.map { idx => (idx, similarIndexes(idx, minEst, minSim, f)) } }
      case (false, _)    => _allSimilarIndexes_notCompact(minEst, minSim, f)
    }
  def allSimilarIndexes(minEst: Double): Iterator[(Int, Idxs)] =
    allSimilarIndexes(minEst, 0.0, null)

  def allSimilarItems(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, Iterator[Sim])] = {
    val ff = if (cfg.orderByEstimate) null else f
    allSimilarIndexes(minEst, minSim, ff) map { case (idx, simIdxs) => (idx, indexesToSims(idx, simIdxs, f, getSketchArray, isNegInf(minEst))) }
  }
  def allSimilarItems(minEst: Double): Iterator[(Int, Iterator[Sim])] =
    allSimilarItems(minEst, 0.0, null)

  def rawSimilarItemsStream(minEst: Double, minSim: Double, f: SimFun): Iterator[Sim] = {
    if (isNegInf(minEst)) {
      requireSimFun(f)
      rawStreamIndexes flatMap { idxs =>
        val res = collection.mutable.ArrayBuffer[Sim]()
        var i = 0 ; while (i < idxs.length) {
          var j = i+1 ; while (j < idxs.length) {
            var sim: Double = 0.0
            if (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
              res += Sim(idxs(i), idxs(j), 0.0, sim)
            }
            j += 1
          }
          i += 1
        }
        res
      }

    } else {
      val minBits = sketch.minSameBits(minEst)
      val skarr = getSketchArray
      rawStreamIndexes flatMap { idxs =>
        val res = collection.mutable.ArrayBuffer[Sim]()
        var i = 0 ; while (i < idxs.length) {
          var j = i+1 ; while (j < idxs.length) {
            val bits = estimator.sameBits(skarr, idxs(i), skarr, idxs(j))
            if (bits >= minBits) {
              var sim: Double = 0.0
              if (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
                res += Sim(idxs(i), idxs(j), estimator.estimateSimilarity(bits), sim)
              }
            }
            j += 1
          }
          i += 1
        }
        res
      }
    }
  }
  def rawSimilarItemsStream(minEst: Double): Iterator[Sim] = rawSimilarItemsStream(minEst, 0.0, null)


  /** must never return duplicates */
  def selectCandidates(idx: Int): Idxs = {
    // TODO candidate selection can be done on the fly without allocations
    //      using some sort of specialized merging iterator
    val rci = rawCandidateIndexes(getSketchArray, idx)
    val candidateCount = rci.iterator.map(_.length).sum

    if (candidateCount <= cfg.maxCandidates) {
      fastSparse.union(rci)

    } else {
      val map = new IntFreqMap(initialSize = cfg.maxCandidates, loadFactor = 0.42, freqThreshold = bands)
      for (is <- rci) { map ++= (is, 1) }
      map.topK(cfg.maxCandidates)
    }
  }

  protected def newIndexResultBuilder(distinct: Boolean = false): IndexResultBuilder =
    IndexResultBuilder.make(distinct, cfg.maxResults)

  protected def indexesToSims(idx: Int, simIdxs: Idxs, f: SimFun, sketch: SketchArray, noEstimates: Boolean) =
    if (noEstimates) {
      simIdxs.iterator.map { simIdx => Sim(idx, simIdx, 0.0, f(idx, simIdx)) }
    } else {
      simIdxs.iterator.map { simIdx =>
        val est = estimator.estimateSimilarity(sketch, idx, sketch, simIdx)
        val sim = if (f == null) est else f(idx, simIdx)
        Sim(idx, simIdx, est, sim)
      }
    }

    protected def isNegInf(d: Double) = d == Double.NegativeInfinity
    protected def requireSimFun(f: SimFun) = require(f != null, "similarity function is required")

}




/** bandLengh - how many elements in one band
  * hashBits  - how many bits of a hash is used (2^hashBits should be roughly equal to number of items)
  */
final case class IntLSH(
    sketch: IntSketch, estimator: IntEstimator, cfg: LSHCfg,
    idxs: Array[Array[Int]], sketches: Array[Array[Int]],
    length: Int,
    sketchLength: Int, bands: Int, bandLength: Int, hashBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Int]
  type Sketching = IntSketching
  type Sketch = IntSketch

  if (sketches != null) {
    require(idxs.length == sketches.length)
  }

  def withConfig(newCfg: LSHCfg): IntLSH = copy(cfg = newCfg)

  def bandHashes(sketchArray: SketchArray, idx: Int): Iterator[Int] =
    Iterator.tabulate(bands) { b => bandHash(sketchArray, idx, b) }

  def bandHash(sketchArray: SketchArray, idx: Int, band: Int): Int =
    LSH.hashSlice(sketchArray, sketch.sketchLength, idx, band, bandLength, hashBits)

  def rawStream: Iterator[(Idxs, SketchArray)] = (idxs.iterator zip sketches.iterator).filter(_._1 != null)
  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)


  def rawCandidates(sketch: SketchArray, idx: Int): Array[(Idxs, SketchArray)] =
    Array.tabulate(bands) { b =>
      val h = LSH.hashSlice(sketch, sketchLength, idx, b, bandLength, hashBits)
      val bucket = b * (1 << hashBits) + h
      (idxs(bucket), if (sketches == null) null else sketches(bucket))
    }.filter { case (idxs, _) => cfg.accept(idxs) }


  def rawCandidateIndexes(sketch: SketchArray, idx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.hashSlice(sketch, sketchLength, idx, b, bandLength, hashBits)
      val bucket = b * (1 << hashBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }


}




final case class BitLSH(
    sketch: BitSketch, estimator: BitEstimator, cfg: LSHCfg,
    idxs: Array[Array[Int]], sketches: Array[Array[Long]],
    length: Int,
    bitsPerSketch: Int, bands: Int, bandBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Long]
  type Sketching = BitSketching
  type Sketch = BitSketch

  if (sketches != null) {
    require(idxs.length == sketches.length)
  }

  def withConfig(newCfg: LSHCfg): BitLSH = copy(cfg = newCfg)

  def bandHashes(sketchArray: Array[Long], idx: Int): Iterator[Int] =
    Iterator.tabulate(bands) { b => LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandBits) }

  def rawStream: Iterator[(Idxs, SketchArray)] = (idxs.iterator zip sketches.iterator).filter(_._1 != null)
  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)


  def rawCandidates(sketch: SketchArray, idx: Int): Array[(Idxs, SketchArray)] =
    Array.tabulate(bands) { b =>
      val h = LSH.ripBits(sketch, bitsPerSketch, idx, b, bandBits)
      val bucket = b * (1 << bandBits) + h
      (idxs(bucket), if (sketches == null) null else sketches(bucket))
    }.filter { case (idxs, _) => cfg.accept(idxs) }


  def rawCandidateIndexes(sketch: SketchArray, idx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.ripBits(sketch, bitsPerSketch, idx, b, bandBits)
      val bucket = b * (1 << bandBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }
}
