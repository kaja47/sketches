package atrox.sketch

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import atrox.fastSparse


final case class MemoryMappedIntLSH(
    estimator: IntEstimator, cfg: LSHCfg,
    mm: IntBuffer,
    reverseIdxs: Array[Array[Int]] = null
  ) extends LSH {

  override def toString = s"MemoryMappedIntLSH(sketchLength = $sketchLength, bands = $bands, bandLength = $bandLength, hashBits = $hashBits, itemsCount = $itemsCount, tableLength = $tableLength)"

  type SketchArray = Array[Int]
  type Sketching = IntSketching

  val sketch = null

  val sketchLength = mm.get(0)
  val bands        = mm.get(1)
  val bandLength   = mm.get(2)
  val hashBits     = mm.get(3)
  val itemsCount   = mm.get(4)
  private val tableLength = mm.get(5)

  private def headerSize = 6

  def withConfig(newCfg: LSHCfg): MemoryMappedIntLSH = copy(cfg = newCfg)
  def withReverseMapping = copy(reverseIdxs = LSH.makeReverseMapping(itemsCount, bands, idxs, tableLength))
  def hasReverseMapping = reverseIdxs != null

  def bandHashes(sketchArray: SketchArray, idx: Int): Iterator[Int] =
    Iterator.tabulate(bands) { b => bandHash(sketchArray, idx, b) }

  def bandHash(sketchArray: SketchArray, idx: Int, band: Int): Int =
    LSH.hashSlice(sketchArray, sketchLength, idx, band, bandLength, hashBits)

  def rawStreamIndexes: Iterator[Idxs] = Iterator.tabulate(itemsCount)(idxs) filter (arr => arr != null && arr.length != 0)

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.hashSlice(skarr, sketchLength, skidx, b, bandLength, hashBits)
      val bucket = b * (1 << hashBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }

  override def rawCandidateIndexes(idx: Int): Array[Idxs] =
    reverseIdxs(idx) map { bucket => idxs(bucket) }


  private def idxs(idx: Int): Array[Int] = {
    require(idx < tableLength, s"idx < itemsCount ($idx < $itemsCount)")

    val start = mm.get(headerSize + idx)
    val end   = mm.get(headerSize + idx + 1)
    val len = end-start

    val res = new Array[Int](len)

    for (i <- 0 until len) {
      res(i) = mm.get(start+i)
    }

    require(fastSparse.isDistinctIncreasingArray(res))

    res
  }
}

object MemoryMappedIntLSH {

  def mmap(fileName: String, estimator: IntEstimator, cfg: LSHCfg = LSHCfg()): MemoryMappedIntLSH = {

    val chan = new RandomAccessFile(fileName, "r")
      .getChannel()

    val len = chan.size()

    val mm = chan
      .map(FileChannel.MapMode.READ_ONLY, 0, len)
      .asIntBuffer
      .asReadOnlyBuffer

    MemoryMappedIntLSH(estimator, cfg, mm)
  }

  def persist(lsh: IntLSH, fileName: String): Unit = {
    // sketchLength | bands | bandLength | hashBits | itemsCount | idxs length | offsets ... + offset behind the last array | arrays
    def arrLen(arr: Array[Int]) = if (arr == null) 0 else arr.length

    val len = (6 + lsh.idxs.length.toLong + 1 + lsh.idxs.map(arrLen).sum) * 4
    if (len > Int.MaxValue) throw new Exception("too long")

    val mmf = new RandomAccessFile(fileName, "rw")
      .getChannel()
      .map(FileChannel.MapMode.READ_WRITE, 0, len)

    val b = mmf.asIntBuffer

    b.put(lsh.sketchLength)
    b.put(lsh.bands)
    b.put(lsh.bandLength)
    b.put(lsh.hashBits)
    b.put(lsh.itemsCount)
    b.put(lsh.idxs.length)

    var off = 6 + lsh.idxs.length + 1

    for (arr <- lsh.idxs) {
      b.put(off)
      off += arrLen(arr)
    }

    b.put(off)

    for (arr <- lsh.idxs) {
      if (arr != null) {
        b.put(arr)
      }
    }

    mmf.force()

  }

}
