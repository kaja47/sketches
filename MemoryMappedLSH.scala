package atrox.sketch

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import atrox.fastSparse


abstract class MemoryMappedLSHTable[SketchArray](val mm: IntBuffer) extends LSHTable[SketchArray] {

  val params = LSHTableParams(
    sketchLength = mm.get(0),
    bands        = mm.get(1),
    bandLength   = mm.get(2),
    hashBits     = mm.get(3),
    itemsCount   = mm.get(4)
  )

  private val tableLength = mm.get(5)

  require(params.bands * (1 << params.hashBits) == tableLength)

  private def headerSize = 6

  def rawStreamIndexes: Iterator[Idxs] = Iterator.tabulate(tableLength)(idxs) filter (arr => arr != null && arr.length != 0)

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs =
    idxs(band * (1 << params.hashBits) + hashFun(skarr, skidx, band, params))

  protected def idxs(idx: Int) = {
    require(idx < tableLength, s"idx < tableLength ($idx < $tableLength)")

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

  override def toString = s"MemoryMappedLSHTable($mm, params = $params, tableLength = $tableLength)"
}


object MemoryMappedLSHTable {

  def mmapTable(fileName: String): IntBuffer = {
    val chan = new RandomAccessFile(fileName, "r")
      .getChannel()

    val len = chan.size()

    chan
      .map(FileChannel.MapMode.READ_ONLY, 0, len)
      .asIntBuffer
      .asReadOnlyBuffer
  }

  def mmap[SketchArray](fileName: String, es: Estimator[SketchArray])(implicit has: HashAndSlice[SketchArray]): MemoryMappedLSHTable[SketchArray] =
    new MemoryMappedLSHTable[SketchArray](mmapTable(fileName)) {
      def hashFun = has.hashFun
      def sliceFun = has.sliceFun
    }

  def persist[SketchArray](table: IntArrayLSHTable[SketchArray], fileName: String): Unit = {
    // sketchLength | bands | bandLength | hashBits | itemsCount | idxs length | offsets ... + offset behind the last array | arrays

    val len = lengthOfTable(table)
    if (len > Int.MaxValue) throw new Exception("too long")

    val mmf = mmapFile(fileName, len.toInt) 
    val b = mmf.asIntBuffer

    b.put(table.params.sketchLength)
    b.put(table.params.bands)
    b.put(table.params.bandLength)
    b.put(table.params.hashBits)
    b.put(table.params.itemsCount)
    b.put(table.idxs.length)

    var off = 6 + table.idxs.length + 1

    for (arr <- table.idxs) {
      b.put(off)
      off += arrLen(arr)
    }

    b.put(off)

    for (arr <- table.idxs) {
      if (arr != null) {
        b.put(arr)
      }
    }

    mmf.force()

  }

  protected def lengthOfTable[SketchArray](table: IntArrayLSHTable[SketchArray]) =
    (6 + table.idxs.length.toLong + 1 + table.idxs.map(arrLen).sum) * 4

  protected def mmapFile(fileName: String, length: Int) =
    new RandomAccessFile(fileName, "rw")
      .getChannel()
      .map(FileChannel.MapMode.READ_WRITE, 0, length)

  private def arrLen(arr: Array[Int]) = if (arr == null) 0 else arr.length

}
