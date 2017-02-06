package atrox.sketch

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.IntBuffer
import java.nio.channels.FileChannel
import atrox.fastSparse



/*
object MmapIntArrArr {
  def persist(arr: Array[Array[Int]])
}

class MmapIntArrArr(bb: ByteBuffer) {

  val ib = bb.asIntBuffer

  val tableLength = ib.get(0)

  val offsets = ib.position(1).limit(1+tableLength).slice()
  val data    = ib.position(1+tableLength).slice()



  val start = offsets.get(idx)
  val end   = offsets.get(idx + 1)
  val len = end-start

  val res = new Array[Int](len)

  for (i <- 0 until len) {
    res(i) = data.get(i)
  }

  require(fastSparse.isDistinctIncreasingArray(res))

  res

}
*/



abstract class MMCommon[SketchArray] extends LSHTable[SketchArray] {
  protected def tableLength: Int

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs =
    idxs(band * (1 << params.hashBits) + hashAndSlice.hashFun(skarr, skidx, band, params))

  def rawStreamIndexes: Iterator[Idxs] = Iterator.tabulate(tableLength)(idxs) filter (arr => arr != null && arr.length != 0)

  def decodeParams(mm: IntBuffer, baseOffset: Int) =
    LSHTableParams(
      sketchLength = mm.get(baseOffset + 0),
      bands        = mm.get(baseOffset + 1),
      bandLength   = mm.get(baseOffset + 2),
      hashBits     = mm.get(baseOffset + 3),
      itemsCount   = mm.get(baseOffset + 4)
    )

  protected def idxs(idx: Int): Array[Int]
}

abstract class MemoryMappedLSHTable[SketchArray](val mm: IntBuffer) extends MMCommon[SketchArray] {

  val params = decodeParams(mm, 0)

  protected val tableLength = mm.get(5)
  require(params.bands * (1 << params.hashBits) == tableLength)

  protected def idxs(idx: Int) = {
    require(idx < tableLength, s"idx < tableLength ($idx < $tableLength)")

    val start = mm.get(MemoryMappedLSHTable.headerSize + idx)
    val end   = mm.get(MemoryMappedLSHTable.headerSize + idx + 1)
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

  def headerSize = 6

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
      def hashAndSlice = has
    }

  // [ sketchLength | bands | bandLength | hashBits | itemsCount | idxs length | offsets ... + offset behind the last array | arrays ]
  def persist[SketchArray](table: IntArrayLSHTable[SketchArray], fileName: String): Unit = {
    val idxs = table.idxs

    val len = lengthOfTable(table)
    if (len > Int.MaxValue) throw new Exception("too long")

    val mmf = mmapFile(fileName, len.toInt) 
    val b = mmf.asIntBuffer

    b.put(table.params.sketchLength)
    b.put(table.params.bands)
    b.put(table.params.bandLength)
    b.put(table.params.hashBits)
    b.put(table.params.itemsCount)
    b.put(idxs.length)

    var off = headerSize + idxs.length + 1

    for (arr <- idxs) {
      b.put(off)
      off += arrLen(arr)
    }

    b.put(off)

    for (arr <- idxs) {
      if (arr != null) {
        b.put(arr)
      }
    }

    mmf.force()

  }

  protected def lengthOfTable[SketchArray](table: IntArrayLSHTable[SketchArray]) =
    (headerSize + table.idxs.length.toLong + 1 + table.idxs.map(arrLen).sum) * 4

  protected def mmapFile(fileName: String, length: Int) =
    new RandomAccessFile(fileName, "rw")
      .getChannel()
      .map(FileChannel.MapMode.READ_WRITE, 0, length)

  private def arrLen(arr: Array[Int]) = if (arr == null) 0 else arr.length

}




/*
abstract class CompactMemoryMappedLSHTable[SketchArray](val mm: IntBuffer) extends MMCommon[SketchArray] {

  val params = decodeParams(mm, 0)

  private val tableLength = mm.get(5)
  private val blockSize   = mm.get(6)
  private val blockTableLength = (tableLength + blockSize-1) / blockSize
  require(params.bands * (1 << params.hashBits) == tableLength)

  protected def idxs(idx: Int) = {
    require(idx < tableLength, s"idx < tableLength ($idx < $tableLength)")

    val boff = mm.get(headerSize + idx / blockSize)

    val lstart = headerSize + blockTableLength + (idx / blockSize * blockSize) /2
    val lend   = headerSize + blockTableLength + idx/2

    val arr = new Array[Short]((lend-lstart)*2)
    for (i < lstart until lend) {
      val j = i-lstart
      val int = mm.get(i)
      arr(j*2)   = int & 0xffff
      arr(j*2+1) = int >>> 16
    }

    var sum = 0
    for (i <- 0 until (arr.length - (idx & 1)) {
      sum += arr(i)
    }


    val dstart = boff + sum
    val dend






    val res = new Array[Int](len)

    for (i <- 0 until len) {
      res(i) = mm.get(start+i)
    }

    require(fastSparse.isDistinctIncreasingArray(res))

    res
  }

  override def toString = s"MemoryMappedLSHTable($mm, params = $params, tableLength = $tableLength)"
}


object CompactMemoryMappedLSHTable {

  def headerSize = 7

  // [ fields | block offsets | lengths (packed in 2B shorts) | data ]
  def persist[SketchArray](table: IntArrayLSHTable[SketchArray], fileName: String): Unit = {
    val len = lengthOfTable(table) // TODO
    if (len > Int.MaxValue) throw new Exception("too long")

    val idxs = table.idxs

    val blockSize = 16
    val tableLength = idxs.length
    val blockTableLength = (tableLength + blockSize-1) / blockSize
    val off = headerSize + blockTableLength + (tableLength+1/2)
    val boffs = idxs.grouped(blockSize).scanLeft(off) { (sum, block) => sum + block.map(arrLen).sum }.toArray


    val mmf = mmapFile(fileName, len.toInt) 
    val b = mmf.asIntBuffer

    b.put(table.params.sketchLength)
    b.put(table.params.bands)
    b.put(table.params.bandLength)
    b.put(table.params.hashBits)
    b.put(table.params.itemsCount)
    b.put(idxs.length)
    b.put(blockSize)

    b.put(boffs)

    for (i <- 0 until idx.length by 2) {
      val l1 = idxs(i).length
      val l2 = idxs(i+1).length
      require(l1 < Short.MaxValue && l2 < Short.MaxValue)
      b.put(l2 << 16 | l1)
    }

    if (idxs.length % 2 == 1) {
      val l1 = idxs(idxs.length-1).length
      b.put(l1)
    }

    for (arr <- idxs) {
      if (arr != null) {
        b.put(arr)
      }
    }

    mmf.force()
  }
}
*/
