package atrox

/** Based on arcane knowledge of http://www.codercorner.com/RadixSortRevisited.htm */
object RadixSort {

  protected def computeOffsets(
    counters: Array[Int], offsets: Array[Int], ends: Array[Int], bytes: Int, length: Int,
    dealWithNegatives: Boolean = true, computeEnds: Boolean = false, detectPossibleSkips: Boolean = true
  ): Int = {

    var canSkip = 0

    // compute offsets/prefix sums
    var byte = 0
    while (byte < bytes) {
      offsets(byte * 256 + 0) = 0

      var i = 1
      while (i < 256) {
        offsets(byte * 256 + i) = counters(byte * 256 + i-1) + offsets(byte * 256 + i-1)
        i += 1
      }

      if (detectPossibleSkips) {
        // detect radices that can be skipped
        i = 0
        while (i < 256) {
          if (counters(byte * 256 + i) < length && counters(byte * 256 + i) != 0) {
            i += 257 // poor man's break
          } else if (counters(byte * 256 + i) == length) {
            canSkip |= (1 << byte)
            i += 257 // poor man's break
          }
          i += 1
        }
      }

      byte += 1
    }

    val lastByte = bytes - 1

    // deal with negative values
    if (dealWithNegatives) {
      var negativeValues = 0
      var i = 128
      while (i < 256) {
        negativeValues += counters(lastByte * 256 + i)
        i += 1
      }

      offsets(lastByte * 256 + 0) = negativeValues
      offsets(lastByte * 256 + 128) = 0

      i = 1
      while (i < 256) {
        val ii = i + 128
        val curr = ii % 256
        val prev = (ii - 1 + 256) % 256
        offsets(lastByte * 256 + curr) = counters(lastByte * 256 + prev) + offsets(lastByte * 256 + prev)
        i += 1
      }
    }

    if (computeEnds) {
      var byte = 0
      while (byte < bytes) {
        var i = 0 ; while (i < 256) {
          ends(byte * 256 + i) = offsets(byte * 256 + (i + 1) % 256)
          i += 1
        }
        ends(byte * 256 + 255) = length
        byte += 1
      }
    }

    canSkip
  }

  protected def handleResults[T](arr: Array[T], input: Array[T], output: Array[T], returnResultInSourceArray: Boolean): (Array[T], Array[T]) = {
    if (returnResultInSourceArray && !(input eq arr)) {
      // copy data into array that was passed as an argument to be sorted
      System.arraycopy(input, 0, output, 0, input.length)
      assert(input != output)
      (output, input)
    } else {
      // return arrays as they are
      assert(input != output)
      (input, output)
    }
  }

  def sort(arr: Array[Int]): Unit = {
    if (arr.length <= 1024) {
      java.util.Arrays.sort(arr)
    } else {
      sort(arr, new Array[Int](arr.length), 0, 4, true)
    }
  }

  def sort(arr: Array[Int], scratch: Array[Int]): (Array[Int], Array[Int]) =
    sort(arr, scratch, 0, 4, false)

  def sort(arr: Array[Int], scratch: Array[Int], returnResultInSourceArray: Boolean): (Array[Int], Array[Int]) =
    sort(arr, scratch, 0, 4, returnResultInSourceArray)


  /** Sorts `arr` array using `scratch` as teporary scratchpad. Returns both
    * arrays, first sorted, second in undefined state. Both returned arrays are the
    * same arrays passed as arguments but it's not specified which is which.
    *
    * If returnResultInSourceArray is set to true, the sorted array is the one
    * passed as argument to be sorted. In this case arrays cannot be swapped.
    */
  def sort(arr: Array[Int], scratch: Array[Int], fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Int], Array[Int]) = {

    require(arr.length == scratch.length)
    require(fromByte < toByte)
    require(fromByte >= 0 && fromByte < 4)
    require(toByte > 0 && toByte <= 4)

    var input = arr
    var output = scratch
    val counters = new Array[Int](4 * 256)
    val offsets  = new Array[Int](4 * 256)

    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = arr.length - 1
    while (i >= 0) {
      var byte = 0
      while (byte < 4) {
        val c = (input(i) >>> (byte * 8)) & 0xff
        counters(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    val canSkip = computeOffsets(counters, offsets, null, 4, arr.length)

    var byte = fromByte
    while (byte < toByte) {

      if ((canSkip & (1 << byte)) == 0) {

        var i = 0
        while (i < arr.length) {
          val c = (input(i) >>> (byte * 8)) & 0xff
          output(offsets(byte * 256 + c)) = input(i)
          offsets(byte * 256 + c) += 1
          i += 1
        }

        // swap input with output
        val tmp = input
        input = output
        output = tmp
      }

      byte += 1
    }

    handleResults(arr, input, output, returnResultInSourceArray)
  }




  def sort(arr: Array[Long]): Unit = {
    if (arr.length <= 1024) {
      java.util.Arrays.sort(arr)
    } else {
      sort(arr, new Array[Long](arr.length), 0, 8, true)
    }
  }

  def sort(arr: Array[Long], scratch: Array[Long]): (Array[Long], Array[Long]) =
    sort(arr, scratch, 0, 8, false)

  def sort(arr: Array[Long], scratch: Array[Long], returnResultInSourceArray: Boolean): (Array[Long], Array[Long]) =
    sort(arr, scratch, 0, 8, returnResultInSourceArray)


  def sort(arr: Array[Long], scratch: Array[Long], fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Long], Array[Long]) = {

    require(arr.length == scratch.length)
    require(fromByte < toByte)
    require(fromByte >= 0 && fromByte < 8)
    require(toByte > 0 && toByte <= 8)

    var input = arr
    var output = scratch
    val counters = new Array[Int](8 * 256)
    val offsets  = new Array[Int](8 * 256)


    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = arr.length - 1
    while (i >= 0) {
      var byte = 0
      while (byte < 8) {
        val c = ((input(i) >>> (byte * 8)) & 0xff).toInt
        counters(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    val canSkip = computeOffsets(counters, offsets, null, 8, arr.length)

    var byte = 0
    while (byte < 8) {
      if ((canSkip & (1 << byte)) == 0) {

        var i = 0
        while (i < arr.length) {
          val c = ((input(i) >>> (byte * 8)) & 0xff).toInt
          output(offsets(byte * 256 + c)) = input(i)
          offsets(byte * 256 + c) += 1
          i += 1
        }

        // swap input with output
        val tmp = input
        input = output
        output = tmp

      }
      byte += 1
    }

    handleResults(arr, input, output, returnResultInSourceArray)
  }
}
