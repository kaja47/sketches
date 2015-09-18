package atrox

import java.util.Arrays
import java.lang.Float.floatToRawIntBits
import java.lang.Double.doubleToRawLongBits


/** Radix sort is non-comparative sorting alorithm that have linear complexity
  * for fixed width integers. In practice it's much faster than
  * java.util.Arrays.sort for arrays larger than 1k. The only drawback is that the
  * implementation used here is not in-place and needs auxilary array that is as
  * big as the input to be sorted.
  *
  * Based on arcane knowledge of http://www.codercorner.com/RadixSortRevisited.htm
  */
object RadixSort {


  protected def computeOffsets(
    counts: Array[Int], offsets: Array[Int], bytes: Int, length: Int,
    dealWithNegatives: Boolean = true, floats: Boolean = false, detectSkips: Boolean = true
  ): Int = {

    var canSkip = 0

    // compute offsets/prefix sums
    var byte = 0
    while (byte < bytes) {
      val b256 = byte * 256

      offsets(b256 + 0) = 0

      var i = 1
      while (i < 256) {
        offsets(b256 + i) = counts(b256 + i-1) + offsets(b256 + i-1)
        i += 1
      }

      if (detectSkips) {
        // detect radices that can be skipped
        var i = 0
        var skip = false
        while (i < 256 && (counts(b256 + i) == length || counts(b256 + i) == 0) && !skip) {
          skip = (counts(b256 + i) == length)
          i += 1
        }

        if (skip) {
          canSkip |= (1 << byte)
        }
      }

      byte += 1
    }

    val lastByte = bytes - 1
    val lb256 = lastByte * 256

    // deal with negative values
    if (dealWithNegatives) {
      var negativeValues = 0
      var i = 128
      while (i < 256) {
        negativeValues += counts(lb256 + i)
        i += 1
      }

      if (!floats) {

        offsets(lb256 + 0) = negativeValues
        offsets(lb256 + 128) = 0

        var i = 1 ; while (i < 256) {
          val ii = i + 128
          val curr = ii % 256
          val prev = (ii - 1 + 256) % 256
          offsets(lb256 + curr) = counts(lb256 + prev) + offsets(lb256 + prev)
          i += 1
        }

      } else {

        offsets(lb256 + 0) = negativeValues
        offsets(lb256 + 255) = counts(lb256 + 255) - 1

        var i = 1 ; while (i < 128) {
          offsets(lb256 + i) = offsets(lb256 + i - 1) + counts(lb256 + i - 1)
          i += 1
        }

        i = 254 ; while (i > 127) {
          offsets(lb256 + i) = offsets(lb256 + i + 1) + counts(lb256 + i)
          i -= 1
        }

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


  protected def checkPreconditions[T](arr: Array[T], scratch: Array[T], from: Int, to: Int, fromByte: Int, toByte: Int, maxBytes: Int) = {
    require(to <= scratch.length)
    require(fromByte < toByte)
    require(fromByte >= 0 && fromByte < maxBytes)
    require(toByte > 0 && toByte <= maxBytes)
    require(from >= 0)
    require(to <= arr.length)
  }



  def sort(arr: Array[Int]): Unit = {
    if (arr.length <= 1024) {
      Arrays.sort(arr)
    } else {
      sort(arr, new Array[Int](arr.length), 0, arr.length, 0, 4, true)
    }
  }

  def sort(arr: Array[Int], scratch: Array[Int]): (Array[Int], Array[Int]) =
    sort(arr, scratch, 0, arr.length, 0, 4, false)

  def sort(arr: Array[Int], scratch: Array[Int], returnResultInSourceArray: Boolean): (Array[Int], Array[Int]) =
    sort(arr, scratch, 0, arr.length, 0, 4, returnResultInSourceArray)

  /** Sorts `arr` array using `scratch` as teporary scratchpad. Returns both
    * arrays, first sorted, second in undefined state. Both returned arrays are the
    * same arrays passed as arguments but it's not specified which is which.
    *
    * If returnResultInSourceArray is set to true, the sorted array is the one
    * passed as argument to be sorted. In this case arrays cannot be swapped.
    *
    * from and fromByte are inclusive positions
    * to and toByte are exclusive positions
    */
  def sort(arr: Array[Int], scratch: Array[Int], from: Int, to: Int, fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Int], Array[Int]) = {

    checkPreconditions(arr, scratch, from, to, fromByte, toByte, 4)

    var input = arr
    var output = scratch
    val counts = new Array[Int](4 * 256)
    val offsets  = new Array[Int](4 * 256)
    var sorted = true
    var last = input(to - 1)

    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = to - 1
    while (i >= from) {
      sorted &= last >= input(i)
      last = input(i)

      var byte = 0
      while (byte < 4) { // iterates through all 4 bytes on purpose, JVM unrolls this loop
        val c = (input(i) >>> (byte * 8)) & 0xff
        counts(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    if (sorted) return (input, output)

    val canSkip = computeOffsets(counts, offsets, 4, arr.length)

    var byte = fromByte
    while (byte < toByte) {
      if ((canSkip & (1 << byte)) == 0) {

        val byteOffsets = Arrays.copyOfRange(offsets, byte * 256, byte * 256 + 256)

        var i = from
        while (i < to) {
          val c = (input(i) >>> (byte * 8)) & 0xff
          output(byteOffsets(c)) = input(i)
          byteOffsets(c) += 1
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
      Arrays.sort(arr)
    } else {
      sort(arr, new Array[Long](arr.length), 0, arr.length, 0, 8, true)
    }
  }

  def sort(arr: Array[Long], scratch: Array[Long]): (Array[Long], Array[Long]) =
    sort(arr, scratch, 0, arr.length, 0, 8, false)

  def sort(arr: Array[Long], scratch: Array[Long], returnResultInSourceArray: Boolean): (Array[Long], Array[Long]) =
    sort(arr, scratch, 0, arr.length, 0, 8, returnResultInSourceArray)

  def sort(arr: Array[Long], scratch: Array[Long], from: Int, to: Int, fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Long], Array[Long]) = {

    checkPreconditions(arr, scratch, from, to, fromByte, toByte, 8)

    var input = arr
    var output = scratch
    val counts = new Array[Int](8 * 256)
    val offsets  = new Array[Int](8 * 256)
    var sorted = true
    var last = input(to - 1)

    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = to - 1
    while (i >= from) {
      sorted &= last >= input(i)
      last = input(i)

      var byte = 0
      while (byte < 8) {
        val c = ((input(i) >>> (byte * 8)) & 0xff).toInt
        counts(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    if (sorted) return (input, output)

    val canSkip = computeOffsets(counts, offsets, 8, arr.length)

    var byte = fromByte
    while (byte < toByte) {
      if ((canSkip & (1 << byte)) == 0) {

        val byteOffsets = Arrays.copyOfRange(offsets, byte * 256, byte * 256 + 256)

        var i = from
        while (i < to) {
          val c = ((input(i) >>> (byte * 8)) & 0xff).toInt
          output(byteOffsets(c)) = input(i)
          byteOffsets(c) += 1
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



  def sort(arr: Array[Float]): Unit = {
    if (arr.length <= 1024) {
      Arrays.sort(arr)
    } else {
      sort(arr, new Array[Float](arr.length), 0, arr.length, 0, 8, true)
    }
  }

  def sort(arr: Array[Float], scratch: Array[Float]): (Array[Float], Array[Float]) =
    sort(arr, scratch, 0, arr.length, 0, 4, false)

  def sort(arr: Array[Float], scratch: Array[Float], returnResultInSourceArray: Boolean): (Array[Float], Array[Float]) =
    sort(arr, scratch, 0, arr.length, 0, 4, returnResultInSourceArray)

  def sort(arr: Array[Float], scratch: Array[Float], from: Int, to: Int, fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Float], Array[Float]) = {

    checkPreconditions(arr, scratch, from, to, fromByte, toByte, 4)

    var input = arr
    var output = scratch
    val counts = new Array[Int](4 * 256)
    val offsets  = new Array[Int](4 * 256)
    var sorted = true
    var last = input(to - 1)

    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = to - 1
    while (i >= from) {
      sorted &= last >= input(i)
      last = input(i)

      var byte = 0
      while (byte < 4) {
        val c = (floatToRawIntBits(input(i)) >>> (byte * 8)) & 0xff
        counts(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    if (sorted) return (input, output)

    val canSkip = computeOffsets(counts, offsets, 4, arr.length, floats = true)

    var byte = fromByte
    while (byte < toByte) {
      if ((canSkip & (1 << byte)) == 0) {

        val byteOffsets = Arrays.copyOfRange(offsets, byte * 256, byte * 256 + 256)

        var i = from
        while (i < to) {
          val c = (floatToRawIntBits(input(i)) >>> (byte * 8)) & 0xff
          output(byteOffsets(c)) = input(i)
          byteOffsets(c) += (if (byte < 3 || input(i) >= 0) 1 else -1)
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



  def sort(arr: Array[Double]): Unit = {
    if (arr.length <= 1024) {
      Arrays.sort(arr)
    } else {
      sort(arr, new Array[Double](arr.length), 0, arr.length, 0, 8, true)
    }
  }

  def sort(arr: Array[Double], scratch: Array[Double]): (Array[Double], Array[Double]) =
    sort(arr, scratch, 0, arr.length, 0, 8, false)

  def sort(arr: Array[Double], scratch: Array[Double], returnResultInSourceArray: Boolean): (Array[Double], Array[Double]) =
    sort(arr, scratch, 0, arr.length, 0, 8, returnResultInSourceArray)

  def sort(arr: Array[Double], scratch: Array[Double], from: Int, to: Int, fromByte: Int, toByte: Int, returnResultInSourceArray: Boolean): (Array[Double], Array[Double]) = {

    checkPreconditions(arr, scratch, from, to, fromByte, toByte, 8)

    var input = arr
    var output = scratch
    val counts = new Array[Int](8 * 256)
    val offsets  = new Array[Int](8 * 256)
    var sorted = true
    var last = input(to - 1)

    // collect counts
    // This loop iterates backward because this way it brings begining of the
    // `arr` array into a cache and that speeds up next iteration.
    var i = to - 1
    while (i >= from) {
      sorted &= last >= input(i)
      last = input(i)

      var byte = 0
      while (byte < 8) {
        val c = (doubleToRawLongBits(input(i)) >>> (byte * 8) & 0xff).toInt
        counts(byte * 256 + c) += 1
        byte += 1
      }
      i -= 1
    }

    if (sorted) return (input, output)

    val canSkip = computeOffsets(counts, offsets, 8, arr.length, dealWithNegatives = true, floats = true, detectSkips = true)

    var byte = fromByte
    while (byte < toByte) {
      if ((canSkip & (1 << byte)) == 0) {

        val byteOffsets = Arrays.copyOfRange(offsets, byte * 256, byte * 256 + 256)

        var i = from
        while (i < to) {
          val c = (doubleToRawLongBits(input(i)) >>> (byte * 8) & 0xff).toInt
          output(byteOffsets(c)) = input(i)
          byteOffsets(c) += (if (byte < 7 || input(i) >= 0) 1 else -1)
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
