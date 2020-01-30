/*
 * Copyright (C) 2016  Nikos Katzouris
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package maritime

import intervalTree.IntervalTree
import maritime.IntervalHandler.{HLEIntervalToAtom, generateIntervalTree, generateLLEInstances}
import oled.datahandling.Example
import data._
import scala.collection.mutable.ListBuffer

/**
  * Created by nkatz at 28/1/20
  */

class ExampleIterator(
    inputSource: Iterator[String],
    batchSize: Int,
    targetEvent: String,
    LLE_bias: List[String],
    mode: String,
    opts: FileDataOptions) extends Iterator[Example] {

  println("Generating intervals tree...")
  val intervalTree: IntervalTree[HLEInterval] = generateIntervalTree(opts.HLE_Files_Dir, opts.HLE_bias)

  var prev_batch_timestamp: Long = 0
  var batchCount = 0

  def hasNext: Boolean = inputSource.hasNext

  /**
    * Parses the input data into logical syntax and generates data mini-batches for training.
    *
    * A data batch is a chunk of input data of given size. Size is measured by temporal duration,
    * so given batchSize = n, a mini-batch consists of input data with time stamps t to t+n.
    *
    */

  def next(): Example = {
    var currentBatch = new ListBuffer[String]
    var timesAccum = scala.collection.mutable.SortedSet[Long]()
    var llesAccum = scala.collection.mutable.SortedSet[String]()

    val INF_TS = 2000000000

    while ((timesAccum.size <= batchSize) && (inputSource.hasNext)) {
      val newLine = inputSource.next()
      val split = newLine.split("\\|")
      println(split.mkString(" "))
      val time = split(1)
      val lle = split(0)
      if (!timesAccum.contains(time.toLong)) timesAccum += time.toLong
      if (!llesAccum.contains(lle)) llesAccum += lle
      currentBatch += generateLLEInstances(newLine, mode)
    }

    val json_time = prev_batch_timestamp
    //currentBatch += generateLLEInstances(newLine, mode)
    batchCount += 1

    //what is the use of this line?
    val nexts = timesAccum.sliding(2).map(x => if (mode == "asp") s"next(${x.last},${x.head})" else s"next(${x.last},${x.head})")
    val intervals = if (inputSource.hasNext) intervalTree.range(prev_batch_timestamp, timesAccum.last) else intervalTree.range(prev_batch_timestamp, INF_TS)

    timesAccum += prev_batch_timestamp

    if (!inputSource.hasNext) timesAccum += INF_TS

    prev_batch_timestamp = timesAccum.last

    var extras = timesAccum.flatMap{ timeStamp =>
      val containedIn = intervals.filter(interval => (interval._3.stime < timeStamp && timeStamp < interval._3.etime))
      containedIn.map(x => HLEIntervalToAtom(x._3, timeStamp.toString, targetEvent))
    }

    extras = extras ++ intervals.map((interval) => if (interval._3.stime >= timesAccum.head) HLEIntervalToAtom(interval._3, interval._3.stime.toString, targetEvent) else "None")
    extras = extras ++ intervals.map((interval) => if (interval._3.etime <= timesAccum.last) HLEIntervalToAtom(interval._3, interval._3.etime.toString, targetEvent) else "None")

    for (x <- extras) currentBatch += x
    for (x <- nexts) currentBatch += x

    val all_events = currentBatch.filter(x => x != "None")
    val annotation = all_events.filter(x => x.startsWith("holdsAt(" + targetEvent))
    val narrative = all_events.filter(x => !x.startsWith("holdsAt(" + targetEvent))

    val curr_exmpl = Example(annotation.toList, narrative.toList, json_time.toString)

    curr_exmpl
  }

}
