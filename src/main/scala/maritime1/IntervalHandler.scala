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

package maritime1

/**
  * Created by nkatz on 18/12/19.
  */

import java.io.File

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.operation.distance.DistanceOp
import data._
import intervalTree.IntervalTree
import oled.app.runutils.InputHandling.InputSource
import oled.app.runutils.RunningOptions
import oled.datahandling.Example

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

// Reads data in this format:
/*change_in_heading|1443694493|1443694493|245257000
change_in_speed_start|1443890320|1443890320|259019000
change_in_speed_end|1443916603|1443916603|311018500
coord|1443686670|1443686670|228041600|-4.47298500000000043286|48.38163999999999731472
entersArea|1443875806|1443875806|228017700|18515
leavesArea|1443887789|1443887789|235113366|21501
gap_start|1444316024|1444316024|246043000
gap_end|1445063602|1445063602|269103970
proximity|1445357304|1445354425|1445357304|true|227519920|227521960
velocity|1443665064|1443665064|228374000|19.30468943370810563920|31.39999999999999857891|35.54927442329611153582
slow_motion_start|1444262324|1444262324|228167900
slow_motion_end|1444281397|1444281397|258088080
stop_start|1444031990|1444031990|227705102
stop_end|1444187303|1444187303|259019000*/
/*
class MongoDataOptions(val dbNames: Vector[String], val chunkSize: Int = 1,
                       val limit: Double = Double.PositiveInfinity.toInt,
                       val targetConcept: String = "None", val sortDbByField: String = "time",
                       val sort: String = "ascending", val what: String = "training") extends MongoSource
*/

class FileDataOptions(val HLE_Files_Dir: String, val LLE_File: String,
    val allHLEs: List[String], val allLLEs: List[String], val runOpts: RunningOptions) extends InputSource

object IntervalHandler {

  // FOR NOW WILL NOT TAKE ARGUMENTS
  def readInputFromFile(opts: FileDataOptions): Iterator[Example] = {
    // The path to the folder with RTEC results with HLE intervals. The generateIntervalTree
    // methods reads from those files (see the method).

    val pathToHLEIntervals = opts.HLE_Files_Dir

    // The path to the critical points (LLEs)
    val pathToLLEs = opts.LLE_File

    println("Generating intervals tree...")

    val intervalTree =
      generateIntervalTree(
        pathToHLEIntervals,
        opts.allHLEs ++ opts.allLLEs // Because there are some HLEs in allLLEs
      )

    // mode is either "asp" or "mln"
    /**
      * Parses the input data into logical syntax and generates data mini-batches for training.
      *
      * A data batch is a chunk of input data of given size. Size is measured by temporal duration,
      * so given batchSize = n, a mini-batch consists of input data with time stamps t to t+n.
      *
      */
    class ExampleIterator(inputSource: Iterator[String], batchSize: Int, targetEvent: String, mode: String)
      extends Iterator[Example] {
      var prev_batch_timestamp: Long = 0
      var batchCount = 0

      // They are used to help me to get what it should to next batch
      var nextBatchLLEsAccum:String = ""
      var nextBatch: String = ""


      def hasNext = inputSource.hasNext

      def next() = {
        var currentBatch = new ListBuffer[String]()//nextBatch.clone()
        currentBatch += nextBatch

        var timesAccum = scala.collection.mutable.SortedSet[Long]()
        var llesAccum = scala.collection.mutable.SortedSet[String]()
        llesAccum += nextBatchLLEsAccum


        val INF_TS = 2000000000

        //var curr_id = 0
        timesAccum += prev_batch_timestamp

        while ((timesAccum.size <= batchSize) && (inputSource.hasNext)) {
          val newLine = inputSource.next()
          //println(newLine)
          val split = newLine.split("\\|")

          //println(split.mkString(" "))

          val time = split(1)
          val lle = split(0)

          if (!timesAccum.contains(time.toLong)) {
            timesAccum += time.toLong
          }

          if (timesAccum.size > batchSize) {
            nextBatchLLEsAccum = lle
            nextBatch = generateLLEInstances(newLine, mode)
          } else {
            if (!llesAccum.contains(lle)) llesAccum += lle

            currentBatch += generateLLEInstances(newLine, mode)
          }
        }

        val json_time = prev_batch_timestamp

        //currentBatch += generateLLEInstances(newLine, mode)
        batchCount += 1

        val t1 = System.nanoTime()
        val intervals = if (inputSource.hasNext) intervalTree.range(prev_batch_timestamp, timesAccum.last) else intervalTree.range(prev_batch_timestamp, INF_TS)
        val dur = (System.nanoTime() - t1) / 1e9d


        if (!inputSource.hasNext) timesAccum += INF_TS

        var extras: List[String] = intervals.flatMap((interval) => {
          if (interval._3.stime >= timesAccum.head) {
            if (opts.allLLEs.contains(interval._3.hle)) {
              timesAccum += interval._3.stime
            }
            HLEIntervalToAtom(interval._3, interval._3.stime.toString, "None", opts.allHLEs)
          } else { List("None") }
        })

        extras = extras ++ intervals.flatMap((interval) => {
          if (interval._3.etime <= (timesAccum - timesAccum.last).last) {
            if (opts.allLLEs.contains(interval._3.hle)) {
              timesAccum += interval._3.stime
            }

            HLEIntervalToAtom(interval._3, interval._3.etime.toString, "None", opts.allHLEs)
          } else List("None")
        })

        //what is the use of this line?
        val nexts = timesAccum.sliding(2).map(x => if (mode == "asp") s"next(${x.last},${x.head})" else s"next(${x.last},${x.head})")
/*
        var nextsHashMap = new mutable.HashMap[Long, Long]()
        var slideIterator = timesAccum.sliding(2)

        while (slideIterator.hasNext) {
          val currSortedSet = slideIterator.next()
          val currKey: Long = currSortedSet.head
          val currVal: Long = currSortedSet.last

          nextsHashMap += (currKey -> currVal)
        }
*/

        prev_batch_timestamp = timesAccum.last

        extras = extras ++ (timesAccum - timesAccum.last).flatMap { timeStamp =>
          val containedIn = intervals.filter(interval => ( //(opts.allHLEs.contains(interval._3.hle) && interval._3.stime < nextsHashMap(timeStamp)
            //&& nextsHashMap(timeStamp) < interval._3.etime) ||
            ( /*opts.allLLEs.contains(interval._3.hle) &&*/ interval._3.stime < timeStamp && timeStamp < interval._3.etime)))

          containedIn.flatMap(x =>
            {
              HLEIntervalToAtom(x._3, timeStamp.toString, "None" /*nextsHashMap(timeStamp).toString*/ , opts.allHLEs)
            })
        } toList

        // Why this line is used?
        if (extras.nonEmpty) {
          val stop = "stop"
        }

        for (x <- extras) currentBatch += x
        for (x <- nexts) currentBatch += x

        val all_events = currentBatch.filter(x => x != "None")

        //var temp = all_events.clone()
        var annotation = ListBuffer[String]()
        var narrative = ListBuffer[String]()

        val hleIter: Iterator[String] = opts.allHLEs.iterator
        val lleIter: Iterator[String] = opts.allLLEs.iterator

        val t2 = System.nanoTime()
        while (lleIter.hasNext) {
          var currEvent = lleIter.next()
          annotation = annotation ++ all_events.filter(x => x.startsWith("happensAt(" + currEvent + "("))
          //temp = all_events.clone()
        }

        while (hleIter.hasNext) {
          var currEvent = hleIter.next()
          narrative = narrative ++ all_events.filter(x => x.startsWith("holdsAt(" + currEvent + "("))
          //temp = all_events.clone()
        }

        val dur2 = (System.nanoTime() - t2) / 1e9d
        print(s"${dur} ${all_events.size} ")

        val curr_exmpl = Example(narrative.toList, annotation.toList, json_time.toString)

        curr_exmpl
      }
    }

    val data = Source.fromFile(pathToLLEs).getLines.filter(x =>
      opts.allLLEs.contains(x.split("\\|")(0))
    //!x.startsWith("coord") && !x.startsWith("velocity") && !x.startsWith("entersArea") && !x.startsWith("leavesArea")
    ).toIterator

    val it: Iterator[Example] = new ExampleIterator(data, opts.runOpts.chunkSize, opts.runOpts.targetHLE, mode = "asp")

    it
  }

  // mode is either "asp" or "mln"
  def generateLLEInstances(line: String, mode: String) = {
    // These have a different schema
    // proximity is HLE
    val abnormalLLEs = Set[String]("coord", "entersArea", "leavesArea", "velocity")
    val split = line.split("\\|")
    if (!abnormalLLEs.contains(split(0))) {

      // These have a common simple schema:
      // change_in_heading, change_in_speed_start, change_in_speed_end,
      // gap_start, gap_end, slow_motion_start, slow_motion_end, stop_start, stop_end
      val lle = split(0)
      val time = split(1)
      val vessel = split(3)
      if (mode == "asp") s"happensAt($lle($vessel),$time)" else s"HappensAt(${lle.capitalize}_$vessel),$time)"
    } else {

      if (split(0) == "coord") {
        //coord|1443686670|1443686670|228041600|-4.47298500000000043286|48.38163999999999731472
        /*
        val lle = split(0)
        val time = split(1)
        val vessel = split(3)
        val lon = split(4)
        val lat = split(5)
        // Don't return nothing in the MLN case (can't handle the coords syntax)
        if ("mode" == "asp") s"happensAt($lle($vessel,$lon,$lat),$time)" else ""
        */
        // do nothing (we won't use coord).
        "None"
      } else if (split(0) == "entersArea" || split(0) == "leavesArea") {
        //entersArea|1443875806|1443875806|228017700|18515
        // For me enters and leaves are has the form
        //['Fluent','Timestamp','Area_ID','Vessel_ID']

        val lle = split(0)
        val time = split(1)
        val vessel = split(3)
        val area = split(2)
        if (mode == "asp") s"happensAt($lle($vessel,$area),$time)"
        else s"HappensAt(${lle.capitalize}_${vessel}_$area,$time)"

      } else if (split(0) == "velocity") {

        // do nothing (we won't use velocity)
        "None"

      } else {
        throw new RuntimeException(s"Unexpected event: $line")
      }
    }
  }

  /* Generate an HLE logical atom. The i var carries all the info, the t var is the particular
   * time point of the generated atom. "target" is the name of the target complex event. The
   * target event is turned into a holdsAt predicate, while all others are turned into happensAt predicates. */
  def HLEIntervalToAtom(i: HLEInterval, t: String, next_t: String, considered_HLEs: List[String] /*target: String*/ ): List[String] = {

    val functor = if (considered_HLEs.contains(i.hle)) "holdsAt" else "happensAt"

    val fluentTerm =
      if (i.value != "true") s"${i.hle}(${(i.vessels :+ i.value).mkString(",")})"
      else s"${i.hle}(${i.vessels.mkString(",")})"

    val timestamp = {
      if (next_t == "None" || !considered_HLEs.contains(i.hle)) {
        t
      } else {
        next_t
      }
    }

    val fluentTerm2 = {
      if (i.hle == "rendezVous" || i.hle == "proximity"
        || i.hle == "pilotBoarding" || i.hle == "tugging") {
        if (i.value != "true") s"${i.hle}(${(i.vessels.reverse :+ i.value).mkString(",")})"
        else s"${i.hle}(${i.vessels.reverse.mkString(",")})"
      } else "None"
    }

    if (fluentTerm2 != "None") {
      List[String](s"$functor($fluentTerm,$timestamp)", s"$functor($fluentTerm2,$timestamp)")
    } else {
      List[String](s"$functor($fluentTerm,$timestamp)")
    }
  }

  def generateIntervalTree(pathToHLEs: String, interestedIn: List[String]) = {

      def getListOfFiles(dir: String): List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
          println(d.listFiles.toList)
          d.listFiles.filter(f => f.isFile).toList
          // This line does not suits me
          // d.listFiles.filter(f => f.isFile && interestedIn.exists(eventName => f.getName.contains(eventName))).toList
        } else {
          List[File]()
        }
      }

    val files = getListOfFiles(pathToHLEs).map(_.getCanonicalPath)

    val intervalTree = new IntervalTree[HLEInterval]()

    var counter = 0
    files foreach { file =>
      println(s"Updating interval tree from $file")

      val data = Source.fromFile(file).getLines.filter(line => interestedIn.contains(line.split("\\|")(0)))

      while (data.hasNext) {
        val newLine = data.next()
        val i = HLEInterval(newLine)

        counter += 1

        //println(counter)
        //println(newLine)
        intervalTree.addInterval(i.stime, i.etime, i)
      }
    }

    intervalTree
  }

}
