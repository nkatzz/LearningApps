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

/**
  * Created by nkatz on 18/12/19.
  */

import java.io.File

import intervalTree.IntervalTree
import oled.app.runutils.InputHandling.InputSource
import oled.datahandling.Example

import scala.io.Source

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

class FileDataOptions(val HLE_Files_Dir: String, val LLE_File: String,
    val batch_size: Int = 1, val HLE_bias: List[String],
    val LLE_bias: List[String], val target_event: String) extends InputSource

object IntervalHandler {


  def readInputFromFile(opts: FileDataOptions): Iterator[Example] = {

    // The path to the folder with RTEC results with HLE intervals.
    // The generateIntervalTree method reads from those files (see the method).
    val pathToHLEIntervals = opts.HLE_Files_Dir

    // The path to the critical points (LLEs)
    val pathToLLEs = opts.LLE_File

    // mode is either "asp" or "mln"
    val data = Source.fromFile(pathToLLEs).getLines.filter(x => opts.LLE_bias.contains(x.split("\\|")(0)))

    new ExampleIterator(data, opts.batch_size, opts.target_event, opts.LLE_bias, "asp", opts)
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
  def HLEIntervalToAtom(i: HLEInterval, t: String, target: String) = {

    val functor = if (i.hle == target) "holdsAt" else "happensAt"

    val fluentTerm =
      if (i.value != "true") s"${i.hle}(${(i.vessels :+ i.value).mkString(",")})"
      else s"${i.hle}(${i.vessels.mkString(",")})"

    s"$functor($fluentTerm,$t)"
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

        //println(newLine)
        intervalTree.addInterval(i.stime, i.etime, i)
      }
    }

    intervalTree
  }

}
