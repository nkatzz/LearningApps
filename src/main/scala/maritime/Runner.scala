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
  * Created by nkatz at 28/1/20
  */

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.LazyLogging
import oled.app.runutils.CMDArgs
import oled.datahandling.Example
import oled.learning.LocalCoordinator
import oled.learning.Types.RunSingleCore
import IntervalHandler.readInputFromFile

object Runner extends LazyLogging {

  def main(args: Array[String]) = {

    val argsok = CMDArgs.argsOk(args)

    if (argsok._1) {

      val runningOptions = CMDArgs.getOLEDInputArgs(args)

      val allLLEs = List("gap_end", "coord", "velocity", "change_in_heading", "entersArea",
        "stop_start", "change_in_speed_start", "gap_start", "change_in_speed_end",
        "stop_end", "leavesArea", "slow_motion_end", "slow_motion_start")

      val allHLEs = List("withinArea", "tuggingSpeed", "stopped", "highSpeedNC",
        "movingSpeed", "underWay", "proximity", "anchoredOrMoored", "changingSpeed", "gap",
        "lowSpeed", "trawlingMovement", "trawlSpeed", "drifting", "loitering", "sarMovement",
        "sarSpeed", "rendezVous", "pilotBoarding", "trawling", "sar", "tugging")

      val targetHLE = runningOptions.targetHLE
      val otherHLEs = allHLEs.filter(_ != targetHLE)

      val modes = runningOptions.globals.MODES

      // I should find a better way
      val bias_fluents = modes.map(clause =>
        clause.split('(')(2)).toSet.toList ++ modes.map(clause => clause.split('(')(3)).toSet.toList

      val HLE_Dir_Path = runningOptions.train + "/HLEs"
      val LLEs_File = runningOptions.train + "/LLEs.csv"

      val HLE_lang_bias = bias_fluents.filter(x => allHLEs.contains(x))
      val LLE_lang_bias = bias_fluents.filter(x => allLLEs.contains(x))

      val fileOpts = new FileDataOptions(
        HLE_Files_Dir = HLE_Dir_Path, LLE_File = LLEs_File,
        batch_size    = runningOptions.chunkSize, HLE_bias = HLE_lang_bias,
        LLE_bias      = LLE_lang_bias, target_event = runningOptions.targetHLE)

      val trainingDataFunction: FileDataOptions => Iterator[Example] = readInputFromFile
      val testingDataFunction: FileDataOptions => Iterator[Example] = readInputFromFile

      val data = trainingDataFunction(fileOpts)

      while (data.hasNext) {
        val e = data.next()
        println(e.queryAtoms)
        println(e.observations + "\n")
        if (e.queryAtoms.nonEmpty) {
          val stop = "stop"
        }
        //val stop = "stop"
      }

      val system = ActorSystem("LocalLearningSystem")
      val startMsg = new RunSingleCore

      val coordinator = system.actorOf(Props(
        new LocalCoordinator(runningOptions, fileOpts, fileOpts,
                             trainingDataFunction, testingDataFunction)), name = "LocalCoordinator")

      coordinator ! startMsg

    } else {
      logger.error(argsok._2)
      System.exit(-1)
    }
  }

}
