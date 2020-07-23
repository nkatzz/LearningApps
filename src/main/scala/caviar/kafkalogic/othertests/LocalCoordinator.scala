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

package caviar.kafkalogic.othertests

import akka.actor.Props
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.InputSource
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.learning.oled.OLEDLearner
import orl.learning.woledasp.WoledASPLearner
import orl.learning.woledmln.WoledMLNLearner
import orl.learning.LocalCoordinator

class SingleTestLocalCoordinator[T <: InputSource](inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends LocalCoordinator(inps, trainingDataOptions,
                                                                          testingDataOptions, trainingDataFunction,
                                                                          testingDataFunction) {

  val t1 = System.nanoTime

  override def receive = {

    case msg: RunSingleCore =>

      if (inps.weightLean) {

        val worker = {
          if (mode == "MLN") {
            context.actorOf(Props(
              new WoledMLNLearner(inps, trainingDataOptions, testingDataOptions,
                                  trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")
          } else {
            context.actorOf(Props(
              new Learner(inps, trainingDataOptions, testingDataOptions,
                          trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")
          }
        }
        worker ! new Run

      } else {

        val worker = context.actorOf(Props(
          new OLEDLearner(inps, trainingDataOptions, testingDataOptions,
                          trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")

        worker ! new Run

      }

    case _: LocalLearnerFinished =>
      val duration = (System.nanoTime - t1) / 1e9d
      context.system.terminate()
  }

}
