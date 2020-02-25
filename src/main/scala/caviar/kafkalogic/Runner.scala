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

package caviar.kafkalogic

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.LazyLogging
import oled.app.runutils.CMDArgs
import oled.app.runutils.InputHandling.{MongoDataOptions, getMongoData}
import oled.datahandling.Example
import oled.kafkalogic.KafkaLocalCoordinator
import oled.learning.LocalCoordinator
import oled.learning.Types.RunSingleCore

object Runner extends LazyLogging {

  def main(args: Array[String]) = {

    val argsok = CMDArgs.argsOk(args)

    if (argsok._1) {

      val runningOptions = CMDArgs.getOLEDInputArgs(args)

      val train1 = Vector("caviar-train")

      val evalOneTestSet = false

      if (!evalOneTestSet) {

        /* Single-pass run on the entire dataset */
        val trainingDataOptions = new MongoDataOptions(dbNames       = train1, chunkSize = runningOptions.chunkSize,
                                                       targetConcept = runningOptions.targetHLE, sortDbByField = "time", what = "training")

        val testingDataOptions = trainingDataOptions

        val trainingDataFunction: MongoDataOptions => Iterator[Example] = getMongoData
        val testingDataFunction: MongoDataOptions => Iterator[Example] = getMongoData

        /*val data = getMongoData(trainingDataOptions)
        var accum = List[String]()
        for (x <- data) {
          if (x.queryAtoms.nonEmpty) {
            //println(x.queryAtoms)
            for (y <- x.queryAtoms ) if (accum.contains(y)) println(s"Duplicate: $y")
            accum = accum ++ x.queryAtoms
          }
        }
        println(accum.size)
        val stop = "stop"*/

        val system = ActorSystem("LocalLearningSystem")
        val startMsg = new RunSingleCore

        val coordinator = system.actorOf(Props(new KafkaLocalCoordinator(3, runningOptions, trainingDataOptions,
                                                                            testingDataOptions, trainingDataFunction, testingDataFunction)), name = "LocalCoordinator")

        coordinator ! startMsg

      } else {

        /*val caviarNum = args.find(x => x.startsWith("caviar-num")).get.split("=")(1)

      val trainSet = Map(1 -> MeetingTrainTestSets.meeting1, 2 -> MeetingTrainTestSets.meeting2, 3 -> MeetingTrainTestSets.meeting3,
        4 -> MeetingTrainTestSets.meeting4, 5 -> MeetingTrainTestSets.meeting5, 6 -> MeetingTrainTestSets.meeting6,
        7 -> MeetingTrainTestSets.meeting7, 8 -> MeetingTrainTestSets.meeting8, 9 -> MeetingTrainTestSets.meeting9,
        10 -> MeetingTrainTestSets.meeting10)

      val dataset = trainSet(caviarNum.toInt)

      val trainingDataOptions =
        new MongoDataOptions(dbNames = dataset._1,//trainShuffled, //
          chunkSize = runningOptions.chunkSize, targetConcept = runningOptions.targetHLE, sortDbByField = "time", what = "training")

      val testingDataOptions =
        new MongoDataOptions(dbNames = dataset._2,
          chunkSize = runningOptions.chunkSize, targetConcept = runningOptions.targetHLE, sortDbByField = "time", what = "testing")

      val trainingDataFunction: MongoDataOptions => Iterator[Example] = FullDatasetHoldOut.getMongoData
      val testingDataFunction: MongoDataOptions => Iterator[Example] = FullDatasetHoldOut.getMongoData

      val system = ActorSystem("HoeffdingLearningSystem")
      val startMsg = "start"

      system.actorOf(Props(new Dispatcher(runningOptions, trainingDataOptions, testingDataOptions,
        trainingDataFunction, testingDataFunction) ), name = "Learner") ! startMsg*/

      }

      /*----------------------------*/
      /*Eval on test set in the end:*/
      /*----------------------------*/
      /*val caviarNum = args.find(x => x.startsWith("caviar-num")).get.split("=")(1)

      val trainSet = Map(1 -> MeetingTrainTestSets.meeting1, 2 -> MeetingTrainTestSets.meeting2, 3 -> MeetingTrainTestSets.meeting3,
        4 -> MeetingTrainTestSets.meeting4, 5 -> MeetingTrainTestSets.meeting5, 6 -> MeetingTrainTestSets.meeting6,
        7 -> MeetingTrainTestSets.meeting7, 8 -> MeetingTrainTestSets.meeting8, 9 -> MeetingTrainTestSets.meeting9,
        10 -> MeetingTrainTestSets.meeting10)

      val dataset = trainSet(caviarNum.toInt)

      val trainingDataOptions =
        new MongoDataOptions(dbNames = dataset._1,//trainShuffled, //
          chunkSize = runningOptions.chunkSize, targetConcept = runningOptions.targetHLE, sortDbByField = "time", what = "training")

      val testingDataOptions =
        new MongoDataOptions(dbNames = dataset._2,
          chunkSize = runningOptions.chunkSize, targetConcept = runningOptions.targetHLE, sortDbByField = "time", what = "testing")

      val trainingDataFunction: MongoDataOptions => Iterator[Example] = FullDatasetHoldOut.getMongoData
      val testingDataFunction: MongoDataOptions => Iterator[Example] = FullDatasetHoldOut.getMongoData

      val system = ActorSystem("HoeffdingLearningSystem")
      val startMsg = "start"

      system.actorOf(Props(new Dispatcher(runningOptions, trainingDataOptions, testingDataOptions,
        trainingDataFunction, testingDataFunction) ), name = "Learner") ! startMsg*/

    } else {
      logger.error(argsok._2)
      System.exit(-1)
    }
  }

}
