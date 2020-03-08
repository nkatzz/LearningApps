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

package caviar.kafkalogic.parallelnocomm

import java.io.{File, FileWriter, PrintWriter}

import akka.actor.{ActorRef, Props}
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.InputSource
import orl.kafkalogic.ProdConsLogic.{createTheoryProducer, writeExamplesToTopic}
import orl.kafkalogic.Types.{MergedTheory, TheoryAccumulated, TheoryResponse, WrapUp}
import orl.learning.LocalCoordinator
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.logic.Clause


object Types {
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int])
  class TheoryAccumulated(var theory: List[Clause])
  class MergedTheory(var theory: List[Clause])
  class WrapUp
}

class KafkaNCLocalCoordinator[T <: InputSource](numOfActors: Int, examplesPerIteration: Int, inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends
  LocalCoordinator(inps, trainingDataOptions,
  testingDataOptions, trainingDataFunction,
  testingDataFunction) {

  import context.become

  private def getTrainingData = trainingDataFunction(trainingDataOptions)

  // The Coordinator gets the data and shares them among the Learners via a Kafka Topic
  private var data = Iterator[Example]()




  var terminate = false

  // Initiate numOfActors KafkaWoledASPLearners
  private var workers: List[ActorRef] = List()

 override def receive : PartialFunction[Any, Unit]= {

    case msg: RunSingleCore =>

      if (inps.weightLean) {
        workers = {
            List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaNCWoledASPLearner(inps, trainingDataOptions,
              testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))
        }
        become(waitResponse)
        data = getTrainingData
        writeExamplesToTopic(data, numOfActors, examplesPerIteration)
        for (worker <- workers) worker ! new Run
      }
    case _: LocalLearnerFinished => context.system.terminate()
  }

  /* In this method, the Coordinator will merge the rules from the theories of the Learners in the following way:
   *  -rules that are not already in the mergedTheory, are inserted as they are.
   *  -when a rule is already in the mergedTheory, it is inserted again and the weight of both rules
   *   (the rule that is already in the mergedTheory and the rule found to be the same) is averaged.
   *   The statistics (fps fns tps tns) are summed up. The same is done for their common refinements.
   */
  def mergeTheories: Receive = {
    case errorAcc: TheoryAccumulated =>

      /* If the Coordinator sees that the Examples have finished, he sends a MergedTheory message to
       * the Learners to get their latest theory, write it to the topic and terminate the Learning process
       */
      if(!terminate) {
        become(waitResponse)
        // if writeExamplesToTopic returns true it means the Examples have finished
        if(writeExamplesToTopic(data, numOfActors, examplesPerIteration)) terminate = true
        for (worker <- workers) worker ! new MergedTheory(List())
        //writeTheoryToTopic(mergedTheory, theoryProd)
      } else {
        for (worker <- workers) worker ! new WrapUp
        //writeTheoryToTopic(mergedTheory, theoryProd)
        become(receive)
        self ! new LocalLearnerFinished
      }
  }

  /* Every time a worker finishes processing a batch, he sends its local theory to the coordinator
   * and when the coordinator has collected numOfActors theories, he merges the theories and then
   * sends a new batch of Examples to each worker
   */

  private var responseCount = 0
  var errorCount = 0
  var currBatch = 0
  val pw = new PrintWriter(new FileWriter(new File("NoCommError"), true))
  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      responseCount += 1
      println("Error "+ theoryRes.perBatchError)
      for(i <- currBatch * 5 to (currBatch * 5) + 4) {
        errorCount += theoryRes.perBatchError(i)
      }
      if (responseCount == numOfActors) {
        pw.write(" error: " + errorCount + "\n")
        pw.flush()
        responseCount = 0
        currBatch += 1
        become(mergeTheories)
        self ! new TheoryAccumulated(List[Clause]())
      }
  }

}
