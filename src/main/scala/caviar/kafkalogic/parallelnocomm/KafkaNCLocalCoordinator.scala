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

  val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
  val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))

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
    case _: LocalLearnerFinished => {
      val averageLoss = avgLoss(errorCount)
      val accumulatedMistakes = errorCount.scanLeft(0.0)(_ + _).tail
      for(i <- averageLoss._3) pw.write(i + " ")
      pw.write("\n")
      pw.flush()
      pw.close()
      for(i <- accumulatedMistakes) pw2.write(i + " ")
      pw2.write("\n")
      pw2.flush()
      pw2.close()
      import scalatikz.pgf.plots.Figure
      Figure("AverageLoss")
        .plot((0 to averageLoss._3.length-1) -> averageLoss._3)
        .havingXLabel("Batch Number (15 Examples 5 to each learner)")
        .havingYLabel("Average Loss")
        .show()
      Figure("AccumulatedError")
        .plot((0 to accumulatedMistakes.length-1) -> accumulatedMistakes)
        .havingXLabel("Batch Number (15 Examples 5 to each learner)")
        .havingYLabel("Accumulated Mistakes")
        .show()
      context.system.terminate()
    }
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
  var currBatch = 0
  var errorCount: Vector[Int] = Vector[Int]()

  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      if(responseCount == 0){
        errorCount = theoryRes.perBatchError
      } else {
        errorCount = errorCount.zip(theoryRes.perBatchError).map(t => t._1 + t._2)
      }
      responseCount += 1
      if (responseCount == numOfActors) {
        responseCount = 0
        currBatch += 1
        become(mergeTheories)
        self ! new TheoryAccumulated(List[Clause]())
      }
  }

  def avgLoss(in: Vector[Int]) = {
    in.foldLeft(0, 0, Vector.empty[Double]) { (x, y) =>
      val (count, prevSum, avgVector) = (x._1, x._2, x._3)
      val (newCount, newSum) = (count + 1, prevSum + y)
      (newCount, newSum, avgVector :+ newSum.toDouble / newCount)
    }
  }

}
