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
import orl.kafkalogic.ProdConsLogic.writeExamplesToTopic
import caviar.kafkalogic.parallelnocomm.Types.{FinishedLearner, TheoryResponse}
import orl.learning.LocalCoordinator
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.logic.Clause


object Types {
  class FinishedLearner
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int], val workerId: Int)
}

class KafkaNCLocalCoordinator[T <: InputSource](numOfActors: Int, communicateAfter: Int, inps: RunningOptions, trainingDataOptions: T,
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
  private val workers: List[ActorRef] =
    List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaNCWoledASPLearner(communicateAfter, inps, trainingDataOptions,
    testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))

  private var finishedWorkers = 0
  var errorCount =new Array[Vector[Int]](numOfActors)

  val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
  val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))

  val addError = (error1: Vector[Int], error2: Vector[Int]) => {
    (error1 zip error2). map{case (x,y) => x + y}
  }

 override def receive : PartialFunction[Any, Unit]= {

    // When the coordinator start, it writes all the examples to the topic in a round robin fashion
    case msg: RunSingleCore =>
      data = getTrainingData
      writeExamplesToTopic(data)
      become(waitResponse)
      workers foreach ( _ ! new Run)

    case _: LocalLearnerFinished => {
      val mergedErrorCount =  errorCount.reduceLeft( (x,y) => addError(x,y))
      val averageLoss = avgLoss(mergedErrorCount)
      val accumulatedMistakes = mergedErrorCount.scanLeft(0.0)(_ + _).tail
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
        .havingXLabel("Batch Number")
        .havingYLabel("Average Loss")
        .show()
      Figure("AccumulatedError")
        .plot((0 to accumulatedMistakes.length-1) -> accumulatedMistakes)
        .havingXLabel("Batch Number")
        .havingYLabel("Accumulated Mistakes")
        .show()
      context.system.terminate()
    }
  }

  /*
   * When a Learner processes communicateAfter Examples it sends its Local theory
   * and error to the coordinator. For the time being the coordinator does nothing
   * with the data received.
   */
  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      // All learners sent their accumulated error and theories
      errorCount(theoryRes.workerId) = theoryRes.perBatchError
    case _: FinishedLearner => {
      finishedWorkers += 1
      if(finishedWorkers == numOfActors) {
        become(receive)
        self ! new LocalLearnerFinished
      }
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
