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

package orl.kafkalogic

import java.io.{File, FileWriter, PrintWriter}

import akka.actor.{ActorRef, Props}
import caviar.kafkalogic.KafkaWoledASPLearner
import orl.datahandling.InputHandling.InputSource
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.MongoDataOptions
import caviar.kafkalogic.MyInputHandling.getMongoData
import orl.kafkalogic.ProdConsLogic.{writeExmplItersToTopic}
import orl.kafkalogic.Types.{CollectDelta, CollectZ, DeltaCollected, DeltaCollection, FinishedLearner, Increment, NewRound, NewSubRound, Run, WarmUpFinished, WarmUpRound, ZCollected, ZCollection}
import orl.learning.Types.{LocalLearnerFinished, RunSingleCore}
import orl.logic.Clause
import orl.learning.LocalCoordinator
import caviar.kafkalogic.FgmUtils.{getNewEstimate, updateEstimateWeights}
import org.apache.spark.util.SizeEstimator._

import math._

object Types {
  class Run(val warmUp: Boolean)
  class Increment(val value: Int, val round: Int, val subRound: Int)
  class NewRound(val newEstimate: List[Clause], val theta: Double)
  class NewSubRound(val theta: Double)
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int])
  class TheoryAccumulated(var theory: List[Clause])
  class MergedTheory(var theory: List[Clause])
  class WrapUp
  class WarmUpRound
  class WarmUpFinished(val WarmUpTheory: List[Clause], val perBatchError: Vector[Int])
  class CollectZ
  class ZCollection(val value: Double)
  class ZCollected
  class CollectDelta
  class DeltaCollected
  class DeltaCollection(val deltaVector: Vector[Double], val newRules: List[Clause])
  class Continue
  class FinishedLearner(val perBatchError: Vector[Int], val workerId: Int)
}

class KafkaLocalCoordinator[T <: InputSource](numOfActors: Int, examplesPerIteration: Int, inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends LocalCoordinator(inps, trainingDataOptions,
                                                                          testingDataOptions, trainingDataFunction,
                                                                          testingDataFunction) {

  import context.become
  import orl.kafkalogic.Types

  //private def getTrainingData = trainingDataFunction(trainingDataOptions)

  val t1 = System.nanoTime

  // The Coordinator gets the data and shares them among the Learners via a Kafka Topic
  private var data = Vector[Iterator[Example]]()

  //val theoryProd = createTheoryProducer()

  var upstreamCost = 0
  var downstreamCost = 0

  // Initiate numOfActors KafkaWoledASPLearners
  private var workers: List[ActorRef] = List()

  var currentEstimate: List[Clause] = List()
  var counter: Int = 0
  var y: Double = 0.0
  var theta: Double = 0.0

  var currentRound: Int = 0
  var currentSubRound: Int = 0

  private var finishedWorkers = 0

  var errorCount = new Array[Vector[Int]](numOfActors)

  val threshold = 3

  var zCollection: Vector[Double] = Vector()
  var deltaCollection: List[Vector[Double]] = List()
  var newRules: List[Clause] = List()

  var communicationCost: BigDecimal = 0.0

  var mergedErrorCount: Vector[Int] = Vector()

  override def receive: PartialFunction[Any, Unit] = {

    case msg: RunSingleCore =>
      data = getMongoData(trainingDataOptions.asInstanceOf[MongoDataOptions])
      writeExmplItersToTopic(data, numOfActors, 2)
      //data = getTrainingData
      //writeExamplesToTopic(data, 2)
      val warmUpLearner = context.actorOf(Props(new KafkaWoledASPLearner(150, inps, trainingDataOptions,
                                                                              testingDataOptions, trainingDataFunction, testingDataFunction)), name = "warmUpLearner")
      warmUpLearner ! new Run(true)
      warmUpLearner ! new WarmUpRound

    case warmUp: WarmUpFinished =>
      currentEstimate = warmUp.WarmUpTheory
      mergedErrorCount = warmUp.perBatchError
      y = sqrt(threshold)
      theta = y / (2 * numOfActors)
      currentRound = 1

      workers = {
        List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaWoledASPLearner(40, inps, trainingDataOptions,
                                                                                           testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))
      }

      become(waitIncrement)

      for (worker <- workers) {
        worker ! new Run(false)
        worker ! new NewRound(currentEstimate, theta)
      }

    case _: LocalLearnerFinished => {

      val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
      val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))
      val pw3 = new PrintWriter(new FileWriter(new File("ExecutionTimes"), true))
      val pw4 = new PrintWriter(new FileWriter(new File("CommunicationCost"), true))
      val duration = (System.nanoTime - t1) / 1e9d
      pw3.write(s"FGM time: $duration\n")
      pw3.flush()
      pw3.close()

      pw4.write(s"FGM communication cost: $communicationCost\n")
      pw4.flush()
      pw4.close()

      val addError: (Vector[Int], Vector[Int]) => Vector[Int] = (error1, error2) => {
        (error1 zip error2).map{ case (x, y) => x + y }
      }
      mergedErrorCount = mergedErrorCount ++ errorCount.reduceLeft((x, y) => addError(x, y))
      val accumulatedMistakes = mergedErrorCount.scanLeft(0)(_ + _).tail
      val averageLoss = avgLoss(mergedErrorCount)
      for (i <- averageLoss._3) pw.write(i + " ")
      pw.write("\n")
      pw.flush()
      pw.close()
      for (i <- accumulatedMistakes) pw2.write(i + " ")
      pw2.write("\n")
      pw2.flush()
      pw2.close()
      import scalatikz.pgf.plots.Figure
      Figure("AverageLoss")
        .plot((0 to averageLoss._3.length - 1) -> averageLoss._3)
        .havingXLabel("Batch Number")
        .havingYLabel("Average Loss")
        .show()
      Figure("AccumulatedError")
        .plot((0 to accumulatedMistakes.length - 1) -> accumulatedMistakes)
        .havingXLabel("Batch Number")
        .havingYLabel("Accumulated Mistakes")
        .show()
      context.system.terminate()
    }
  }

  def waitIncrement: Receive = {
    case incr: Increment =>
      communicationCost += estimate(incr)
      if (incr.round == currentRound && incr.subRound == currentSubRound) {
        counter += incr.value
        if (counter > numOfActors) {
          // request and collect
          become(collectFromSites)
          workers.foreach(x => x ! new CollectZ)
        }
      }

    case finished: FinishedLearner =>
      finishedWorkers += 1
      errorCount(finished.workerId) = finished.perBatchError
      if (finishedWorkers == numOfActors) {
        become(receive)
        self ! new LocalLearnerFinished
      }

  }

  def collectFromSites: Receive = {
    case finished: FinishedLearner =>
      finishedWorkers += 1
      errorCount(finished.workerId) = finished.perBatchError
      if (finishedWorkers == numOfActors) {
        become(receive)
        self ! new LocalLearnerFinished
      }

    case coll: ZCollection =>
      communicationCost += estimate(coll)
      zCollection = zCollection :+ coll.value
      if (zCollection.length == (numOfActors - finishedWorkers)) self ! new ZCollected

    case _: ZCollected =>
      y = zCollection.foldLeft(0.0)(_ + _)
      zCollection = Vector()
      if (y <= 0.05 * numOfActors * sqrt(threshold)) {
        workers.foreach(worker => worker ! new CollectDelta)
      } else {
        counter = 0
        theta = y / (2 * numOfActors)
        become(waitIncrement)
        val newSubRound = new NewSubRound(theta)
        communicationCost += numOfActors * estimate(newSubRound)
        println("\n\n\n******************************************************************* STARTING NEW SUBROUND **************************************************************************\n\n\n")
        workers.foreach(x => x ! newSubRound)
        currentSubRound += 1
      }

    case coll: DeltaCollection =>
      communicationCost += estimate(coll)
      deltaCollection = deltaCollection :+ coll.deltaVector
      val rulesToAdd = coll.newRules.filter(rule => !currentEstimate.exists(estimateRule => estimateRule.uuid == rule.uuid) &&
        !newRules.exists(newRule => newRule.uuid == rule.uuid))
      newRules = newRules ++ rulesToAdd
      if (deltaCollection.length == (numOfActors - finishedWorkers)) self ! new DeltaCollected

    case _: DeltaCollected =>
      val newWeights = getNewEstimate(deltaCollection)
      currentEstimate = updateEstimateWeights(currentEstimate, newWeights)
      currentEstimate = currentEstimate ::: newRules
      newRules = List()
      counter = 0
      y = sqrt(threshold)
      theta = y / (2 * numOfActors)
      currentRound += 1
      currentSubRound = 0
      deltaCollection = List()
      println("\n\n\n********************************************************************************* STARTING NEW ROUND *******************************************************************************************************\n\n\n")
      val newRound = new NewRound(currentEstimate, theta)
      communicationCost += numOfActors * estimate(newRound)
      workers.foreach(_ ! newRound)
      become(waitIncrement)
  }

  def avgLoss(in: Vector[Int]) = {
    in.foldLeft(0, 0, Vector.empty[Double]) { (x, y) =>
      val (count, prevSum, avgVector) = (x._1, x._2, x._3)
      val newCount = if (count >= 150) count + numOfActors else count + 1
      val newSum = prevSum + y
      (newCount, newSum, avgVector :+ newSum.toDouble / newCount)
    }
  }
}

