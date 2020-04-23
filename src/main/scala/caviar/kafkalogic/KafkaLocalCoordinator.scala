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
import orl.kafkalogic.ProdConsLogic.{createTheoryProducer, writeExamplesToTopic, writeTheoryToTopic}
import orl.kafkalogic.Types.{Run, CollectDelta, CollectZ, Continue, DeltaCollected, DeltaCollection, Increment, MergedTheory, NewRound, NewSubRound, TheoryAccumulated, TheoryResponse, WarmUpFinished, WarmUpRound, WrapUp, ZCollected, ZCollection}
import orl.learning.Types.{LocalLearnerFinished, RunSingleCore}
import orl.logic.Clause
import orl.learning.LocalCoordinator
import caviar.kafkalogic.FgmUtils.{getNewEstimate, getSumOfDelta, getWeightVector, updateEstimateWeights}

import math._


object Types {
  class Run(val warmUp: Boolean)
  class Increment(val value: Int)
  class NewRound(val newEstimate: List[Clause],val theta: Double)
  class NewSubRound(val theta: Double)
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int])
  class TheoryAccumulated(var theory: List[Clause])
  class MergedTheory(var theory: List[Clause])
  class WrapUp
  class WarmUpRound
  class WarmUpFinished(val WarmUpTheory: List[Clause])
  class CollectZ
  class ZCollection(val value: Double)
  class ZCollected
  class CollectDelta
  class DeltaCollected
  class DeltaCollection(val deltaVector: Vector[Double],val newRules: List[Clause])
  class Continue
}

class KafkaLocalCoordinator[T <: InputSource](numOfActors: Int, examplesPerIteration: Int, inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends
  LocalCoordinator(inps, trainingDataOptions,
  testingDataOptions, trainingDataFunction,
  testingDataFunction) {

  import context.become
  import orl.kafkalogic.Types

  private def getTrainingData = trainingDataFunction(trainingDataOptions)

  // The Coordinator gets the data and shares them among the Learners via a Kafka Topic
  private var data = Iterator[Example]()

  var mergedTheory: List[Clause] = List()

  /* Every time the Coordinator accumulates the Learners' theories, it merges them to a List[Clause]
   *  and writes it to a Kafka Topic.
   */
  val theoryProd = createTheoryProducer()

  var terminate = false

  // Initiate numOfActors KafkaWoledASPLearners
  private var workers: List[ActorRef] = List()

  private var localTheories: List[Clause] = List()
  private var responseCount = 0
  private var errorCount: Vector[Int] = Vector[Int]()
  private var currBatch = 0
  var currentEstimate: List[Clause] = List()
  var counter : Int = 0
  var y : Double = 0.0
  var theta: Double = 0.0

  val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
  val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))

  var zCollection: Vector[Double] = Vector()
  var deltaCollection: List[Vector[Double]] = List()
  var newRules: List[Clause] = List()
  var sitesWithUpdatedParams = 0

 override def receive : PartialFunction[Any, Unit]= {

    case msg: RunSingleCore =>
      data = getTrainingData
      writeExamplesToTopic(data)
      val warmUpLearner =  context.actorOf(Props(new KafkaWoledASPLearner(30,inps, trainingDataOptions,
        testingDataOptions, trainingDataFunction, testingDataFunction)), name = "warmUpLearner")
      warmUpLearner ! new Run(true)
      warmUpLearner ! new WarmUpRound

    case warmUp: WarmUpFinished =>
      currentEstimate = warmUp.WarmUpTheory
      y = sqrt(8 * exp(-4))
      theta = y / (2 * numOfActors)

      workers = {
        List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaWoledASPLearner(15,inps, trainingDataOptions,
          testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))
      }

      become(waitIncrement)

      for (worker <- workers)
      {
        worker ! new Run(false)
        worker ! new NewRound(currentEstimate, theta)
      }

    case _: LocalLearnerFinished => {
      if(terminate) {
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
      }
      context.system.terminate()
    }
  }

  def waitIncrement: Receive = {
    case incr: Increment =>
      counter += incr.value
      if (counter > numOfActors) {
        // request and collect
        become(collectFromSites)
        workers.foreach(x => x ! new CollectZ)
      } else sender() ! new Continue

  }

  def collectFromSites: Receive = {
    case coll: ZCollection =>
      zCollection = zCollection :+ coll.value
      if(zCollection.length == numOfActors) self ! new ZCollected

    case _:ZCollected =>
      y = 0
      zCollection.foreach(x => y += x)
      zCollection = Vector()
      if(y <= 0.01* numOfActors* sqrt(8*exp(-4))) {
        workers.foreach(worker => worker ! new CollectDelta)
      } else {
        counter = 0
        theta = y/(2* numOfActors)
        become(waitIncrement)
        workers.foreach(x => x ! new NewSubRound(theta))
      }

    case coll: DeltaCollection =>
      deltaCollection = deltaCollection :+ coll.deltaVector
      newRules = newRules ::: coll.newRules.filter(rule =>
        !newRules.exists(nRule => nRule.thetaSubsumes(rule) && rule.thetaSubsumes(nRule) &&
          !currentEstimate.exists(nRule => nRule.thetaSubsumes(rule) && rule.thetaSubsumes(nRule))))
      if(deltaCollection.length == numOfActors) self ! new DeltaCollected

    case _: DeltaCollected =>
      deltaCollection.foreach(deltaVector => if(deltaVector.exists(_ != 0)){
        sitesWithUpdatedParams += 1
      })
      val sumOfDeltas = getSumOfDelta(deltaCollection)
      val newWeights = getNewEstimate(getWeightVector(currentEstimate),sumOfDeltas,sitesWithUpdatedParams)
      currentEstimate = updateEstimateWeights(currentEstimate,newWeights)
      currentEstimate = currentEstimate ::: newRules
      newRules = List()




  }


  def avgLoss(in: Vector[Int]) = {
    in.foldLeft(0, 0, Vector.empty[Double]) { (x, y) =>
      val (count, prevSum, avgVector) = (x._1, x._2, x._3)
      val (newCount, newSum) = (count + 1, prevSum + y)
      (newCount, newSum, avgVector :+ newSum.toDouble / newCount)
    }
  }
}


