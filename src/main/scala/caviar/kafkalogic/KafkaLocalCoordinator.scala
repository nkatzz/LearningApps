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
import orl.kafkalogic.Types.{MergedTheory, TheoryAccumulated, TheoryResponse, WrapUp}
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.logic.Clause
import orl.learning.LocalCoordinator


object Types {
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int])
  class TheoryAccumulated(var theory: List[Clause])
  class MergedTheory(var theory: List[Clause])
  class WrapUp
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

 override def receive : PartialFunction[Any, Unit]= {

    case msg: RunSingleCore =>

      if (inps.weightLean) {
        workers = {
            List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaWoledASPLearner(inps, trainingDataOptions,
              testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))
        }
        become(waitResponse)
        data = getTrainingData
        writeExamplesToTopic(data, numOfActors, examplesPerIteration)
        for (worker <- workers) worker ! new Run
      }
    case _: LocalLearnerFinished => {
      if(terminate) {
        val averageLoss = avgLoss(errorCount)
        import scalatikz.pgf.plots.Figure
        Figure("AverageLoss")
          .plot((0 to currBatch) -> averageLoss._3)
          .havingXLabel("Batch Number (15 Examples 5 to each learner)")
          .havingYLabel("Average Loss")
          .show()
      }
      context.system.terminate()
    }
  }

  // Looks for a Rule in the merged theory that is the same as clauseA a clause from the accumulated theory from the learners
  def checkForEqualRule(clauseA: Clause, firstOccurrence: Int, lastOccurrence: Int):Boolean = {
    import scala.util.control.Breaks._
    var foundSameRule = false
    breakable {
      for (i <- firstOccurrence to lastOccurrence) {
        var foundDifferentRefinement = false
        breakable {
          for ( ref <- mergedTheory(i).refinements) {
            if(!clauseA.refinements.exists(x => x.thetaSubsumes(ref) && ref.thetaSubsumes(x))) {
              foundDifferentRefinement = true
              break
            }
          }
          // if no different refinement was found the rules are equal
          if (!foundDifferentRefinement) {
            foundSameRule = true
            break
          }
        }
      }
    }
    foundSameRule
  }

  /* In this method, the Coordinator will merge the rules from the theories of the Learners in the following way:
   *  -rules that are not already in the mergedTheory, are inserted as they are.
   *  -when a rule is already in the mergedTheory, it is inserted again and the weight of both rules
   *   (the rule that is already in the mergedTheory and the rule found to be the same) is averaged.
   *   The statistics (fps fns tps tns) are summed up. The same is done for their common refinements.
   */
  def mergeTheories: Receive = {
    case theoryAcc: TheoryAccumulated =>

      // for every rule in the theory accumulated from the workers
      for (clauseAcc <- theoryAcc.theory) {
        val firstOccurrence = mergedTheory.indexWhere(clause => clause.thetaSubsumes(clauseAcc) && clauseAcc.thetaSubsumes(clause))

        // if this rule is not in the merged theory add it as it is
        if (firstOccurrence == -1) {
          mergedTheory = mergedTheory :+ clauseAcc
        }
        else {
          var lastOccurrence = mergedTheory.lastIndexWhere(clause => clause.thetaSubsumes(clauseAcc) && clauseAcc.thetaSubsumes(clause))
          val foundSameRule = checkForEqualRule(clauseAcc, firstOccurrence, lastOccurrence)
          // if an exact same rule is found the weights and statistics should be merged but the rule should not be added to the theory again
          if(!foundSameRule) {
            var firstPart = mergedTheory.slice(0, lastOccurrence + 1)
            firstPart = firstPart :+ clauseAcc
            mergedTheory = firstPart ::: mergedTheory.slice(lastOccurrence + 1, mergedTheory.length)
            // one more rule was added next to the same ones
            lastOccurrence += 1
          }
          // merge the statistics
          mergeStats(clauseAcc, firstOccurrence, lastOccurrence)
        }
      }
      println("******************* MERGED THEORY **********************")
      for(clause <- mergedTheory) {
        println(clause.tostring )
        println ("with stats-> weight: " + clause.weight + " " + clause.tps)
      }

      /* If the Coordinator sees that the Examples have finished, he sends a MergedTheory message to
       * the Learners to get their latest theory, write it to the topic and terminate the Learning process
       */
      if(!terminate) {
        become(waitResponse)
        // if writeExamplesToTopic returns true it means the Examples have finished
        if(writeExamplesToTopic(data, numOfActors, examplesPerIteration)) terminate = true
        for (worker <- workers) worker ! new MergedTheory(mergedTheory)
        //writeTheoryToTopic(mergedTheory, theoryProd)
      } else {
        for (worker <- workers) worker ! new WrapUp
        //writeTheoryToTopic(mergedTheory, theoryProd)
        become(receive)
        self ! new LocalLearnerFinished
      }
  }

  def mergeStats(clauseAcc: Clause, firstOccurrence: Int, lastOccurrence: Int): Unit = {
    // it is the first time the theory is merged so keep the best stats in the same rules
    var mergedWeight: Double = 0.0
    var totalTps = 0
    var totalTns = 0
    var totalFps = 0
    var totalFns = 0
    if (currBatch == 1) {
      if (mergedTheory(firstOccurrence).tps < clauseAcc.tps) {
        mergedWeight = clauseAcc.weight
        totalTps = clauseAcc.tps
        totalTns = clauseAcc.tns
        totalFps = clauseAcc.fps
        totalFns = clauseAcc.fns
      } else {
        mergedWeight = mergedTheory(firstOccurrence).weight
        totalTps = mergedTheory(firstOccurrence).tps
        totalTns = mergedTheory(firstOccurrence).tns
        totalFps = mergedTheory(firstOccurrence).fps
        totalFns = mergedTheory(firstOccurrence).fns
      }
    }
    // else find how many new statistics we have and add them
    else {
      mergedWeight = (clauseAcc.weight + mergedTheory(firstOccurrence).weight) / 2
      totalTps = mergedTheory(firstOccurrence).tps + (clauseAcc.tps - mergedTheory(firstOccurrence).tps)
      totalTns = mergedTheory(firstOccurrence).tns + (clauseAcc.tns - mergedTheory(firstOccurrence).tns)
      totalFps = mergedTheory(firstOccurrence).fps + (clauseAcc.fps - mergedTheory(firstOccurrence).fps)
      totalFns = mergedTheory(firstOccurrence).fns + (clauseAcc.fns - mergedTheory(firstOccurrence).fns)
    }
    // set the same stats for all the same rules
    for (i <- firstOccurrence to lastOccurrence) {
      mergedTheory(i).weight = mergedWeight
      mergedTheory(i).tps = totalTps
      mergedTheory(i).tns = totalTns
      mergedTheory(i).fps = totalFps
      mergedTheory(i).fns = totalFns
    }
    mergeRefinements(clauseAcc, firstOccurrence, lastOccurrence)
  }

  def mergeRefinements(clauseAcc: Clause, firstOccurrence: Int, lastOccurrence: Int): Unit = {
    var mergedWeight: Double = 0.0
    var totalTps = 0
    var totalTns = 0
    var totalFps = 0
    var totalFns = 0
    // if it is the first time the theory is merged keep the statistics of the clause with more tps
    if (currBatch == 1) {
      for (ref <- clauseAcc.refinements) {
        val sameRefinement = findSameRefinement(ref, firstOccurrence, lastOccurrence)
        if( sameRefinement == null || sameRefinement.tps < ref.tps) {
          mergedWeight = ref.weight
          totalTps = ref.tps
          totalTns = ref.tns
          totalFps = ref.fps
          totalFns = ref.fns
        }
        else if (sameRefinement.tps > ref.tps) {
          mergedWeight = sameRefinement.weight
          totalTps = sameRefinement.tps
          totalTns = sameRefinement.tns
          totalFps = sameRefinement.fps
          totalFns = sameRefinement.fns
        }
        for(i <- firstOccurrence to lastOccurrence) {
          for( refM <- mergedTheory(i).refinements) {
            if( refM.thetaSubsumes(ref) && ref.thetaSubsumes(refM)) {
              refM.weight = mergedWeight
              refM.tps = totalTps
              refM.tns = totalTns
              refM.fps = totalFps
              refM.fns = totalFns
            }
          }
        }

      }
    } else {
      for (ref <- clauseAcc.refinements) {
        val sameRefinement = findSameRefinement(ref,firstOccurrence, lastOccurrence)
        if(sameRefinement != null) {
          mergedWeight = (ref.weight + sameRefinement.weight) / 2
          totalTps = sameRefinement.tps + (ref.tps - sameRefinement.tps)
          totalTns = sameRefinement.tns + (ref.tns - sameRefinement.tns)
          totalFps = sameRefinement.fps + (ref.fps - sameRefinement.fps)
          totalFns = sameRefinement.fns + (ref.fns - sameRefinement.fns)
          for(i <- firstOccurrence to lastOccurrence) {
            for( refM <- mergedTheory(i).refinements) {
              if( refM.thetaSubsumes(ref) && ref.thetaSubsumes(refM)) {
                refM.weight = mergedWeight
                refM.tps = totalTps
                refM.tns = totalTns
                refM.fps = totalFps
                refM.fns = totalFns
              }
            }
          }
        }
      }
    }
  }

  def findSameRefinement(ref: Clause, firstOccurrence: Int, lastOccurrence: Int): Clause = {
    val sameRules = mergedTheory.slice(firstOccurrence, lastOccurrence+1)
    var returnRef: Clause = null
    for(rule <- sameRules) {
      val sameRefinement = rule.refinements.indexWhere(clause => clause.thetaSubsumes(ref) && ref.thetaSubsumes(clause))
      if (sameRefinement != -1) returnRef = rule.refinements(sameRefinement)
    }
    ref
  }



  /* Every time a worker finishes processing a batch, he sends its local theory to the coordinator
   * and when the coordinator has collected numOfActors theories, he merges the theories and then
   * sends a new batch of Examples to each worker
   */

  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      if (mergedTheory.isEmpty) mergedTheory = theoryRes.theory
      else localTheories ++= theoryRes.theory

      if(responseCount == 0){
        errorCount = theoryRes.perBatchError
      } else {
        errorCount = errorCount.zip(theoryRes.perBatchError).map(t => t._1 + t._2)
      }
      responseCount += 1
      println("Error "+ theoryRes.perBatchError)
      println("Coordinator received Theory From Learner with length: " + theoryRes.theory.length)
      if (responseCount == numOfActors) {
        responseCount = 0
        println(avgLoss(errorCount))
        currBatch += 1
        become(mergeTheories)
        self ! new TheoryAccumulated(localTheories)
        localTheories = List()
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


