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
import org.apache.commons.math3.optimization.Weight
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

  val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
  val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))

 override def receive : PartialFunction[Any, Unit]= {

    case msg: RunSingleCore =>

      if (inps.weightLean) {
        val warmUpLearner =  context.actorOf(Props(new KafkaWoledASPLearner(inps, trainingDataOptions,
          testingDataOptions, trainingDataFunction, testingDataFunction)), name = "warmupLearner")
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
      var theoryAccumulated = theoryAcc.theory
      var newMergedTheory: List[Clause] = List()

      // first merge the already existing rules and their refinements
      var i = 0
      for (clause <- mergedTheory) {
        i+=1
        // exact same rules get merged
        val SameRules = theoryAccumulated.filter(x => x.thetaSubsumes(clause) && clause.thetaSubsumes(x) && areExactSame(x,clause))
        if(SameRules.nonEmpty) {
          mergedTheory
          theoryAccumulated = theoryAccumulated.filter(x => !(x.thetaSubsumes(clause) && clause.thetaSubsumes(x) && areExactSame(x,clause)))
          val newClause = mergeStats(clause, SameRules)
          newMergedTheory = newMergedTheory :+ newClause
        }
      }
      // new rules generated are added to the merged theory
      for(rule <- theoryAccumulated) {
        if(!newMergedTheory.exists(x => x.thetaSubsumes(rule) && rule.thetaSubsumes(x) && areExactSame(x,rule))) {
          newMergedTheory = newMergedTheory :+ rule
        }
      }
      mergedTheory = newMergedTheory

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

  def mergeStats(clause: Clause, sameRules: List[Clause]): Clause = {

    var exactSameRules = 1
    var newWeight = clause.weight
    var newTps = clause.tps
    var newTns = clause.tns
    var newFps = clause.fps
    var newFns = clause.fns
    for (sameRule <- sameRules) {
      // if the rules are exactly the same merge their statistics and weight and add only once to the new Merged theory
        if(currBatch ==1) {
          if(clause.weight < sameRule.weight) newWeight = sameRule.weight
          if(clause.tps < sameRule.tps)
            {
              newTps = sameRule.tps
              newTns = sameRule.tns
              newFps = sameRule.fps
              newFns = sameRule.fns
            }
        } else {
          exactSameRules += 1
          newWeight += sameRule.weight
          if((sameRule.tps -clause.tps) < 0 )
            {
              println("should not happen")
            }
          newTps += (sameRule.tps -clause.tps)
          newTns += (sameRule.tns -clause.tns)
          newFps += (sameRule.fps -clause.fps)
          newFns += (sameRule.fns -clause.fns)
        }
      }
    clause.weight = newWeight/exactSameRules
    clause.tps = newTps
    clause.tns = newTns
    clause.fps = newFps
    clause.fns = newFns
    // mergeRefinements(clause, sameRules)
    clause
  }

  def mergeRefinements(clause: Clause, sameRules: List[Clause]): Unit = {
    if(clause.refinements.length == 2) {
      if( clause.refinements(0).thetaSubsumes(clause.refinements(1)) && clause.refinements(1).thetaSubsumes(clause.refinements(0))){
        println("this should never happen but happens")
      }
    }
    for (ref <- clause.refinements) {
      var sameRefinements = 1
      var newWeight = ref.weight
      var newTps = ref.tps
      var newTns = ref.tns
      var newFps = ref.fps
      var newFns = ref.fns
      for (rule <- sameRules) {
        for( ref2 <- rule.refinements) {
          if(ref2.thetaSubsumes(ref) && ref.thetaSubsumes(ref2)) {
            if (currBatch == 1) {
              if (ref2.weight > newWeight) newWeight = ref2.weight
              if(ref2.tps >newTps) {
                newTps = ref2.tps
                newTns = ref2.tns
                newFps = ref2.fps
                newFns = ref2.fns
              }
            }
            else {
              sameRefinements += 1
              newWeight += ref2.weight
              newTps += (ref2.tps - ref.tps)
              newTns += (ref2.tns - ref.tns)
              newFps += (ref2.fps - ref.fps)
              newFns += (ref2.fns - ref.fns)
            }
          }
        }
      }
      ref.weight = newWeight/sameRefinements
      ref.tps = newTps
      ref.tns = newTns
      ref.fps = newFps
      ref.fns = newFns
    }
  }

  // Checks if two rules have the same refinements
  def areExactSame(clause1: Clause, clause2: Clause): Boolean = {
    var foundDifferentRefinement = false
    for( ref1 <- clause1.refinements) {
      if (!clause2.refinements.exists(ref2 => ref2.thetaSubsumes(ref1) && ref1.thetaSubsumes(ref2))) {
       foundDifferentRefinement = true
      }
    }
    if(!foundDifferentRefinement) true
    else false
  }

  /* Every time a worker finishes processing a batch, he sends its local theory to the coordinator
   * and when the coordinator has collected numOfActors theories, he merges the theories and then
   * sends a new batch of Examples to each worker
   */

  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      localTheories ++= theoryRes.theory

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


