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
  class TheoryResponse(var theory: List[Clause])
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

  /* Every time the Coordinator accumulates the Learners' theories, it merges them to a List[Clause]
   *  and writes it to a Kafka Topic.
   */
  val theoryProd = createTheoryProducer()

  var terminate = false

  // Initiate numOfActors KafkaWoledASPLearners
  private var workers: List[ActorRef] = List()

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
    case _: LocalLearnerFinished => context.system.terminate()
  }

  /* In this method, the Coordinator will merge the rules from the theories of the Learners in the following way:
   *  -rules that are not already in the mergedTheory, are inserted as they are.
   *  -when a rule is already in the mergedTheory, it is inserted again and the weight of both rules
   *   (the rule that is already in the mergedTheory and the rule found to be the same) is averaged.
   *   The statistics (fps fns tps tns) are summed up. The same is done for their common refinements.
   */
  def mergeTheories: Receive = {
    case theoryAcc: TheoryAccumulated =>

      var mergedTheory: List[Clause] = List()

      for(clauseA <- theoryAcc.theory) {
        val firstOccurrence = mergedTheory.indexWhere(clause => clause.thetaSubsumes(clauseA) && clauseA.thetaSubsumes(clause))
        if (firstOccurrence == -1) {
          mergedTheory ::= clauseA
        } else {
          val lastOccurrence = mergedTheory.lastIndexWhere(clause => clause.thetaSubsumes(clauseA) && clauseA.thetaSubsumes(clause))
          val mergedWeight = (clauseA.weight + mergedTheory(firstOccurrence).weight) / 2
          val total_tps = clauseA.tps + mergedTheory(firstOccurrence).tps
          val total_tns = clauseA.tns + mergedTheory(firstOccurrence).tns
          val total_fps = clauseA.fps + mergedTheory(firstOccurrence).fps
          val total_fns = clauseA.fns + mergedTheory(firstOccurrence).fns
          var firstPart= mergedTheory.slice(0, firstOccurrence) ::: mergedTheory.slice(firstOccurrence, lastOccurrence)
          firstPart ::= clauseA
          mergedTheory = firstPart ::: mergedTheory.slice(lastOccurrence, mergedTheory.length)
          for(i <- firstOccurrence to lastOccurrence + 1) {
            mergedTheory(i).weight = mergedWeight
            mergedTheory(i).tps = total_tps
            mergedTheory(i).tns = total_tns
            mergedTheory(i).fps = total_fps
            mergedTheory(i).fns = total_fns
          }

          // now merge the statistics and weights of common refinements
          for (refA <- mergedTheory(lastOccurrence +1).refinements) {
            var foundSameRefinement = false
            var i = firstOccurrence
            while (i <= lastOccurrence && !foundSameRefinement) {
              val sameRefinement = mergedTheory(i).refinements.indexWhere(clause => clause.thetaSubsumes(refA) && refA.thetaSubsumes(clause))
              if (sameRefinement != -1) {
                foundSameRefinement = true
                var mergedWeight = (refA.weight + mergedTheory(i).refinements(sameRefinement).weight) / 2
                var total_tps = refA.tps + mergedTheory(i).refinements(sameRefinement).tps
                var total_tns = refA.tns + mergedTheory(i).refinements(sameRefinement).tns
                var total_fps = refA.fps + mergedTheory(i).refinements(sameRefinement).fps
                var total_fns = refA.fns + mergedTheory(i).refinements(sameRefinement).fns
                for(i <- firstOccurrence to lastOccurrence + 1) {
                  for (refM <- mergedTheory(i).refinements) {
                    if (refM.thetaSubsumes(refA) && refA.thetaSubsumes(refM)) {
                      refM.weight = mergedWeight
                      refM.tps = total_tps
                      refM.tns = total_tns
                      refM.fps = total_fps
                      refM.fns = total_fns
                    }
                  }
                }
              }
              i += 1
            }
          }
        }
      }

      println("******************* MERGED THEORY **********************")
      for(clause <- mergedTheory) {
        println(clause.head+ " := " +clause.body )
        println ("with stats-> weight: " + clause.weight + " " + clause.tps)
      }
/*
      // For every clause in the theories accumulated from the Learners
        for (clauseA <- theoryAcc.theory) {
          // For every clause in the merged theory (starts as an empty List)
          for (clauseM <- mergedTheory) {
            // if the clause is already in the merged theory
            if (clauseM.thetaSubsumes(clauseA) && clauseA.thetaSubsumes(clauseM)) {
              // average their weights and sum up their statistics
              mergeRules(clauseA, clauseM)
              // do the same for the common refinements
              for (refA <- clauseA.refinements) {
                for (refM <- clauseM.refinements) {
                  if (refA.thetaSubsumes(refM) && refM.thetaSubsumes(refA)) {
                    mergeRules(refA, refM)
                  }
                }
              }
            }
          }
          //insert the clause to the merged theory
          mergedTheory ::= clauseA
        }

 */

      /* If the Coordinator sees that the Examples have finished, he sends a MergedTheory message to
       * the Learners to get their latest theory, write it to the topic and terminate the Learning process
       */
      if(!terminate) {
        become(waitResponse)
        // if writeExamplesToTopic returns true it means the Examples have finished
        if(writeExamplesToTopic(data, numOfActors, examplesPerIteration)) terminate = true
        for (worker <- workers) worker ! new MergedTheory(mergedTheory)
        writeTheoryToTopic(mergedTheory, theoryProd)
      } else {
        for (worker <- workers) worker ! new WrapUp
        writeTheoryToTopic(mergedTheory, theoryProd)
        become(receive)
        self ! new LocalLearnerFinished
      }
  }

  def mergeRules(ruleA: Clause, ruleB: Clause): Unit = {
    ruleA.weight = (ruleA.weight + ruleB.weight)/2
    ruleA.tps = ruleA.tps + ruleB.tps
    ruleA.tns = ruleA.tns + ruleB.tns
    ruleA.fps = ruleA.fps + ruleB.fps
    ruleA.fns = ruleA.fns + ruleB.fns
    ruleB.weight = (ruleA.weight + ruleB.weight)/2
    ruleB.tps = ruleA.tps + ruleB.tps
    ruleB.tns = ruleA.tns + ruleB.tns
    ruleB.fps = ruleA.fps + ruleB.fps
    ruleB.fns = ruleA.fns + ruleB.fns
  }

  /* Every time a worker finishes processing a batch, he sends its local theory to the coordinator
   * and when the coordinator has collected numOfActors theories, he merges the theories and then
   * sends a new batch of Examples to each worker
   */

  private var localTheories: List[Clause] = List()
  private var responseCount = 0
  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      responseCount += 1
      localTheories ++= theoryRes.theory
      println("Coordinator received Theory From Learner with length: " + theoryRes.theory.length)
      if (responseCount == numOfActors) {
        responseCount = 0
        become(mergeTheories)
        self ! new TheoryAccumulated(localTheories)
        localTheories = List()
      }
  }

}
