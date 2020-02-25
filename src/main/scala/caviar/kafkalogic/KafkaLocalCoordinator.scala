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

package oled.kafkalogic

import akka.actor.{Actor, ActorRef, Props}
import oled.app.runutils.InputHandling.InputSource
import oled.app.runutils.RunningOptions
import oled.datahandling.Example
import oled.kafkalogic.ProdConsLogic.{createTheoryProducer, writeExamplesToTopic, writeTheoryToTopic}
import oled.kafkalogic.Types.{MergedTheory, TheoryAccumulated, TheoryResponse}
import oled.learning.LocalCoordinator
import oled.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import oled.logic.Clause

object Types {
  class TheoryResponse(var theory: (List[Clause], Boolean))
  class TheoryAccumulated(var theory: List[(List[Clause], Boolean)])
  class MergedTheory(var theory: List[Clause])
}

class KafkaLocalCoordinator[T <: InputSource](numOfActors: Int, inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends LocalCoordinator(inps: RunningOptions, trainingDataOptions: T,
  testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
  testingDataFunction: T => Iterator[Example]) {

  import context.become
  import oled.kafkalogic.Types

  private def getTrainingData = trainingDataFunction(trainingDataOptions)
  private var data = Iterator[Example]()
  val theoryProd = createTheoryProducer()

  // Initiate numOfActors KafkaLearners
  private val workers = List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaLearner(inps, trainingDataOptions,
    testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))

  override def receive: PartialFunction[Any, Unit] = {

    case msg: RunSingleCore =>

      for (worker <- workers) worker ! new Run

      become(waitResponse)

      data = getTrainingData
      writeExamplesToTopic(data, numOfActors)

    //val worker: ActorRef = context.actorOf(Props(new KafkaLearner(inps, trainingDataOptions,
    //  testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")

    //worker ! new Run

    case _: LocalLearnerFinished =>
      context.system.terminate()
      theoryProd.close()
  }

  import scala.util.control.Breaks._
  var insertClause = true
  def sendExamples: Receive = {
    case theoryAcc: TheoryAccumulated =>

      var mergedTheory: List[Clause] = List()
      print("\n\n********* Theory accumulated *********\n\n")

      // For every clause in the theories accumulated from the Learners
      for(theoryA <- theoryAcc.theory) {
        for(clauseA <- theoryA._1) {
          insertClause = true
          // For every clause in the merged theory (starts as an empty List)
          for(clauseM <- mergedTheory) {
            // if the clause is already in the merged theory
            if( clauseM.thetaSubsumes(clauseA) && clauseA.thetaSubsumes(clauseM)) {
              // check for refinements not already in the merged theory
              var newRefinements = List()
              var foundRefinement = false
              for(ref <- clauseA.refinements) {
                for(ref2 <- clauseM.refinements) {
                  if( ref.thetaSubsumes(ref2) && ref2.thetaSubsumes(ref)) {
                    foundRefinement = true
                  }
                }
                if (!foundRefinement) clauseM.refinements ::= ref
              }
              insertClause = false
            }
          }
          // if the clause was not found in the merged theory insert it
          if (insertClause) mergedTheory ::= clauseA
        }
      }
      println("THIS IS THE MERGED THEORY "+ mergedTheory)
      for (worker <- workers) worker ! new MergedTheory(mergedTheory)
      writeTheoryToTopic(mergedTheory, theoryProd)

      become(waitResponse)
      writeExamplesToTopic(data, numOfActors)
  }

  /* Every time a worker finishes processing a batch, he sends its local theory to the coordinator
   * and when the coordinator has collected numOfActors theories, he merges the theories and then
   * sends a new Example to each worker
   */

  private var localTheories: List[(List[Clause], Boolean)] = List()
  private var responseCount = 0
  def waitResponse: Receive = {
    case theoryRes: TheoryResponse =>
      responseCount += 1
      localTheories ::= theoryRes.theory
      println("LOCAL LEARNER THEORY RECEIVED" + theoryRes.theory._1)
      if (responseCount == numOfActors) {
        responseCount = 0
        become(sendExamples)
        self ! new TheoryAccumulated(localTheories)
        localTheories = List()
      }
  }

}
