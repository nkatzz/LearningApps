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

package caviar.kafkalogic.simplemerge

import java.io.{File, FileWriter, PrintWriter}

import akka.actor.{ActorRef, Props}
import caviar.kafkalogic.MyInputHandling.getMongoData
import caviar.kafkalogic.simplemerge.Types.{MergedTheory, StartMerging, TheoryResponse}
import org.apache.spark.util.SizeEstimator.estimate
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.{InputSource, MongoDataOptions}
import orl.kafkalogic.ProdConsLogic.writeExmplItersToTopic
import orl.learning.LocalCoordinator
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.logic.Clause

object Types {
  class MergedTheory(val theory: List[Clause])
  class TheoryResponse(var theory: List[Clause], val perBatchError: Vector[Int], val workerId: Int, val finished: Boolean)
  class StartMerging
}

class KafkaMergeLocalCoordinator[T <: InputSource](numOfActors: Int, communicateAfter: Int, inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) extends LocalCoordinator(inps, trainingDataOptions,
                                                                          testingDataOptions, trainingDataFunction,
                                                                          testingDataFunction) {

  import context.become

  val t1 = System.nanoTime

  private def getTrainingData = trainingDataFunction(trainingDataOptions)

  // The Coordinator gets the data and shares them among the Learners via a Kafka Topic
  private var data: Vector[Iterator[Example]] = Vector()

  var terminate = false
  var currRound = 0

  // Initiate numOfActors KafkaWoledASPLearners
  private val workers: List[ActorRef] =
    List.tabulate(numOfActors)(n => context.actorOf(Props(new KafkaNCWoledASPLearner(communicateAfter, inps, trainingDataOptions,
                                                                                     testingDataOptions, trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}_${n}"))

  private var finishedWorkers = 0
  var errorCount = new Array[Vector[Int]](numOfActors)

  val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
  val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))
  val pw3 = new PrintWriter(new FileWriter(new File("ExecutionTimes"), true))
  val pw4 = new PrintWriter(new FileWriter(new File("CommunicationCost"), true))

  val addError: (Vector[Int], Vector[Int]) => Vector[Int] = (error1: Vector[Int], error2: Vector[Int]) => {
    (error1 zip error2).map{ case (x, y) => x + y }
  }

  var communicationCost: BigDecimal = 0.0
  var responseCount = 0
  var mergedTheory: List[Clause] = List()
  var theoryAccumulated: List[Clause] = List()

  override def receive: PartialFunction[Any, Unit] = {

    // When the coordinator start, it writes all the examples to the topic in a round robin fashion
    case msg: RunSingleCore =>
      data = getMongoData(trainingDataOptions.asInstanceOf[MongoDataOptions])
      writeExmplItersToTopic(data, numOfActors, 2)
      become(waitResponse)
      workers foreach (_ ! new Run)

    case _: LocalLearnerFinished => {
      val duration = (System.nanoTime - t1) / 1e9d
      pw3.write(s"Simple Merge time: $duration\n")
      pw3.flush()
      pw3.close()

      pw4.write(s"Simple Merge communication cost: $communicationCost\n")
      pw4.flush()
      pw4.close()

      val mergedErrorCount = errorCount.reduceLeft((x, y) => addError(x, y))
      val averageLoss = avgLoss(mergedErrorCount)
      val accumulatedMistakes = mergedErrorCount.scanLeft(0.0)(_ + _).tail
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

  /*
   * When a Learner processes communicateAfter Examples it sends its Local theory
   * and error to the coordinator. For the time being the coordinator does nothing
   * with the data received.
   */
  def waitResponse: Receive = {
    case _: StartMerging =>
      var newMergedTheory: List[Clause] = List()
      // first merge the already existing rules and their refinements
      for (clause <- mergedTheory) {
        // exact same rules get merged
        val SameRules = theoryAccumulated.filter(x => x.thetaSubsumes(clause) && clause.thetaSubsumes(x) && areExactSame(x, clause))
        if (SameRules.nonEmpty) {
          theoryAccumulated = theoryAccumulated.filter(x => !(x.thetaSubsumes(clause) && clause.thetaSubsumes(x) && areExactSame(x, clause)))
          val newClause = mergeStats(clause, SameRules)
          newMergedTheory = newMergedTheory :+ newClause
        }
      }
      // new rules generated are added to the merged theory
      for (rule <- theoryAccumulated) {
        if (!newMergedTheory.exists(x => x.thetaSubsumes(rule) && rule.thetaSubsumes(x) && areExactSame(x, rule))) {
          newMergedTheory = newMergedTheory :+ rule
        }
      }
      mergedTheory = newMergedTheory
      workers.foreach(worker => {
        val merThe = new MergedTheory(mergedTheory)
        communicationCost += estimate(merThe)
        worker ! merThe
      })

    case theoryRes: TheoryResponse =>
      if (theoryRes.finished) {
        finishedWorkers += 1
        if (finishedWorkers == numOfActors) {
          become(receive)
          self ! new LocalLearnerFinished
        }
      } else {
        communicationCost += estimate(theoryRes)
        // All learners sent their accumulated error and theories
        theoryAccumulated = theoryAccumulated ::: theoryRes.theory
        errorCount(theoryRes.workerId) = theoryRes.perBatchError
        responseCount += 1
        println("phga na mpw")
      }
      if (responseCount == numOfActors - finishedWorkers) {
        println("mpgha")
        responseCount = 0
        currRound += 1
        self ! new StartMerging
      }
  }

  def avgLoss(in: Vector[Int]) = {
    in.foldLeft(0, 0, Vector.empty[Double]) { (x, y) =>
      val (count, prevSum, avgVector) = (x._1, x._2, x._3)
      val (newCount, newSum) = (count + numOfActors, prevSum + y)
      (newCount, newSum, avgVector :+ newSum.toDouble / newCount)
    }
  }

  def areExactSame(clause1: Clause, clause2: Clause): Boolean = {
    var foundDifferentRefinement = false
    for (ref1 <- clause1.refinements) {
      if (!clause2.refinements.exists(ref2 => ref2.thetaSubsumes(ref1) && ref1.thetaSubsumes(ref2))) {
        foundDifferentRefinement = true
      }
    }
    if (!foundDifferentRefinement) true
    else false
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
      if (currRound == 1) {
        if (clause.weight < sameRule.weight) newWeight = sameRule.weight
        if (clause.tps < sameRule.tps) {
          newTps = sameRule.tps
          newTns = sameRule.tns
          newFps = sameRule.fps
          newFns = sameRule.fns
        }
      } else {
        if ((sameRule.tps - clause.tps) <= 0) {

        } else {
          exactSameRules += 1
          newWeight += sameRule.weight
          newTps += (sameRule.tps - clause.tps)
          newTns += (sameRule.tns - clause.tns)
          newFps += (sameRule.fps - clause.fps)
          newFns += (sameRule.fns - clause.fns)
        }
      }
    }
    clause.weight = newWeight / exactSameRules
    clause.tps = newTps
    clause.tns = newTns
    clause.fps = newFps
    clause.fns = newFns
    clause
  }

}
