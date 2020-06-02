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

package caviar.kafkalogic

import java.time.Duration
import java.util.Collections

import akka.actor.PoisonPill

import scala.collection.JavaConverters._
import orl.datahandling.InputHandling.InputSource
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import org.apache.kafka.clients.consumer.KafkaConsumer
import orl.kafkalogic.ProdConsLogic.createExampleConsumer
import orl.kafkalogic.Types.{CollectDelta, CollectZ, Continue, DeltaCollection, FinishedLearner, Increment, MergedTheory, NewRound, NewSubRound, Run, TheoryResponse, WarmUpFinished, WrapUp, ZCollection}
import orl.learning.Types.{FinishedBatch, LocalLearnerFinished, StartOver}
import orl.utils.Utils.underline
import orl.learning.woledasp.WoledASPLearner
import orl.logic.Clause
import FgmUtils._
import org.apache.kafka.common.TopicPartition

import math._

class KafkaWoledASPLearner[T <: InputSource](
    communicateAfter: Int,
    inps: RunningOptions,
    trainingDataOptions: T,
    testingDataOptions: T,
    trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example])
  extends WoledASPLearner(
    inps,
    testingDataOptions,
    trainingDataOptions,
    trainingDataFunction,
    testingDataFunction) {

  import context.become

  // round is used for the case that a learner sends an increment for round X while the coordinator has started a new round X + 1
  // In this case, the increment sent should be ignored by the coordinator ( i guess ) and the Learner should start round X + 1
  var currentRound: Int = 0
  var currentSubRound: Int = 0
  var warmUp = false
  var quantum: Double = 0.0
  var counter: Int = 0
  var zeta: Double = 0.0
  var currentEstimate: List[Clause] = List()
  val threshold = 5
  private val workerId = self.path.name.slice(self.path.name.indexOf("_") + 1, self.path.name.length)

  val exampleConsumer: KafkaConsumer[String, Example] = createExampleConsumer(workerId)


  val duration: Duration = Duration.ofMillis(3000)

  private def getNextBatch: Example = {
    if(!data.isEmpty) data.next()
    else {
      exampleConsumer.commitAsync()
      val records = exampleConsumer.poll(duration)
      records.forEach(record => println("Worker: " + workerId+ " read example with head: " + record.value.observations.head +
        "at Offset: " + record.offset() + ", Partition: " + record.partition()))

      if(records.isEmpty) {
        Example()
      } else {
        records.forEach(record => data = data ++ Iterator(record.value()))
        data.next()
      }
    }
  }

 override def receive: PartialFunction[Any, Unit] = {
    case msg: Run =>
      warmUp = msg.warmUp
      if(!warmUp) {
        val topicPartitions = List(new TopicPartition("ExamplesTopic", workerId.toInt)).asJava
        exampleConsumer.assign(topicPartitions)
      } else exampleConsumer.subscribe(Collections.singletonList("ExamplesTopic"))
      become(controlState)
      start()
  }

  override def start(): Unit = {
    this.repeatFor -= 1
    if(warmUp) self ! getNextBatch
  }

  override def controlState: Receive = {

    case _: CollectZ =>
      val updatedWeights = getCurrentWeights(state.initiationRules ++ state.terminationRules, currentEstimate)
      val estimateWeights = getWeightVector(currentEstimate)
      zeta = safeZoneFunction(updatedWeights,estimateWeights, threshold)
      context.parent ! new ZCollection(zeta)

    case _: CollectDelta =>
      val deltaVector = getDeltaVector(state.initiationRules ++ state.terminationRules, currentEstimate)
      val newRules = (state.initiationRules ++ state.terminationRules).filter(rule => !currentEstimate.exists(estimateRule => rule.## == estimateRule.##))
      context.parent ! new DeltaCollection(deltaVector, newRules)

    case round: NewRound =>
      currentEstimate = round.newEstimate
      state.updateRules(copyEstimate(round.newEstimate),"replace",inps)
      quantum = round.theta
      counter = 0
      zeta = sqrt(threshold)
      currentRound +=1
      currentSubRound = 0
      if( batchCount == 1) self ! getNextBatch

    case round: NewSubRound =>
      val updatedWeights = getCurrentWeights(state.initiationRules ++ state.terminationRules, currentEstimate)
      zeta = safeZoneFunction(updatedWeights,getWeightVector(currentEstimate), threshold)

      quantum = round.theta
      counter = 0
      currentSubRound += 1

    case exmpl: Example =>
      if (exmpl.isEmpty) {
        wrapUp()
      } else {
        become(processingState)
        self ! exmpl
      }

    case _: FinishedBatch =>
      // This is the place to do any control checks that
      // may require communication with the coordinator (or other actors).
      // This is why we separate between control and processing state, so that we
      // may do any necessary checks right after a data chunk has been processed.
      // For now, just get the next data chunk.
      if(batchCount % communicateAfter == 0) {
        if(warmUp) {
          context.parent ! new WarmUpFinished(state.initiationRules ++ state.terminationRules, state.perBatchError)
          shutDown()
        } else {
          val updatedWeights =  getCurrentWeights(state.initiationRules ++ state.terminationRules, currentEstimate)
          val estimateWeights = getWeightVector(currentEstimate)
          val increment = ((zeta - safeZoneFunction(updatedWeights,estimateWeights, threshold))/quantum).toInt
          if(increment > counter) {
            val incrementMessage = new Increment(increment - counter, currentRound, currentSubRound)
            counter = increment
            context.parent ! incrementMessage
          }
        }
      }
      self ! getNextBatch


    case _: StartOver =>
      logger.info(underline(s"Starting a new training iteration (${this.repeatFor} iterations remaining.)"))
      state.finishedIterationInfo(logger)
      become(controlState)
      start()
  }

  override def processingState: Receive = {
    case exmpl: Example =>
      process(exmpl)
      batchCount += 1
      become(controlState)
      self ! new FinishedBatch
  }

  override def shutDown(): Unit = {
    exampleConsumer.close()
    if(!warmUp)context.parent ! new FinishedLearner(state.perBatchError, workerId.toInt)
    self ! PoisonPill

  }
}
