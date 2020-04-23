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
import orl.kafkalogic.Types.{CollectDelta, CollectZ, Continue, Increment, MergedTheory, NewRound, NewSubRound, Run, TheoryResponse, WarmUpFinished, WrapUp, ZCollection}
import orl.learning.Types.{FinishedBatch, LocalLearnerFinished, StartOver}
import orl.utils.Utils.underline
import orl.learning.woledasp.WoledASPLearner
import orl.logic.Clause
import FgmUtils._

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

  var warmUp = false
  var quantum: Double = 0.0
  var counter: Int = 0
  var zeta: Double = 0.0
  var currentEstimate: List[Clause] = List()

  private val workerId = self.path.name.slice(self.path.name.indexOf("_") + 1, self.path.name.length)

  //  Creates a Theory producer that writes the expanded theory to the Theory Topic
  //val theoryProducer: KafkaProducer[String, Array[Byte]] = createTheoryProducer(workerId)

  /* Create an Example Consumer for the getNextBatch method that consumes exactly one
   * Example from the topic. If the topic is empty it returns an empty Example to simulate
   * the end of the Iterator
   */
  val exampleConsumer: KafkaConsumer[String, Example] = createExampleConsumer(workerId)
  exampleConsumer.subscribe(Collections.singletonList("ExamplesTopic"))

  val duration: Duration = Duration.ofMillis(1000)

 @scala.annotation.tailrec
  final def getRecord: Example = {
    val records = exampleConsumer.poll(duration)
    if (!records.isEmpty) {
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value.observations.head +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())
      }
      records.asScala.head.value
    } else getRecord
  }


  /* for the time being the local coordinator first writes the Examples to the topic and
   * then starts the learner so the learner consumes examples one by one until it has no more
   * to read.
   */
  /*def getRecord: Example = {
    val records = exampleConsumer.poll(duration)
    if(records.isEmpty) new Example()
    else records.asScala.head.value()
  }*/

  //private def getTrainingData = trainingDataFunction(trainingDataOptions)
  private def getNextBatch: Example = {
    getRecord
  }

 override def receive: PartialFunction[Any, Unit] = {
    case msg: Run =>
      warmUp = msg.warmUp
      become(controlState)
      start()
  }

  override def start(): Unit = {
    this.repeatFor -= 1
    if(warmUp) self ! getNextBatch
  }

  /* When the Learner fails to read examples, it means that the batch the Coordinator sent
   * has finished. So the Learner sends its local theory to the Coordinator via a TheoryResponse
   * message. Then the Learner waits for a MergedTheory message, to get the theory the Coordinator
   * sent by merging the theories of all the Learners.
   */
  override def controlState: Receive = {
    case exmpl: Example =>
      if (exmpl.isEmpty) {
        println("worker " + workerId + " sends theory after " + batchCount + " Examples")
        context.parent ! new TheoryResponse(state.initiationRules ++ state.terminationRules, state.perBatchError)
      } else {
        become(processingState)
        self ! exmpl
      }

    case _: Continue =>
      self ! getNextBatch

    case _: CollectZ =>
      val currentWeights = getCurrentWeights(state.getAllRules(inps,"all"), currentEstimate)
      zeta = safeZoneFunction(currentWeights,getWeightVector(currentEstimate), sqrt(8*exp(-4)))
      context.parent ! new ZCollection(zeta)

    case _: CollectDelta =>
      val estimateWeights = getWeightVector(currentEstimate)
      val currentWeights = getCurrentWeights(state.getAllRules(inps,"top"), currentEstimate)
      val deltaVector = (currentWeights zip estimateWeights).map{case (x,y) => x - y}
      context.parent ! deltaVector

    case round: NewRound =>
      currentEstimate = round.newEstimate
      state.updateRules(round.newEstimate,"replace",inps)
      quantum = round.theta
      counter = 0
      zeta = sqrt(8*exp(-4))
      self ! getNextBatch

    case round: NewSubRound =>
      val currentWeights = getCurrentWeights(state.getAllRules(inps,"top"), currentEstimate)
      zeta = safeZoneFunction(currentWeights,getWeightVector(currentEstimate), sqrt(8*exp(-4)))
      quantum = round.theta
      counter = 0
      self ! getNextBatch

    case _: FinishedBatch =>
      // This is the place to do any control checks that
      // may require communication with the coordinator (or other actors).
      // This is why we separate between control and processing state, so that we
      // may do any necessary checks right after a data chunk has been processed.
      // For now, just get the next data chunk.
      if(batchCount % communicateAfter == 0) {
        if(warmUp) {
          context.parent ! new WarmUpFinished(state.initiationRules ++ state.terminationRules)
          shutDown()
        } else {
          val currentWeights = getCurrentWeights(state.getAllRules(inps,"top"), currentEstimate)
          val increment = ((zeta - safeZoneFunction(currentWeights,getWeightVector(currentEstimate), 8*exp(-4)))/quantum).toInt
          if(increment > counter) {
              val incrementMessage = new Increment(increment - counter)
              counter = increment
            context.parent ! incrementMessage
          } else self ! getNextBatch
        }
      }  else self ! getNextBatch


    case _: WrapUp =>
      wrapUp()
      context.parent ! new LocalLearnerFinished

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
      exampleConsumer.commitAsync()
      become(controlState)
      self ! new FinishedBatch
  }

  override def shutDown(): Unit = {
    //theoryProducer.close()
    exampleConsumer.close()
    self ! PoisonPill
    if(!warmUp)context.parent ! new LocalLearnerFinished
  }
}
