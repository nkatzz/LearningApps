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

import java.time.Duration
import java.util.Collections

import akka.actor.PoisonPill
import org.apache.kafka.clients.consumer.KafkaConsumer
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.InputSource
import orl.kafkalogic.ProdConsLogic.createExampleConsumer
import orl.kafkalogic.Types.{MergedTheory, TheoryResponse, WrapUp}
import orl.learning.Types.{FinishedBatch, LocalLearnerFinished, StartOver}
import orl.learning.woledasp.WoledASPLearner
import orl.utils.Utils.underline

import scala.collection.JavaConverters._

class KafkaNCWoledASPLearner[T <: InputSource](
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

  private val workerId = self.path.name.slice(self.path.name.indexOf("_") + 1, self.path.name.length)

  //  Creates a Theory producer that writes the expanded theory to the Theory Topic
  //val theoryProducer: KafkaProducer[String, Array[Byte]] = createTheoryProducer(workerId)

  /* Create an Example Consumer for the getNextBatch method that consumes exactly one
   * Example from the topic. If the topic is empty it returns an empty Example to simulate
   * the end of the Iterator
   */
  val exampleConsumer: KafkaConsumer[String, Example] = createExampleConsumer(workerId)
  exampleConsumer.subscribe(Collections.singletonList("ExamplesTopic"))

  val duration: Duration = Duration.ofMillis(5000)

  /* for the time being the local coordinator first writes the Examples to the topic and
   * then starts the learner so the learner consumes examples one by one until it has no more
   * to read.
   */
  def getRecord: Example = {
    val records = exampleConsumer.poll(duration)
    if(records.isEmpty) new Example()
    else records.asScala.head.value()
  }

  //private def getTrainingData = trainingDataFunction(trainingDataOptions)
  private def getNextBatch: Example = {
    getRecord
  }

  override def start(): Unit = {
    this.repeatFor -= 1
    self ! getNextBatch
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
        context.parent ! new TheoryResponse(List(), state.perBatchError)
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
      self ! getNextBatch


    case mergedTheory: MergedTheory =>
      self ! getNextBatch

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
    context.parent ! new LocalLearnerFinished
  }
}
