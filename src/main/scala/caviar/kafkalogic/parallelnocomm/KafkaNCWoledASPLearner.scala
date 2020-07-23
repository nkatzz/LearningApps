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
import caviar.kafkalogic.parallelnocomm.Types.{FinishedLearner, TheoryResponse}
import orl.learning.Types.{FinishedBatch, StartOver}
import orl.learning.woledasp.WoledASPLearner
import orl.utils.Utils.underline

class KafkaNCWoledASPLearner[T <: InputSource](
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

  private val workerId = self.path.name.slice(self.path.name.indexOf("_") + 1, self.path.name.length)

  // One consumer is created for each Learner and is responsible for one partition
  val exampleConsumer: KafkaConsumer[String, Example] = createExampleConsumer(workerId)

  val duration: Duration = Duration.ofMillis(500)

  /*
   * Examples are kept in val data. If data is empty the Learner tries to get records from the topic.
   * For test purposes if the Learner cannot read from the topic it Wraps Up and signals the coordinator
   * that it has finished processing the examples
   */
  private def getNextBatch: Example = {
    if (!data.isEmpty) data.next()
    else {
      exampleConsumer.commitAsync()
      val records = exampleConsumer.poll(duration)
      records.forEach(record => println("Worker: " + workerId + " read example with head: " + record.value.observations.head +
        "at Offset: " + record.offset() + ", Partition: " + record.partition()))

      if (records.isEmpty) {
        Example()
      } else {
        records.forEach(record => data = data ++ Iterator(record.value()))
        data.next()
      }
    }
  }

  override def start(): Unit = {
    this.repeatFor -= 1
    self ! getNextBatch
  }

  override def controlState: Receive = {
    case exmpl: Example =>
      if (exmpl.isEmpty) {
        val theory = state.getAllRules(inps, "all")
        context.parent ! new TheoryResponse(theory, state.perBatchError, workerId.toInt)
        wrapUp()
      } else {
        become(processingState)
        self ! exmpl
      }

    /*
     * Every communicateAfter examples processed the Learner sends its local theory
     * and perBatch error to the coordinator. the workerId is used for identification
     */
    case _: FinishedBatch =>
      // This is the place to do any control checks that
      // may require communication with the coordinator (or other actors).
      // This is why we separate between control and processing state, so that we
      // may do any necessary checks right after a data chunk has been processed.
      // For now, just get the next data chunk.
      if (batchCount % communicateAfter == 0) {
        val theory = state.getAllRules(inps, "all")
        context.parent ! new TheoryResponse(theory, state.perBatchError, workerId.toInt)
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
    context.parent ! new FinishedLearner
    self ! PoisonPill
  }
}
