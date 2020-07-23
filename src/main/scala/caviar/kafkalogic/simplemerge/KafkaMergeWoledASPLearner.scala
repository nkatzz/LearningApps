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

import java.time.Duration

import akka.actor.PoisonPill
import caviar.kafkalogic.simplemerge.Types.{MergedTheory, TheoryResponse}
import org.apache.kafka.clients.consumer.KafkaConsumer
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.InputSource
import orl.kafkalogic.ProdConsLogic.createExampleConsumer
import orl.kafkalogic.Types.NewRound
import orl.learning.Types.{FinishedBatch, StartOver}
import orl.learning.woledasp.WoledASPLearner
import orl.logic.Clause
import orl.utils.Utils.underline
import caviar.kafkalogic.FgmUtils.{format => myFormat}

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

  val duration: Duration = Duration.ofMillis(10000)

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
      println("EKANA POLLAK")
      if (records.isEmpty) {
        println("AUTO EGINE")
        Example()
      } else {
        println("AUTO SIGOYRA IXUI")
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
    case mergedTheory: MergedTheory =>
      state.updateRules(copyEstimate(mergedTheory.theory), "replace", inps)
      self ! getNextBatch

    case exmpl: Example =>
      if (exmpl.isEmpty) {
        val theory = state.getAllRules(inps, "all")
        context.parent ! new TheoryResponse(theory, state.perBatchError, workerId.toInt, false)
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
        println("skatoyles ")
        val theory = state.getAllRules(inps, "all")
        context.parent ! new TheoryResponse(theory, state.perBatchError, workerId.toInt, false)
      } else self ! getNextBatch

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
    println("H MAMA MOY DEN ME AFHNEI NA KLEISW")
    exampleConsumer.close()
    context.parent ! new TheoryResponse(List(), state.perBatchError, workerId.toInt, true)
    self ! PoisonPill
  }

  def copyEstimate(estimate: List[Clause]): List[Clause] = {
    val copyList = estimate.map(rule => {
      val cp = new Clause(rule.head, rule.body, uuid = rule.uuid)
      cp.parentClause = rule.parentClause
      cp.isBottomRule = rule.isBottomRule
      cp.isTopRule = rule.isTopRule
      cp.weight = myFormat(rule.weight)
      cp.subGradient = rule.subGradient
      cp.mistakes = rule.mistakes
      cp.adamGradient = rule.adamGradient
      cp.adamSquareSubgradient = rule.adamSquareSubgradient
      cp.tps = rule.tps
      cp.fps = rule.fps
      cp.fns = rule.fns
      cp.tns = rule.tns
      cp.actualGroundings = rule.actualGroundings
      cp.seenExmplsNum = rule.seenExmplsNum

      cp.supportSet = rule.supportSet.map(r => {
        val newR = new Clause(r.head, r.body, r.uuid)
        newR.parentClause = r.parentClause
        newR.isBottomRule = r.isBottomRule
        newR.isTopRule = r.isTopRule
        newR.weight = myFormat(r.weight)
        newR.subGradient = r.subGradient
        newR.mistakes = r.mistakes
        newR.adamGradient = r.adamGradient
        newR.adamSquareSubgradient = r.adamSquareSubgradient
        newR.tps = r.tps
        newR.fps = r.fps
        newR.fns = r.fns
        newR.tns = r.tns
        newR.actualGroundings = r.actualGroundings
        newR.seenExmplsNum = r.seenExmplsNum
        newR
      })

      cp.refinements = rule.refinements.map(ref => {
        val newRef = new Clause(ref.head, ref.body, ref.uuid)
        newRef.parentClause = ref.parentClause
        newRef.isBottomRule = ref.isBottomRule
        newRef.isTopRule = ref.isTopRule
        newRef.weight = myFormat(ref.weight)
        newRef.subGradient = ref.subGradient
        newRef.mistakes = ref.mistakes
        newRef.adamGradient = ref.adamGradient
        newRef.adamSquareSubgradient = ref.adamSquareSubgradient
        newRef.tps = ref.tps
        newRef.fps = ref.fps
        newRef.fns = ref.fns
        newRef.tns = ref.tns
        newRef.actualGroundings = ref.actualGroundings
        newRef.seenExmplsNum = ref.seenExmplsNum
        newRef
      })
      cp
    })
    copyList
  }

}

