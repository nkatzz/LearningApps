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

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.time.Duration
import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import oled.app.runutils.InputHandling.InputSource
import oled.app.runutils.RunningOptions
import oled.datahandling.Example
import oled.inference.{ASPSolver, MAPSolver}
import oled.learning.{Learner, LearningUtils, State}
import oled.logic.{Clause, Literal, LogicUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.slf4j.LoggerFactory
import oled.kafkalogic.ProdConsLogic.{createExampleConsumer, createTheoryProducer, writeExamplesToTopic, writeTheoryToTopic}
import oled.kafkalogic.Types.{TheoryResponse, MergedTheory}
import oled.learning.Types.{FinishedBatch, LocalLearnerFinished, StartOver}
import oled.learning.structure.OldStructureLearningFunctions.{generateNewRules, growNewRuleTest}
import oled.learning.structure.{OldStructureLearningFunctions, RuleExpansion}
import oled.learning.weights.Test2
import oled.utils.Utils.{underline, underlineStars}
import org.apache.kafka.clients.producer.KafkaProducer


class KafkaLearner[T <: InputSource](
    inps: RunningOptions,
    trainingDataOptions: T,
    testingDataOptions: T,
    trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example])
  extends Learner(
    inps: RunningOptions, trainingDataOptions: T,
    testingDataOptions: T,
    trainingDataFunction: T => Iterator[Example],
    testingDataFunction: T => Iterator[Example]) {

  import context.become

  private val startTime = System.nanoTime()
  private val logger = LoggerFactory.getLogger(self.path.name)
  private var repeatFor = inps.repeatFor
  private var data = Iterator[Example]()
  private var state = new State(inps)

  private var inertiaAtoms = Set.empty[Literal]
  private var batchCount = 0
  private var withHandCrafted = false

  private val workerId = self.path.name.slice(self.path.name.indexOf("_") + 1, self.path.name.length)

  //  Creates a Theory producer that writes the expanded theory to the Theory Topic
  //val theoryProducer: KafkaProducer[String, Array[Byte]] = createTheoryProducer(workerId)

  /* Create an Example Consumer for the getNextBatch method that consumes exactly one
   * Example from the topic. If the topic is empty it returns an empty Example to simulate
   * the end of the Iterator
   */
  val exampleConsumer: KafkaConsumer[String, Example] = createExampleConsumer(workerId)
  exampleConsumer.subscribe(Collections.singletonList("ExamplesTopic"))

  val duration: Duration = Duration.ofMillis(100)

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

  /*def getRecord: Example = {
    val records = exampleConsumer.poll(duration)
    if(records.isEmpty) new Example()
    else records.asScala.head.value()
  }*/

  //private def getTrainingData = trainingDataFunction(trainingDataOptions)
  private def getNextBatch: Example = {
    getRecord
  }

  override def start(): Unit = {
    this.repeatFor -= 1
    self ! getNextBatch
  }

  override def controlState: Receive = {
    case exmpl: Example =>
      if (exmpl.isEmpty) {
        wrapUp()
        context.parent ! new LocalLearnerFinished
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
      state.updateRules(mergedTheory.theory, "replace", inps)
      self ! new FinishedBatch


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
      // self ! new FinishedBatch
  }

  override def process(exmpl: Example): Unit = {

    //logger.info("\n"+underline(s"*** BATCH $batchCount *** "))

    var e = Example()

    var rules = List.empty[Clause]
    var inferredState = Map.empty[String, Boolean]
    var tpCounts = 0
    var fpCounts = 0
    var fnCounts = 0
    var totalGroundings = 0
    var rulesCompressed = List.empty[Clause]
    var inferenceTime = 0.0
    var scoringTime = 0.0

    if (batchCount == 76) {
      val stop = "stop"
    }

    if (inps.weightLean) {

      e = LearningUtils.dataToMLNFormat(exmpl, inps)

      rules = state.getAllRules(inps, "all").filter(x => x.body.nonEmpty)
      //rules = state.getAllRules(inps, "top")

/***** DEBUG *****/
      val all = rules.flatMap(x => x.refinements :+ x)
      if (all.size > 50) {
        val stop = "stop"
        val r = Test2.transform(rules)
        val stopAgain = ""
      }
/***** DEBUG *****/

      rulesCompressed = LogicUtils.compressTheory(rules)
      //rulesCompressed = LogicUtils.compressTheoryKeepMoreSpecific(rules)

      val map = oled.utils.Utils.time(MAPSolver.solve(rulesCompressed, e, this.inertiaAtoms, inps))
      val mapInfResult = map._1
      inferenceTime = map._2

      inferredState = mapInfResult._1

      // Doing this in parallel is trivial (to speed things up in case of many rules/large batches).
      // Simply split the rules to multiple workers, the grounding/counting tasks executed are completely rule-independent.
      //println("      Scoring...")

      val scoring = oled.utils.Utils.time {
        LearningUtils.scoreAndUpdateWeights(e, inferredState,
                                            state.getAllRules(inps, "all").toVector, inps, logger, batchCount = batchCount)
      }
      val (_tpCounts, _fpCounts, _fnCounts, _totalGroundings, _inertiaAtoms) = scoring._1
      scoringTime = scoring._2

      tpCounts = _tpCounts
      fpCounts = _fpCounts
      fnCounts = _fnCounts
      totalGroundings = _totalGroundings
      inertiaAtoms = _inertiaAtoms.toSet

      /*=============== OLED ================*/
    } else {
      e = exmpl
      rulesCompressed = state.getBestRules(inps.globals, "score") //.filter(x => x.score(inps.scoringFun) >= 0.9)
      if (rulesCompressed.nonEmpty) {
        val inferredState = ASPSolver.crispLogicInference(rulesCompressed, e, inps.globals)
        val (_tpCounts, _fpCounts, _fnCounts, _totalGroundings, _inertiaAtoms) =
          LearningUtils.scoreAndUpdateWeights(e, inferredState, state.getAllRules(inps, "all").toVector, inps, logger)
        tpCounts = _tpCounts
        fpCounts = _fpCounts
        fnCounts = _fnCounts
        totalGroundings = _totalGroundings
        inertiaAtoms = _inertiaAtoms.toSet
      } else {
        fnCounts = e.queryAtoms.size
      }
    }

    updateStats(tpCounts, fpCounts, fnCounts)

    this.inertiaAtoms = inertiaAtoms
    this.inertiaAtoms = Set.empty[Literal] // Use this to difuse inertia

    state.perBatchError = state.perBatchError :+ (fpCounts + fnCounts)

    logger.info(batchInfoMsg(rulesCompressed, tpCounts, fpCounts, fnCounts, inferenceTime, scoringTime))

    //logger.info(s"\n${state.perBatchError}")
    //logger.info(s"\nFPs: $fpCounts, FNs: $fnCounts")

    if (!withHandCrafted) {
      state.totalGroundings += totalGroundings
      state.updateGroundingsCounts(totalGroundings)

      var newInit = List.empty[Clause]
      var newTerm = List.empty[Clause]

      if (fpCounts > 0 || fnCounts > 0) {
        //if (fpCounts > 2 || fnCounts > 2) {

        /*if (!inps.weightLean) {
          val topInit = state.initiationRules.filter(_.body.nonEmpty)
          val topTerm = state.terminationRules.filter(_.body.nonEmpty)
          val growNewInit = OldStructureLearningFunctions.growNewRuleTest(topInit, e, inps.globals, "initiatedAt")
          val growNewTerm = OldStructureLearningFunctions.growNewRuleTest(topTerm, e, inps.globals, "terminatedAt")
          //newInit = if (growNewInit) OldStructureLearningFunctions.generateNewRulesOLED(topInit, e, "initiatedAt", inps.globals) else Nil
          //newTerm = if (growNewTerm) OldStructureLearningFunctions.generateNewRulesOLED(topTerm, e, "terminatedAt", inps.globals) else Nil
          newInit = OldStructureLearningFunctions.generateNewRulesOLED(topInit, e, "initiatedAt", inps.globals) //if (growNewInit) generateNewRules(topInit, e, "initiatedAt", inps.globals) else Nil
          newTerm = OldStructureLearningFunctions.generateNewRulesOLED(topTerm, e, "terminatedAt", inps.globals) //if (growNewTerm) generateNewRules(topTerm, e, "terminatedAt", inps.globals) else Nil
        }*/

        //This is the "correct one" so far.
        val theory = rulesCompressed
        val newRules = OldStructureLearningFunctions.generateNewRules(theory, e, inps)
        val (init, term) = newRules.partition(x => x.head.predSymbol == "initiatedAt")

        newInit = init //.filter(p => !state.isBlackListed(p))
        newTerm = term //.filter(p => !state.isBlackListed(p))

        val allNew = newInit ++ newTerm
        if (allNew.nonEmpty) LearningUtils.showNewRulesMsg(fpCounts, fnCounts, allNew, logger)
        state.updateRules(newInit ++ newTerm, "add", inps)

      }

      val newRules = newInit ++ newTerm

      // score the new rules and update their weights
      val newRulesWithRefs = newRules.flatMap(x => x.refinements :+ x).toVector
      LearningUtils.scoreAndUpdateWeights(e, inferredState, newRulesWithRefs, inps, logger, newRules = true)

      /* Rules' expansion. */
      // We only need the top rules for expansion here.
      val init = state.initiationRules
      val term = state.terminationRules
      val expandedTheory = RuleExpansion.expandRules(init ++ term, inps, logger)
      context.parent ! new TheoryResponse(expandedTheory)
      //writeTheoryToTopic(expandedTheory, theoryProducer)

      //state.updateRules(expandedTheory._1, "replace", inps)

      //val pruningSpecs = new PruningSpecs(0.8, 2, 100)
      //val pruned = state.pruneRules(pruningSpecs, inps, logger)
    }
  }

  override def wrapUp() = {
    logger.info(s"\nFinished the data")
    if (repeatFor > 0) {
      self ! new StartOver
    } else if (repeatFor == 0) {
      val theory = state.getAllRules(inps, "top")

      showStats(theory)

      if (trainingDataOptions != testingDataOptions) { // test set given, eval on that
        val testData = testingDataFunction(testingDataOptions)
        LearningUtils.evalOnTestSet(testData, theory, inps)
      }
      //theoryProducer.close()
      exampleConsumer.close()
      shutDown()

    } else { // Just to be on the safe side...
      throw new RuntimeException("This should never have happened (repeatFor is negative).")
    }
  }

  override def batchInfoMsg(theoryForPrediction: List[Clause], tpCounts: Int, fpCounts: Int,
                   fnCounts: Int, inferenceTime: Double, scoringTime: Double) = {

    val batchMsg = underlineStars(s"*** BATCH $batchCount ***")
    val theoryMsg = underline(s"TPs: $tpCounts, FPs: $fpCounts, FNs: $fnCounts. Inference time: $inferenceTime, scoring time: $scoringTime. Theory used for prediction:")
    val theory = {
      if (inps.weightLean) {
        theoryForPrediction.map(x => s"${x.tostring} | W: ${format(x.weight)} | Precision: ${format(x.precision)} | (TPs,FPs): (${x.tps}, ${x.fps}) ").mkString("\n")
      } else {
        theoryForPrediction.map(x => s"${x.tostring} | Precision: ${format(x.precision)} | (TPs,FPs): (${x.tps}, ${x.fps}) ").mkString("\n")

      }
    }
    if (theoryForPrediction.nonEmpty) s"\n$batchMsg\n$theoryMsg\n$theory" else s"*** BATCH $batchCount ***"
  }

  override def updateStats(tpCounts: Int, fpCounts: Int, fnCounts: Int) = {
    state.totalTPs += tpCounts
    state.totalFPs += fpCounts
    state.totalFNs += fnCounts
  }
}
