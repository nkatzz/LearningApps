package caviar.kafkalogic.othertests

import akka.actor.Props
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.datahandling.InputHandling.InputSource
import orl.learning.Types.{LocalLearnerFinished, Run, RunSingleCore}
import orl.learning.oled.OLEDLearner
import orl.learning.woledasp.WoledASPLearner
import orl.learning.woledmln.WoledMLNLearner
import orl.learning.LocalCoordinator

class SingleTestLocalCoordinator[T <: InputSource](inps: RunningOptions, trainingDataOptions: T,
                                          testingDataOptions: T, trainingDataFunction: T => Iterator[Example],
                                          testingDataFunction: T => Iterator[Example]) extends
  LocalCoordinator(inps, trainingDataOptions,
    testingDataOptions, trainingDataFunction,
    testingDataFunction) {

  override   def receive = {

    case msg: RunSingleCore =>

      if (inps.weightLean) {

        val worker = {
          if (mode == "MLN") {
            context.actorOf(Props(
              new WoledMLNLearner(inps, trainingDataOptions, testingDataOptions,
                trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")
          } else {
            context.actorOf(Props(
              new Learner(inps, trainingDataOptions, testingDataOptions,
                trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")
          }
        }
        worker ! new Run

      } else {

        val worker = context.actorOf(Props(
          new OLEDLearner(inps, trainingDataOptions, testingDataOptions,
            trainingDataFunction, testingDataFunction)), name = s"worker-${this.##}")

        worker ! new Run

      }

    case _: LocalLearnerFinished => context.system.terminate()
  }

}
