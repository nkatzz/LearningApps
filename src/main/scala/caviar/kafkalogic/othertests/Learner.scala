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

package caviar.kafkalogic.othertests

import orl.datahandling.InputHandling.InputSource
import orl.app.runutils.RunningOptions
import orl.datahandling.Example
import orl.learning.Types.{FinishedBatch, LocalLearnerFinished, StartOver}
import orl.utils.Utils.underline
import orl.learning.woledasp.WoledASPLearner
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import akka.actor.PoisonPill

class Learner[T <: InputSource](
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

    val pw = new PrintWriter(new FileWriter(new File("AvgError"), true))
    val pw2 = new PrintWriter(new FileWriter(new File("AccMistakes"), true))


    import context.become


    override def processingState: Receive = {
        case exmpl: Example =>
            process(exmpl)
            batchCount += 1
            become(controlState)
            self ! new FinishedBatch
    }

    def avgLoss(in: Vector[Int]) = {
        in.foldLeft(0, 0, Vector.empty[Double]) { (x, y) =>
            val (count, prevSum, avgVector) = (x._1, x._2, x._3)
            val (newCount, newSum) = (count + 1, prevSum + y)
            (newCount, newSum, avgVector :+ newSum.toDouble / newCount)
        }
    }

    override def shutDown() = {
        var error: Vector[Int] = Vector()
        for(i <- 0 to state.perBatchError.length/3){
            val tempError = state.perBatchError(i) + state.perBatchError(i+1) + state.perBatchError(i+2)
            error = error :+ tempError
        }

        val accumulatedMistakes = state.perBatchError.scanLeft(0.0)(_ + _).tail
        var mistakes: Vector[Double] = Vector()
        for(i <- 0 to accumulatedMistakes.length/3){
            val tempError = accumulatedMistakes(i) + accumulatedMistakes(i+1) + accumulatedMistakes(i+2)
            mistakes = mistakes :+ tempError
        }

        val averageLoss = avgLoss(error)
        for(i <- averageLoss._3) pw.write(i + " ")
        pw.write("\n")
        pw.flush()
        pw.close()
        for(i <- mistakes) pw2.write(i + " ")
        pw2.write("\n")
        pw2.flush()
        pw2.close()
        import scalatikz.pgf.plots.Figure
        Figure("AverageLoss")
          .plot((0 to averageLoss._3.length-1) -> averageLoss._3)
          .havingXLabel("Batch Number (15 Examples 5 to each learner)")
          .havingYLabel("Average Loss")
          .show()
        Figure("AccumulatedError")
          .plot((0 to mistakes.length-1) -> mistakes)
          .havingXLabel("Batch Number (15 Examples 5 to each learner)")
          .havingYLabel("Accumulated Mistakes")
          .show()
        self ! PoisonPill
        context.parent ! new LocalLearnerFinished
    }

}
