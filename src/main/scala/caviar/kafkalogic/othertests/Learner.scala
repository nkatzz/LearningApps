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
import orl.learning.Types.{FinishedBatch, StartOver}
import orl.utils.Utils.underline
import orl.learning.woledasp.WoledASPLearner
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

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

    val pw = new PrintWriter(new FileWriter(new File("SingleLearnerError"), true))

    private def getNextBatch: Example = if (data.isEmpty) Example() else data.next()

    import context.become


    override def processingState: Receive = {
        case exmpl: Example =>
            process(exmpl)
            println(state.perBatchError + " " +batchCount)
            if(((batchCount +1 ) % 15) ==0) {
                var error = 0
                for (i <- state.perBatchError) {
                    error += i
                }
               pw.write("Batch: "+ batchCount+ " error: " + error + "\n")
                pw.flush()
            }
            batchCount += 1
            become(controlState)
            self ! new FinishedBatch
    }
}
