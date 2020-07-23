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

import scala.io.Source

object GraphMaker {

  def main(args: Array[String]): Unit = {
    var singleVec: Vector[Double] = Vector()
    var noCommVec: Vector[Double] = Vector()
    var extendedNoCommVec: Vector[Double] = Vector()
    var extendedMergeVec: Vector[Double] = Vector()
    var extendedFgmVec: Vector[Double] = Vector()
    var fgmVec: Vector[Double] = Vector()
    var fgmRange: List[Int] = List()
    var mergeVec: Vector[Double] = Vector()
    val numOfLearners = 16
    var filename = "AvgError"
    var j = 0
    for (line <- Source.fromFile(filename).getLines) {
      val l1: Array[String] = line.split(" ")
      if (j == 0) {
        for (i <- l1) singleVec = singleVec :+ i.toDouble
      } else if (j == 1) {
        for (i <- l1) noCommVec = noCommVec :+ i.toDouble
        extendedNoCommVec = noCommVec.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
      } else if (j == 2) {
        for (i <- l1) fgmVec = fgmVec :+ i.toDouble
        val splitFgmVec = fgmVec.splitAt(150)
        extendedFgmVec = splitFgmVec._1
        extendedFgmVec = extendedFgmVec ++ splitFgmVec._2.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
        fgmRange = (0 to 149).toList ++ (149 + numOfLearners to extendedFgmVec.length - 1 by numOfLearners).toList
      } else {
        for (i <- l1) mergeVec = mergeVec :+ i.toDouble
        extendedMergeVec = mergeVec.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
      }
      j += 1
    }
    import scalatikz.pgf.plots.Figure
    Figure("AverageLoss")
      .plot((0 to singleVec.length - 1).toList -> singleVec)
      .plot((numOfLearners - 1 to extendedNoCommVec.length - 1 by numOfLearners) -> extendedNoCommVec)
      .plot(fgmRange -> extendedFgmVec)
      .plot((numOfLearners - 1 to extendedMergeVec.length - 1 by numOfLearners) -> extendedMergeVec)
      .havingLegends("Single Learner", "No Communication", "FGM", "SimpleMerge")
      .havingXLabel("Batch Number")
      .havingYLabel("Average Loss")
      .show()

    filename = "AccMistakes"
    j = 0
    singleVec = Vector()
    noCommVec = Vector()
    fgmVec = Vector()
    mergeVec = Vector()
    for (line <- Source.fromFile(filename).getLines) {
      val l1: Array[String] = line.split(" ")
      if (j == 0) {
        for (i <- l1) singleVec = singleVec :+ i.toDouble
      } else if (j == 1) {
        for (i <- l1) noCommVec = noCommVec :+ i.toDouble
        extendedNoCommVec = noCommVec.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
      } else if (j == 2) {
        for (i <- l1) fgmVec = fgmVec :+ i.toDouble
        val splitFgmVec = fgmVec.splitAt(150)
        extendedFgmVec = splitFgmVec._1
        extendedFgmVec = extendedFgmVec ++ splitFgmVec._2.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
        fgmRange = (0 to 149).toList ++ (149 + numOfLearners to extendedFgmVec.length - 1 by numOfLearners).toList
      } else {
        for (i <- l1) mergeVec = mergeVec :+ i.toDouble
        extendedMergeVec = mergeVec.flatMap({ x => List.fill(numOfLearners - 1)(0.0) :+ x })
      }
      j += 1
    }
    import scalatikz.pgf.plots.Figure
    Figure("AccumulatedMistakes")
      .plot((0 to singleVec.length - 1).toList -> singleVec)
      .plot((numOfLearners - 1 to extendedNoCommVec.length - 1 by numOfLearners) -> extendedNoCommVec)
      .plot(fgmRange -> extendedFgmVec)
      .plot((numOfLearners - 1 to extendedMergeVec.length - 1 by numOfLearners) -> extendedMergeVec)
      .havingLegends("Single Learner", "No Communication", "FGM", "SimpleMerge")
      .havingXLabel("Batch Number")
      .havingYLabel("Accumulated Mistakes")
      .show()
  }

}
