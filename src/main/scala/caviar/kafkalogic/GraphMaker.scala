package caviar.kafkalogic

import scala.io.Source

object GraphMaker {

  def main(args: Array[String]): Unit = {
    var singleLearnerVec: Vector[Double] = Vector()
    var mergeVec: Vector[Double] = Vector()
    var noMergeVec: Vector[Double] = Vector()
    var filename = "AvgError"
    var j = 0
    for (line <- Source.fromFile(filename).getLines) {
      val l1: Array[String] = line.split(" ")
      if( j== 0) {
        for (i <- l1) singleLearnerVec = singleLearnerVec :+ i.toDouble
      } else if( j ==1) {
        for( i <- l1) mergeVec = mergeVec :+ i.toDouble
      } else {
        for( i <- l1) noMergeVec = noMergeVec :+ i.toDouble
      }
      j += 1
    }
    import scalatikz.pgf.plots.Figure
    Figure("AverageLoss")
      .plot((0 to singleLearnerVec.length-1) -> singleLearnerVec)
      .plot((0 to mergeVec.length-1) -> mergeVec)
      .plot((0 to noMergeVec.length-1) -> noMergeVec)
      .havingLegends("SingleLearner", "Merged Theory","No Communication" )
      .havingXLabel("Batch Number (120 examples each batch) Merge every: 120 examples 3 Learners")
      .havingYLabel("Average Loss")
      .show()

    filename = "AccMistakes"
    j = 0
    singleLearnerVec = Vector()
    mergeVec = Vector()
    noMergeVec = Vector()
    for (line <- Source.fromFile(filename).getLines) {
      val l1: Array[String] = line.split(" ")
      if( j== 0) {
        for (i <- l1) singleLearnerVec = singleLearnerVec :+ i.toDouble
      } else if( j ==1) {
        for( i <- l1) mergeVec = mergeVec :+ i.toDouble
      } else {
        for( i <- l1) noMergeVec = noMergeVec :+ i.toDouble
      }
      j += 1
    }
    println(singleLearnerVec)
    println(mergeVec)
    println(noMergeVec)
    import scalatikz.pgf.plots.Figure
    Figure("AccumulatedMistakes")
      .plot((0 to singleLearnerVec.length-1) -> singleLearnerVec)
      .plot((0 to mergeVec.length-1) -> mergeVec)
      .plot((0 to noMergeVec.length-1) -> noMergeVec)
      .havingLegends("SingleLearner", "Merged Theory","No Communication" )
      .havingXLabel("Batch Number (120 examples each batch) Merge every: 120 examples 3 Learners")
      .havingYLabel("Accumulated Mistakes")
      .show()
  }


}
