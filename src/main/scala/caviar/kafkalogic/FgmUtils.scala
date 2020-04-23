package caviar.kafkalogic

import math._
import orl.logic.Clause

object FgmUtils {
  def getWeightVector(theoryEstimate: List[Clause]): Vector[Double] = {
    theoryEstimate.map(rule => rule.weight).toVector
  }

  def safeZoneFunction(weightVector: Vector[Double], estimateVector: Vector[Double], threshold: Double): Double = {
    sqrt(threshold) - sqrt((weightVector zip estimateVector).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def getCurrentWeights(updatedRules: List[Clause], currentEstimate: List[Clause]): Vector[Double] = {
    currentEstimate.map(rule => {
      val find = updatedRules.find(updatedRule => areExactSame(rule,updatedRule))
      find match {
        case Some(res) => res.weight
        case None => println("this should never happen because rules do not get deleted right?"); 0.0
      }
    }).toVector
  }

  def areExactSame(clause1: Clause, clause2: Clause): Boolean = {
    if (clause1.thetaSubsumes(clause2) && clause2.thetaSubsumes(clause1)) true
    else false
  }

  def getSumOfDelta(deltaVectors: List[Vector[Double]]):Vector[Double] = {
    var returnValue = deltaVectors.head
    val restDeltaVectors = deltaVectors.drop(1)
    restDeltaVectors.foreach(vec =>  returnValue = returnValue.zip(vec).map(t => t._1 + t._2) )
    returnValue
  }

  def getNewEstimate(currentEstimate: Vector[Double],sumOfDeltas: Vector[Double],sitesWithUpdatedParams: Int): Vector[Double] = {
    var newEstimate: Vector[Double] = Vector()
    val sumsIter = sumOfDeltas.iterator
    currentEstimate.foreach(e => newEstimate = newEstimate :+ (e + sumsIter.next * 1/sitesWithUpdatedParams))
    newEstimate
  }

  def updateEstimateWeights(currentEstimate: List[Clause], newWeights: Vector[Double]): List[Clause] = {
    val weightsIter = newWeights.iterator
    currentEstimate.foreach(x => x.weight += weightsIter.next())
    currentEstimate
  }
}
