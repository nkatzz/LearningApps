package caviar.kafkalogic

import java.text.DecimalFormat

import math._
import orl.logic.Clause

object FgmUtils {
  def copyEstimate(estimate: List[Clause]): List[Clause] = {
    val copyList = estimate.map(rule => {
      val cp = new Clause(rule.head, rule.body, uuid = rule.uuid)
      cp.parentClause = rule.parentClause
      cp.isBottomRule = rule.isBottomRule
      cp.isTopRule = rule.isTopRule
      cp.weight = rule.weight
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
        val newR = new Clause(r.head,r.body,r.uuid)
        newR.parentClause = r.parentClause
        newR.isBottomRule = r.isBottomRule
        newR.isTopRule = r.isTopRule
        newR.weight = r.weight
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
        newRef.weight = ref.weight
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

  def getWeightVector(theory: List[Clause]): Vector[Double] = {
    theory.map(rule => rule.weight).toVector.map(x => format(x))
  }

  def safeZoneFunction(updatedWeights: Vector[Double], estimateWeights: Vector[Double], threshold: Double): Double = {
    val zeroedEstimateWeights = estimateWeights ++ Vector.fill(updatedWeights.length - estimateWeights.length)(0.0)
    sqrt(threshold) - sqrt((updatedWeights zip zeroedEstimateWeights).map { case (x,y) => pow(x - y, 2) }.sum)
  }

  def getCurrentWeights(updatedRules: List[Clause], currentEstimate: List[Clause]): Vector[Double] = {
    val existingWeights = currentEstimate.map(rule => updatedRules.find(updatedRule => updatedRule.uuid == rule.uuid) match {
      case Some(res) =>
        {
          res.weight
        }
      case None =>
      {
        rule.weight
      }
    })

    val newRuleWeights = updatedRules.filter(updatedRule => !currentEstimate.exists(rule => rule.## == updatedRule.##)).map(x => x.weight)
    (existingWeights ++ newRuleWeights).toVector.map(x => format(x))
  }

  // the delta vector contains only the changes in the current estimate
  def getDeltaVector(updatedRules: List[Clause], currentEstimate: List[Clause]): Vector[Double] = {

    val existingWeights = currentEstimate.map(rule => {
      val find = updatedRules.find(updatedRule => updatedRule.uuid == rule.uuid)
      find match {
      case Some(res) => res.weight
      case None => rule.weight
    }})

    val estimateWeights = getWeightVector(currentEstimate)

    (existingWeights zip estimateWeights).map {case (x,y) => x - y}.toVector.map(x => format(x))
  }

  def areEqual(clause1: Clause, clause2: Clause): Boolean = {
    if (clause1.thetaSubsumes(clause2) && clause2.thetaSubsumes(clause1)) true
    else false
  }

  def format(x: Double) = {
    val defaultNumFormat = new DecimalFormat("0.######")
    defaultNumFormat.format(x).toDouble
  }

  def getNewEstimate(deltaCollection: List[Vector[Double]]): Vector[Double] = {
    val nonZeroCount = deltaCollection
      .map(vector => vector.map(element => {if(element!=0) 1 else 0}))
      .reduceLeft( (x,y) => (x zip y)
        .map{case (x, y) => x + y})

    val addedDeltaVectors = deltaCollection.reduceLeft( (vec1, vec2) => (vec1 zip vec2).map{case (x,y) => x + y})

    (addedDeltaVectors zip nonZeroCount).map{case (x: Double, y: Int) => if(y != 0) x / y else x}.map(x => format(x))
  }

  def updateEstimateWeights(currentEstimate: List[Clause], newWeights: Vector[Double]): List[Clause] = {
    val newEstimate = (currentEstimate zip newWeights).map{
      case(rule, w) => {
        rule.weight += w
        rule.weight = format(rule.weight)
        rule
      }
    }
    newEstimate
  }
}
