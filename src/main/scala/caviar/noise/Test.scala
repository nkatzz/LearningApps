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

package caviar.noise

/**
  * Created by nkatz at 28/1/20
  */
object Test extends App {

  def presision(tps: Int, fps: Int) = tps.toDouble/(tps.toDouble+fps.toDouble)

  //tps * (Math.log(thisCoverage) - Math.log(parentCoverage))

  def gain(tps: Int, thisPresision: Double, parentPrecision: Double) = tps * (Math.log(thisPresision) - Math.log(parentPrecision))

  val thisPrecision = presision(48, 0)
  val parentPrecision = presision(182, 182)

  val _gain = gain(48, thisPrecision, parentPrecision)

  val max = 182.toDouble * (-Math.log(parentPrecision))



  val normalizedGain = _gain / max

  println(_gain)
  println(normalizedGain)





  def foilGain(thisTPs: Int, thisFPs: Int, parentTPs: Int, parentFPs: Int ) = {

    def precision(tps: Int, fps: Int) = tps.toDouble/(tps.toDouble+fps.toDouble)

    val thisCoverage = precision(thisTPs, thisFPs)
    val parentCoverage = precision(parentTPs, parentFPs)

    if (thisCoverage == 0.0) {
      // If thisCoverage == 0.0 then this rules covers nothing, it's useless, so set it's gain to 0.
      // Note that otherwise we'll have a logarithm evaluated to -infinity (log(0)).
      // If, on the other hand thisCoverage == 1.0 then this rule is perfect (but so is the parent --
      // the parent cannot have smaller coverage), so again, no gain.
      0.0
    } else {
      // here thisCoverage is in (0,1)
      if (parentCoverage == 1.0 || parentCoverage == 0.0) {
        // If parentCoverage == 1.0 then the parent rule is perfect, no way to beat that, so set this rule's gain to 0
        // Note that otherwise we'll have the parent's log evaluated to 0 and the gain formula
        // returning a negative value (parentTPs * log(thisCoverage), which is < 0 since thisCoverage < 1).
        // Eitherway, we only care for positive gain.
        // If, on the other hand, parentCoverage == 0.0 then thisCoverage == 0 (the parent covers nothing, so no way for
        // this rule -- a refinement --  to cover something)
        0.0
      } else {
        // here parentCoverage is in (0,1)
        val _gain = thisTPs.toDouble * (Math.log(thisCoverage) - Math.log(parentCoverage))

        // We are interested only in positive gain, therefore we consider 0 as the minimum of the gain function:
        val gain = if (_gain <= 0) 0.0 else _gain

        // This is the maximum gain for a given rule:
        val max = parentTPs.toDouble * (-Math.log(parentCoverage))
        val normalizedGain = gain / max

        normalizedGain
      }
    }
  }

  println(foilGain(48,0,182,182))



}
