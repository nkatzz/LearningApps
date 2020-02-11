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

package maritime

import oled.logic.Clause

/**
  * Created by nkatz at 28/1/20
  */

object Test extends App {

  val r = "initiatedAt(rendezVous(X0,X1),X2) :- happensAt(gap_end(X0),X2),happensAt(gap_end(X1),X2),happensAt(stop_start(X0),X2),happensAt(stop_start(X1),X2),happensAt(change_in_heading(X0),X2),happensAt(change_in_heading(X1),X2),happensAt(change_in_speed_start(X0),X2),happensAt(change_in_speed_start(X1),X2),happensAt(change_in_speed_end(X0),X2),happensAt(change_in_speed_end(X1),X2),happensAt(stop_end(X0),X2),happensAt(stop_end(X1),X2),happensAt(gap_end(X0),X2),happensAt(gap_end(X1),X2),happensAt(slow_motion_end(X0),X2),happensAt(slow_motion_end(X1),X2),happensAt(slow_motion_start(X0),X2),happensAt(slow_motion_start(X1),X2),happensAt(slow_motion_start(X1),X2),happensAt(stopped(X0,nearPorts),X2),happensAt(stopped(X1,nearPorts),X2),happensAt(stopped(X0,farFromPorts),X2),happensAt(stopped(X1,farFromPorts),X2),happensAt(proximity(X0,X1),X2),happensAt(proximity(X1,X0),X2),happensAt(lowSpeed(X1),X2),happensAt(lowSpeed(X0),X2)."

  val t = Clause.parse(r)

  println(t.tostring)

}
