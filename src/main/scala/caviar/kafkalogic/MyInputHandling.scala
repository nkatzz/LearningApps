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

import com.mongodb.casbah.{MongoClient, MongoCollection}
import orl.datahandling.Example
import orl.datahandling.InputHandling.{MongoDataOptions, getMongoData}

object MyInputHandling {
  def getMongoData(opts: MongoDataOptions): Vector[Iterator[Example]] = {
    val mc = MongoClient()

    val exmplIters = opts.dbNames map { dbName =>

      val collection: MongoCollection = mc(dbName)("examples")

      val data = opts.allData(collection, opts.sort, opts.sortDbByField) map { x =>
        val e = Example(x)

        opts.targetConcept match {
          case "None" => Example(e.queryAtoms, e.observations, e.time)
          case _ => Example(e.queryAtoms filter (_.contains(opts.targetConcept)), e.observations, e.time)
        }
      }

      if (opts.what == "training") {
        opts.chunkSize > 1 match {
          case false => data
          case _ =>
            data.grouped(opts.chunkSize).map { x =>
              //data.sliding(opts.chunkSize).map { x =>
              x.foldLeft(Example()) { (z, y) => Example(z.queryAtoms ++ y.queryAtoms, z.observations ++ y.observations, x.head.time) }
            }
        }
      } else { // no chunking for testing data
        ///*
        val firstDataPoint = data.next()
        val annotation = firstDataPoint.queryAtoms
        val narrative = firstDataPoint.observations
        val time = firstDataPoint.time
        val merged = data.foldLeft(annotation, narrative) { (accum, ex) =>
          (accum._1 ++ ex.queryAtoms, accum._2 ++ ex.observations)
        }
        val e = Example(merged._1, merged._2, time)
        Iterator(e)
        //*/

        // comment the above and uncomment this to have chunked data
        /*
        data.grouped(opts.chunkSize).map { x =>
          //data.sliding(opts.chunkSize).map { x =>
          x.foldLeft(Example()) { (z, y) =>
            new Example(annot = z.annotation ++ y.annotation, nar = z.narrative ++ y.narrative, _time = x.head.time)
          }
        }
        */
      }
    }
    exmplIters
  }
}

