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

package orl.kafkalogic

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import orl.logic.Clause
import orl.datahandling.Example
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class ExampleSerializer extends Serializer[Example] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }
  override def serialize(s: String, t: Example): Array[Byte] = {
    if (t == null)
      null
    else {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.writeValueAsString(t).getBytes
    }
  }
  override def close(): Unit = {
  }
}

class ExampleDeserializer extends Deserializer[Example] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }
  override def deserialize(s: String, bytes: Array[Byte]): Example = {
    val mapper = new ObjectMapper()
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.registerModule(DefaultScalaModule)
    val exmpl = mapper.readValue(bytes, classOf[Example])
    exmpl
  }
  override def close(): Unit = {
  }
}

