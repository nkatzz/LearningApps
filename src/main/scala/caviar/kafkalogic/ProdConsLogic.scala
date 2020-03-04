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
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import orl.datahandling.Example
import orl.logic.Clause
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object ProdConsLogic {

  def createExampleProducer(): KafkaProducer[String, Example] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka Example Producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "orl.kafkalogic.ExampleSerializer")
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner")

    val producer = new KafkaProducer[String, Example](props)
    producer
  }

  def createExampleConsumer(id: String): KafkaConsumer[String, Example] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer_" + id)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "orl.kafkalogic.ExampleDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1") // We only want to get one example per poll

    val exampleConsumer = new KafkaConsumer[String, Example](props)
    exampleConsumer
  }

  def createTheoryProducer(): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaTheoryProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "41943040")

    val producer = new KafkaProducer[String, Array[Byte]](props)
    producer
  }

  def writeExamplesToTopic(data: Iterator[Example], numOfActors: Int, examplesPerIteration: Int): Boolean = {
    val producer = createExampleProducer()
    var examplesFinished = false
    for (i <- 1 to numOfActors * examplesPerIteration) {
      if (data.nonEmpty) {
        val exmpl = data.next()
        val record = new ProducerRecord[String, Example]("ExamplesTopic", exmpl)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(), metadata.get().partition(),
          metadata.get().offset());
      } else examplesFinished = true
    }
    producer.close()
    examplesFinished
  }

  def writeTheoryToTopic(data: List[Clause], producer: KafkaProducer[String, Array[Byte]]): Unit = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(data)
    val value = bos.toByteArray
    out.close()

    val record = new ProducerRecord[String, Array[Byte]]("TheoryTopic", value)
    val metadata = producer.send(record)
    printf(s"sent record(key=%s value=%s) " +
      "meta(partition=%d, offset=%d)\n",
      record.key(), record.value(), metadata.get().partition(),
      metadata.get().offset());
  }
}
