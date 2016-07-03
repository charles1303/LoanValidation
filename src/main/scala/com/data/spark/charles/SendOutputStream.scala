package com.data.spark.charles

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import scala.util._


class SendOutputStream extends Serializable{
  
  private var broker: String = Properties.envOrElse("KAFKA_BROKER_URL", "localhost:9092")
  
  def setBroker(value: String) {broker = value}
  
  def getBroker(): String = this.broker
  
  
  def sendToKafkaForFlume(lineStream: String,topic: String) ={
      val props = new HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBroker())
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String,String](props)
      val message = new ProducerRecord[String, String](topic,null,lineStream)
      producer.send(message)
      Thread.sleep(1000)
  }
   
}
