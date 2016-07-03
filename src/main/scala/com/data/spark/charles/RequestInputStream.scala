package com.data.spark.charles

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import com.data.spark.charles.processor.DataStreamProcessor
import com.data.spark.charles.processor.DataFileProcessor
import scala.util._

class RequestInputStream {
  
  
  private var streamingContext: StreamingContext = _
  
  private var  topics: Set[String] = _
  
  private var  streamPauseSeconds: Int = _
  
  def setStreamingContext(value: StreamingContext) {streamingContext = value}
  
  def getStreamingContext(): StreamingContext = this.streamingContext
  
  def setTopics(value: Set[String]) {topics = value}
  
  def getTopics(): Set[String] = this.topics
  
  def setStreamPauseSeconds(value: Int) {streamPauseSeconds = value}
  
  def getStreamPauseSeconds(): Int = this.streamPauseSeconds
  
 }

object RequestInputStream{
  private var requestInputStreamObj: RequestInputStream = _
  
  def loadStreamingContextClass(pauseTimeSeconds: Int): RequestInputStream ={
    if (this.requestInputStreamObj == null){
      val reqInputStream = new RequestInputStream
      val conf = new SparkConf().setAppName("A Kafka Stream Implementation").setMaster("local[2]")
      reqInputStream.setStreamPauseSeconds(pauseTimeSeconds)
      reqInputStream.setStreamingContext(new StreamingContext(conf,Seconds(pauseTimeSeconds)))
      this.requestInputStreamObj = reqInputStream
    }
    this.requestInputStreamObj
  }
  
  def getStreamingContext(): StreamingContext ={
    this.requestInputStreamObj.streamingContext
  }
  
  
  
  def readDataStream(topics: Set[String])={
    
    val kafkaParams: Map[String,String] = Map("metadata.broker.list" -> Properties.envOrElse("KAFKA_BROKER_URL", "localhost:9092"))
    val pauseTimeSeconds: Int = Integer.valueOf(Properties.envOrElse("PAUSE_TIME_STREAM_WINDOW", "20"))
    val dataFileUploadKafkaTopic = Properties.envOrElse("DATA_FILE_UPLOAD_KAFKA_TOPIC", "dataFileUploadKafkaTopic")
    val dataFormKafkaTopic = Properties.envOrElse("DATA_FORM_KAFKA_TOPIC", "dataFormKafkaTopic")
      
        if (this.requestInputStreamObj == null){
          this.requestInputStreamObj = this.loadStreamingContextClass(pauseTimeSeconds)
        }
        
    for( topic <- topics ){
        var singleTopicSet: Set[String] = Set(topic)
          val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](this.requestInputStreamObj.streamingContext, kafkaParams, singleTopicSet)
           
        val lines = stream.map(_._2)
        lines.print()
        
        val splitLines:DStream[String] = lines.flatMap(_.split("\n"))

        val data = splitLines.map(

                splitData => {
                      val dataStringValue = splitData
                      dataStringValue
                })
                
        topic match {
          case "dataFileUploadKafkaTopic" => {
            println("In dataFileUploadKafkaTopic")
            data.foreachRDD{ rdd => if (!rdd.isEmpty) {
              println("Data is not empty for dataFileUploadKafkaTopic")
                  val dataFileProcessor = new DataFileProcessor
                  dataFileProcessor.updateModel(rdd)
                }else{
                  println("Data is empty for dataFileUploadKafkaTopic")
                }
            
            }
         }
          case "dataFormKafkaTopic" => {
            println("In dataFormKafkaTopic")
            data.foreachRDD{ rdd => if (!rdd.isEmpty) {
              println("Data is not empty for dataFormKafkaTopic")
                  DataStreamProcessor.processData(rdd)
                }else{
                  println("Data is empty for dataFormKafkaTopic")
                }
            }
             
            }
          case _ => {println("No Kafka Topic Set for "+topic)}
        }  
      }
      this.requestInputStreamObj.streamingContext.start()
      this.requestInputStreamObj.streamingContext.awaitTermination();
    
  }
 
  
}