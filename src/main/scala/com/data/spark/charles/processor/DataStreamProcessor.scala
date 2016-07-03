package com.data.spark.charles.processor

import org.apache.spark.rdd.RDD
import com.data.spark.charles.RequestInputStream
import com.data.spark.charles.SendOutputStream
import com.data.spark.charles.utils.BroadCastVarUtil
import org.apache.spark.broadcast.Broadcast
import com.data.spark.charles.utils.ValidationUtil
import com.data.spark.charles.utils.RandomForestImplClass
import com.data.spark.charles.utils.SmtpEmailer
import org.apache.spark.streaming.dstream.DStream
import scala.util._

class DataStreamProcessor {
  
}

object DataStreamProcessor{
  
  val to_hdfsKafkaTopic = Properties.envOrElse("TO_HDFS_KAFKA_TOPIC", "to_hdfsKafkaTopic")
  val to_hdfs_error_data = Properties.envOrElse("TO_HDFS_DATA_ERROR_KAFKA_TOPIC", "to_hdfs_error_data")
  
  def processData(lineStream: RDD[String])={
    var isValid = false
    var valUtils = new ValidationUtil
    lineStream.foreach { line =>
      isValid = valUtils.validateFieldCnt(line)
      var processedLine = ""
      var emailAddr = line.split(",")(6)
      var emailMsg = "Invalid Data. Please resend information"
      if(isValid == true){
        val randomForestImpl = new RandomForestImplClass
        var result = randomForestImpl.executeMachineLearning(line);
        emailMsg = getStatus(result)
        
      }else{
        //send to Flume for HDFS via Kafka
        var sos = new SendOutputStream
        sos.sendToKafkaForFlume(processedLine, "to_hdfs_error_data")
        
      }
      val mailClient = new SmtpEmailer
      mailClient.sendMail(emailAddr, emailMsg)
      
    }
    
  }
  def getStatus(result: Double): String = {
    val stringResponse: String = result match {
          case 1 => "Valid Loan Applicant"
          case 2 => "Low Risk Loan Applicant"
          case 3 => "Medium Risk Loan Applicant"
          case 4 => "High Risk Loan Applicant"
        }       
        stringResponse
  }
  
     
}