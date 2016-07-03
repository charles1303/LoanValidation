package com.app.spark.ibm.sparkApp

import scala.util._
import com.data.spark.charles.RequestInputStream

object AppStartUp extends App{
  
  val dataFileUploadKafkaTopic = Properties.envOrElse("DATA_FILE_UPLOAD_KAFKA_TOPIC", "dataFileUploadKafkaTopic")
  val dataFormKafkaTopic = Properties.envOrElse("DATA_FORM_KAFKA_TOPIC", "dataFormKafkaTopic")
  
  RequestInputStream.readDataStream(Set(dataFileUploadKafkaTopic,dataFormKafkaTopic))
}