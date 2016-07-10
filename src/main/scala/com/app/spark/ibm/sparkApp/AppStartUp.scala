package com.app.spark.ibm.sparkApp

import scala.util._
import com.data.spark.charles.RequestInputStream
import com.data.spark.charles.utils.RandomForestImplClass

object AppStartUp extends App{
  
  val dataFileUploadKafkaTopic = Properties.envOrElse("DATA_FILE_UPLOAD_KAFKA_TOPIC", "dataFileUploadKafkaTopic")
  val dataFormKafkaTopic = Properties.envOrElse("DATA_FORM_KAFKA_TOPIC", "dataFormKafkaTopic")
  
  //Load training model into memory
  val randomForestImpl = new RandomForestImplClass
  randomForestImpl.reloadTrainingModel()
  
  RequestInputStream.readDataStream(Set(dataFileUploadKafkaTopic,dataFormKafkaTopic))
}