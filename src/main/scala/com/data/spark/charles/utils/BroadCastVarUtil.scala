package com.data.spark.charles.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.data.spark.charles.RequestInputStream
import org.apache.spark.mllib.tree.model._

class BroadCastVarUtil {
  
  private var broadCastVariable : Broadcast[RDD[String]] = _
  
  private var trainingForestModel : RandomForestModel = _
  
  def setBroadCastVariable(value: Broadcast[RDD[String]]) {broadCastVariable = value}
  
  def getBroadCastVariable(): Broadcast[RDD[String]] = this.broadCastVariable
  
  def setTrainingForestModel(value: RandomForestModel) {trainingForestModel = value}
  
  def getTrainingForestModel(): RandomForestModel = this.trainingForestModel
  
}

object BroadCastVarUtil{
  
  private var broadCastVarUtilObj: BroadCastVarUtil = _
  
  def getBroadCastVarUtilObj(): BroadCastVarUtil = this.broadCastVarUtilObj
  
  def loadObjectAttribute() ={
    if (this.broadCastVarUtilObj == null){
      println("broadCastVarUtilObj is null====")
      val broadCastVarUtil = new BroadCastVarUtil
      this.broadCastVarUtilObj = broadCastVarUtil
     }
    this.broadCastVarUtilObj
  }
  
  def updateBroadCastVariable2(rdd: RDD[String]) = {
    loadObjectAttribute
    
    if(this.broadCastVarUtilObj.getBroadCastVariable() == null){
      
      this.broadCastVarUtilObj.setBroadCastVariable(RequestInputStream.getStreamingContext().sparkContext.broadcast(rdd.cache()))
    }else{
      var tempBroadcastVar = this.broadCastVarUtilObj.getBroadCastVariable()
      val newBroadCastVar = tempBroadcastVar.value.union(rdd)
      this.broadCastVarUtilObj.setBroadCastVariable(RequestInputStream.getStreamingContext().sparkContext.broadcast(newBroadCastVar.cache()))
      
    }
    
    
  }
  
  def updateBroadCastVariable(rdd: RDD[String]) = {
    loadObjectAttribute
    
      var tempBroadcastVar = this.broadCastVarUtilObj.getBroadCastVariable()
      val newBroadCastVar = tempBroadcastVar.value.union(rdd)
      this.broadCastVarUtilObj.setBroadCastVariable(RequestInputStream.getStreamingContext().sparkContext.broadcast(newBroadCastVar.cache()))
    
  }
  
  def updateTrainedForestModel(forestModel: RandomForestModel) = {
    loadObjectAttribute
    this.broadCastVarUtilObj.setTrainingForestModel(forestModel)
    
  }
  
}
   