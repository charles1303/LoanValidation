package com.data.spark.charles.utils

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import com.data.spark.charles.processor.DataFileProcessor
import scala.util._

class RandomForestImplClass {
  
  def this(inputRddData: RDD[String]) { this(); updateTrainingModel(inputRddData);}
  
    
  def executeMachineLearning(inputData: String) : Double={
    if(BroadCastVarUtil.getBroadCastVarUtilObj().getTrainingForestModel() == null){
      reloadTrainingModel()
    }
    predict(inputData)
  }
  
  def reloadTrainingModel() = {
    val hdfsBaseUrl = Properties.envOrElse("HDFS_BASE_URL", "hdfs://127.0.0.1:9000")
    val inputFileData = SparkContextClass.loadData(hdfsBaseUrl+"/tmp/models/*").cache()
    updateTrainingModel(inputFileData)
        
  }
  
    
  def prepareData(dataInput: RDD[String]): RDD[LabeledPoint] = {
    
      val data = dataInput.map { line =>
        val values = line.split(',').map(_.toDouble)
        val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
        val soil = values.slice(14, 54).indexOf(1.0).toDouble
        val featureVector =
        Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
        val label = values.last - 1
        LabeledPoint(label, featureVector)
      }
    val splitArray: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.8,0.1,0.1))
    splitArray(0).cache()
  }
  
  def updateTrainingModel(data: RDD[String]) = {
    
    val trainData = prepareData(data)
    
    val model = RandomForest.trainClassifier(trainData, 7, Map(10 -> 4, 11 -> 40), 20,"auto", "entropy", 30, 300)
    BroadCastVarUtil.updateTrainedForestModel(model)
    
  }
  
  def predict(data:String) = {
    val vector = Vectors.dense(data.split(',').map(_.toDouble))
    BroadCastVarUtil.getBroadCastVarUtilObj().getTrainingForestModel().predict(vector)
  }
  
  def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]):
      MulticlassMetrics = {
        val predictionAndLabels = data.map(example => (model.predict(example.features), example.label)
            )
            new MulticlassMetrics(predictionAndLabels)
  }
  
}