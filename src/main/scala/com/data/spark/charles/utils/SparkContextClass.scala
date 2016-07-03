package com.data.spark.charles.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

class SparkContextClass private{
  private var sparkContext: SparkContext = _
  
  def setSparkContext(value: SparkContext) {sparkContext = value}
  
  def getSparkContext(): SparkContext = this.sparkContext
  
}

object SparkContextClass{
  private var sparkContextClassObj: SparkContextClass = _
  
  private def loadSparkContextClass(): SparkContextClass ={
    if (this.sparkContextClassObj == null){
      val scc = new SparkContextClass
      val conf = new SparkConf()
      .setAppName("A Random Forest Implementation")
      .setMaster("local[*]")
      //.setMaster("spark://127.0.0.1:4404")
      //.set("spark.driver.host", "127.0.0.1")
      //.set("spark.shuffle.blockTransferService", "nio")
      //conf.set("spark.cores.max", "2")
      //conf.set("spark.dynamicAllocation.enabled","true");
      scc.setSparkContext(new SparkContext(conf))
      this.sparkContextClassObj = scc
    }
    this.sparkContextClassObj
  }
  
  def loadData(filepath: String): RDD[String] ={
    if (this.sparkContextClassObj == null){
      this.sparkContextClassObj = this.loadSparkContextClass()
    }
    this.sparkContextClassObj.getSparkContext().textFile(filepath)
  }
  
}