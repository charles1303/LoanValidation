package com.data.spark.charles.utils

import org.apache.spark.rdd.RDD

class ValidationUtil extends Serializable{
  
  val noOfExpectedCSVColumns: Int = 23
  
  def validateFieldCnt(rddLine: String): Boolean = {
    var isValid : Boolean = false
    var cnt : Array[String] = rddLine.split(",")
    println("cnt length==="+cnt.length)
    if(cnt.length < noOfExpectedCSVColumns){
       isValid = true;
       println("Is Valid")
       println("Is Valid Object====="+isValid)
    }else{
      println("Is Not Valid")
       println("Is Valid Object====="+isValid)
    }
    
    isValid
    
  }
  
}