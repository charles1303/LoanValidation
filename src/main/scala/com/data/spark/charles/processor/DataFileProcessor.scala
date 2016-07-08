package com.data.spark.charles.processor

import com.data.spark.charles.utils.SparkContextClass
import org.apache.spark.streaming.dstream.DStream
import com.data.spark.charles.utils.RandomForestImplClass
import com.data.spark.charles.RequestInputStream
import scala.util._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import com.data.spark.charles.SendOutputStream

class DataFileProcessor extends Serializable{
    val hdfsBaseUrl = Properties.envOrElse("HDFS_BASE_URL", "hdfs://127.0.0.1:9000")
      val dataSourceBaseUrl = Properties.envOrElse("DATA_SOURCE_BASE_URL", "/home/uploads/")
  def updateModel(lineStream: RDD[String])={
    lineStream.foreach { fileName =>
      var file = fileName
      if(!new java.io.File(dataSourceBaseUrl+file).exists){
        file = fileName + ".COMPLETED"
      }
      
      val inputFileData = RequestInputStream.getStreamingContext().sparkContext.textFile("file://"+dataSourceBaseUrl+file).cache()
      val randomForestModel = new RandomForestImplClass(inputFileData)
      randomForestModel.updateTrainingModel(inputFileData)
      //val sos = new SendOutputStream
      //sos.
      //Let Flume and its Interceptor handle this
      //deleteHadoopFile(fileName,inputFileData)
        
      }
  }
  
  def deleteHadoopFile(fileName: String,inputFileData: RDD[String]) = {
    val conf = new Configuration();
      conf.set("fs.hdfs.impl",(classOf[org.apache.hadoop.hdfs.DistributedFileSystem]).getName);
      conf.set("fs.file.impl",(classOf[org.apache.hadoop.fs.LocalFileSystem]).getName);
      val  hdfs = FileSystem.get(java.net.URI.create(hdfsBaseUrl), conf);
      try { 
            hdfs.delete(new org.apache.hadoop.fs.Path("/tmp/models/"), true)
            inputFileData.saveAsTextFile(hdfsBaseUrl+"/tmp/models/"+fileName)
          } catch { 
                case _ : Throwable => { } 
          }
  }
  
  
}