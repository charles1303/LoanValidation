This is a Spark Streaming Application that uses Machine Learning Algorithm (Random Forest) to validate if
intending applicants/users qualify to use a service.
The dependencies of this application include the following:

1. Install Apache Kafka
2. Start up the Apache Zookeeper
3. Start up Apache Kafka
4. Create the following topics
  a. dataFileUploadKafkaTopic
  b. dataFormKafkaTopic
  
5. Install and start up Apache Hadoop
6. Install and start up Apache Flume
7. Set up this Flume agent

spool_for_hdfs.sources = spooled_dir
spool_for_hdfs.channels = MemChannel
spool_for_hdfs.sinks = HDFS

spool_for_hdfs.sources.spooled_dir.interceptors = pre_persist
spool_for_hdfs.sources.spooled_dir.interceptors.pre_persist.type=com.charles.flume.interceptor.DeleteHadoopFileInterceptor$Builder
spool_for_hdfs.sources.spooled_dir.interceptors.pre_persist.threadNum = 200
spool_for_hdfs.channels.memory_channel.type   = memory

spool_for_hdfs.channels.MemChannel.type = memory
spool_for_hdfs.channels.MemChannel.capacity = 500
spool_for_hdfs.channels.MemChannel.transactionCapacity = 200
spool_for_hdfs.channels.MemChannel.fileSuffix = 
#spool_for_hdfs.channels.MemChannel.deletePolicy = immediate

spool_for_hdfs.sources.spooled_dir.channels = MemChannel
spool_for_hdfs.sources.spooled_dir.type = spooldir
#spool_for_hdfs.sources.spooled_dir.type = http
spool_for_hdfs.sources.spooled_dir.spoolDir = /home/uploads/
spool_for_hdfs.sources.spooled_dir.fileHeader = true

spool_for_hdfs.sinks.HDFS.channel = MemChannel
spool_for_hdfs.sinks.HDFS.type = hdfs
spool_for_hdfs.sinks.HDFS.hdfs.path = hdfs://127.0.0.1:9000/tmp/models
spool_for_hdfs.sinks.HDFS.hdfs.fileType = DataStream
spool_for_hdfs.sinks.HDFS.hdfs.writeFormat = Text
spool_for_hdfs.sinks.HDFS.hdfs.batchSize = 100
spool_for_hdfs.sinks.HDFS.hdfs.rollSize = 0
spool_for_hdfs.sinks.HDFS.hdfs.rollCount = 0
spool_for_hdfs.sinks.HDFS.hdfs.rollInterval = 3000

8. Create the following environmental variables
HDFS_BASE_URL : Hadoop base url
DATA_SOURCE_BASE_URL : upload folder for model updates
EMAIL_ADDRESS : Email for mail sending
EMAIL_PASSWORD: Email password for mail sending
DATA_FILE_UPLOAD_KAFKA_TOPIC : Kafka Topic for file upload
DATA_FORM_KAFKA_TOPIC : Kafka Topic for user form inputs

9. Build the Spark application and submit to cluster
10. Java Web Client or a mobile application
TODO
Upload the Java REST based web client
