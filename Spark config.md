Download the JDK from Oracle's JDK site or use OpenJDK.
Download winutils.exe and extract into C:\users\<user>\Hadoop
Download Apache Spark "Pre-built for Apache Hadoop 3.3 and later." (.tgz) and extract into C:\Users\<user>\Spark\spark-3.5.3-bin-hadoop3

#Create and set Environment Variables for system and user (can also set in Windows)
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-21.0.5.11-hotspot"
setx SPARK_HOME "C:\Users\kdabc\Spark\spark-3.5.3-bin-hadoop3"
setx HADOOP_HOME "C:\Users\kdabc\hadoop"
setx PYSPARK_HOME "C:\Users\kdabc\AppData\Local\Programs\Python\Python312\python.exe"

#Add to system and user PATH
%SPARK_HOME%\bin
%HADOOP_HOME%\bin

#verify Spark is configured 
spark-submit --version




