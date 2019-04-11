#!/bin/bash

echo "Copy Hadoop conf files to /root/hadoop/etc/hadoop/"
cp /etc/simple-grid/config/hadoop-env.sh /root/hadoop/etc/hadoop/
cp /etc/simple-grid/config/core-site.xml /root/hadoop/etc/hadoop/
cp /etc/simple-grid/config/hdfs-site.xml /root/hadoop/etc/hadoop/
cp /etc/simple-grid/config/mapred-site.xml /root/hadoop/etc/hadoop/
cp -f /etc/simple-grid/config/yarn-site.xml /root/hadoop/etc/hadoop/
cp -f /etc/simple-grid/config/slaves /root/hadoop/etc/hadoop/

echo "Copying spark config files"
cp /etc/simple-grid/config/spark-defaults.conf /root/hadoop/spark/conf/

cat /etc/simple-grid/config/hadoop_env.sh >> ~/.bashrc
source ~/.bashrc

echo "Formatting HDFS"
hdfs namenode -format

echo "Starting HDFS"
start-dfs.sh

echo "Start YARN"
start-yarn.sh

echo "HDFS report"
hdfs dfsadmin -report

echo "YARN nodes"
yarn node -list

echo "Initializing the Spark Hadoop Master Container"

echo "Creating spark-logs hdfs directory"
hdfs dfs -mkdir /spark-logs

echo "Start Spark History Server"
$SPARK_HOME/sbin/start-history-server.sh

echo "All Done!"